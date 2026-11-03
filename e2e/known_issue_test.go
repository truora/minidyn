package e2e

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const (
	// runKnownEnv makes known-issue cases run as normal tests (useful while fixing one).
	runKnownEnv = "MINIDYN_E2E_RUN_KNOWN"
)

// issueURL is where a knownIssue number ("#146") is tracked.
const issueURL = "https://github.com/truora/minidyn/issues/"

var knownIssueRE = regexp.MustCompile(`^#[1-9][0-9]*$`)

// RunE2EKnown is RunE2E for a case with a known minidyn divergence, tracked as a GitHub
// issue: knownIssue is its number, e.g. "#146".
//
// With knownIssue empty it is exactly RunE2E. Otherwise the case is an expected failure:
//
//   - it still fails: the test is reported as skipped with a link to the issue, so CI stays
//     green while the bug stays visible;
//   - it now passes: the test FAILS, telling you to remove knownIssue and close the issue,
//     so a fixed bug can never keep a stale skip.
//
// Failures inside the test body (require.*) cannot be intercepted in-process, so the case
// is re-run in a child test process (same binary, same DynamoDB Local container) and its
// exit status decides. Set MINIDYN_E2E_RUN_KNOWN=1 to run known-issue cases normally.
func RunE2EKnown[T any](t *testing.T, knownIssue string, fn func(t *testing.T, client *dynamodb.Client) T) {
	t.Helper()

	if knownIssue != "" && !knownIssueRE.MatchString(knownIssue) {
		t.Fatalf("knownIssue %q must be a GitHub issue number like \"#146\"", knownIssue)
	}

	if knownIssue == "" || os.Getenv(runKnownEnv) != "" || os.Getenv(childEnv) != "" {
		RunE2E(t, fn)

		return
	}

	// Starts (or reuses) DynamoDB Local before spawning; skips when Docker is unavailable.
	_ = sharedDynamoDBLocalEndpoint(t)

	passed, output := runKnownIssueChild(t)

	// The child failing is only the known divergence if minidyn is what differs: a failure
	// on DynamoDB Local or a panic means the test or the infrastructure is broken.
	if strings.Contains(output, "--- FAIL: "+t.Name()+"/dynamodb-local ") || strings.Contains(output, "\npanic: ") {
		t.Errorf("known parity issue %s: the case failed for a reason other than minidyn's divergence:\n%s",
			knownIssue, tail(output, 4000))

		return
	}

	if passed {
		t.Errorf("known parity issue %s appears FIXED: this case now passes.\n"+
			"Remove its knownIssue field and close %s%s (or remove the marker from\n"+
			"every other test that still references it first).",
			knownIssue, issueURL, knownIssue[1:])

		return
	}

	t.Logf("child output (tail):\n%s", tail(output, 2000))
	t.Skipf("known parity issue %s still failing: %s%s", knownIssue, issueURL, knownIssue[1:])
}

// runKnownIssueChild re-runs the current test in a child process and reports whether it
// passed. It is inconclusive (the parent skips) if the child can't be run.
func runKnownIssueChild(t *testing.T) (passed bool, output string) {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Skipf("cannot locate test binary to verify known issue: %v", err)
	}

	segments := strings.Split(t.Name(), "/")
	for i, s := range segments {
		segments[i] = "^" + regexp.QuoteMeta(s) + "$"
	}

	args := []string{"-test.run", strings.Join(segments, "/"), "-test.count=1", "-test.v"}

	// Bound the child by the parent's deadline, and kill it if the parent test ends first.
	if deadline, ok := t.Deadline(); ok {
		args = append(args, "-test.timeout="+time.Until(deadline).Round(time.Second).String())
	}

	cmd := exec.CommandContext(t.Context(), exe, args...) //nolint:gosec // re-executes this test binary with fixed flags
	cmd.Env = append(childEnviron(), childEnv+"=1")

	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	runErr := cmd.Run()
	output = buf.String()

	var exitErr *exec.ExitError

	switch {
	case runErr == nil:
		// Exit 0 only counts as a pass if the case really ran and passed.
		return strings.Contains(output, "--- PASS: "+t.Name()+" "), output
	case errors.As(runErr, &exitErr):
		return false, output
	default:
		t.Skipf("cannot run child test process to verify known issue: %v", runErr)

		return false, output
	}
}

// childEnviron is the parent's environment without trace settings: in strict mode every
// known case would fail on its documented gaps, so a fixed issue would never be reported.
func childEnviron() []string {
	var env []string

	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, traceModeEnv+"=") && !strings.HasPrefix(kv, traceOutEnv+"=") {
			env = append(env, kv)
		}
	}

	return env
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}

	return "..." + s[len(s)-n:]
}
