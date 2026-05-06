// Command run-e2e runs e2e parity tests via go test -json and prints only failures, or OK
// with a test-case count (pass/fail/skip events from test2json).
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// testEvent matches go test -json output lines.
// See: go doc test2json
type testEvent struct {
	Action  string `json:"Action"`
	Package string `json:"Package"`
	Test    string `json:"Test"`
	Output  string `json:"Output"`
}

func main() {
	pattern, extra := parseArgs(os.Args)

	cmd, stdout, err := startGoTest(pattern, extra)
	if err != nil {
		fmt.Fprintf(os.Stderr, "run-e2e: %v\n", err)
		os.Exit(1)
	}

	col := newJSONLCollector()
	if err := col.scan(stdout); err != nil {
		fmt.Fprintf(os.Stderr, "run-e2e: reading test output: %v\n", err)
		_ = cmd.Process.Kill()

		os.Exit(1)
	}

	waitErr := cmd.Wait()
	os.Exit(report(col, waitErr))
}

func parseArgs(argv []string) (pattern string, extra []string) {
	pattern = "TestE2E_"
	extra = argv[1:]

	if len(extra) > 0 && !strings.HasPrefix(extra[0], "-") {
		pattern = extra[0]
		extra = extra[1:]
	}

	return pattern, extra
}

func startGoTest(pattern string, extra []string) (*exec.Cmd, io.ReadCloser, error) {
	args := append([]string{"test", "./e2e/...", "-json", "-run", pattern}, extra...)
	cmd := exec.Command("go", args...) //nolint:gosec // variable args are required here
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}

	return cmd, stdout, nil
}

func eventKey(pkg, test string) string {
	return pkg + "\x00" + test
}

type failureRecord struct {
	key, name string
	out       string
}

type jsonlCollector struct {
	testOut     map[string]*strings.Builder
	pkgOut      map[string]*strings.Builder
	failedTests []failureRecord
	pkgFailed   bool
	nPass       int
	nFail       int
	nSkip       int
}

func newJSONLCollector() *jsonlCollector {
	return &jsonlCollector{
		testOut: make(map[string]*strings.Builder),
		pkgOut:  make(map[string]*strings.Builder),
	}
}

func (c *jsonlCollector) scan(r io.Reader) error {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		var ev testEvent
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			continue
		}

		c.handle(ev)
	}

	return sc.Err()
}

func (c *jsonlCollector) handle(ev testEvent) {
	switch ev.Action {
	case "output":
		c.recordOutput(ev)
	case "fail":
		c.recordFail(ev)
	case "pass":
		if ev.Test != "" {
			c.nPass++
			delete(c.testOut, eventKey(ev.Package, ev.Test))
		}
	case "skip":
		if ev.Test != "" {
			c.nSkip++
			delete(c.testOut, eventKey(ev.Package, ev.Test))
		}
	}
}

func (c *jsonlCollector) recordOutput(ev testEvent) {
	if ev.Test != "" {
		k := eventKey(ev.Package, ev.Test)
		if c.testOut[k] == nil {
			c.testOut[k] = &strings.Builder{}
		}
		_, _ = c.testOut[k].WriteString(ev.Output)

		return
	}

	if ev.Package == "" {
		return
	}

	if c.pkgOut[ev.Package] == nil {
		c.pkgOut[ev.Package] = &strings.Builder{}
	}
	_, _ = c.pkgOut[ev.Package].WriteString(ev.Output)
}

func (c *jsonlCollector) recordFail(ev testEvent) {
	if ev.Test != "" {
		c.nFail++
		k := eventKey(ev.Package, ev.Test)

		out := ""
		if b := c.testOut[k]; b != nil {
			out = b.String()
		}
		name := ev.Package + " " + ev.Test
		c.failedTests = append(c.failedTests, failureRecord{k, name, out})

		return
	}

	c.pkgFailed = true
}

func report(c *jsonlCollector, waitErr error) int {
	totalCases := c.nPass + c.nFail + c.nSkip

	if len(c.failedTests) == 0 && !c.pkgFailed {
		if waitErr != nil {
			fmt.Fprintln(os.Stderr, "FAIL")

			return exitCode(waitErr)
		}

		printOKSummary(totalCases, c.nPass, c.nSkip)

		return 0
	}

	nFailedUnique := printFailureDetails(c)
	printFailureSummary(c, totalCases, nFailedUnique)

	if waitErr != nil {
		return exitCode(waitErr)
	}

	if len(c.failedTests) > 0 || c.pkgFailed {
		return 1
	}

	return 0
}

func printOKSummary(totalCases, nPass, nSkip int) {
	fmt.Printf("OK (%d test cases: %d passed", totalCases, nPass)

	if nSkip > 0 {
		fmt.Printf(", %d skipped", nSkip)
	}

	fmt.Println(")")
}

func printFailureDetails(c *jsonlCollector) (nFailedUnique int) {
	seen := make(map[string]bool)
	for _, f := range c.failedTests {
		if seen[f.key] {
			continue
		}
		seen[f.key] = true
		nFailedUnique++

		fmt.Printf("--- FAIL: %s\n%s", f.name, f.out)

		if f.out != "" && !strings.HasSuffix(f.out, "\n") {
			fmt.Println()
		}
	}

	if c.pkgFailed {
		for pkg, b := range c.pkgOut {
			if b.Len() == 0 {
				continue
			}

			s := b.String()

			fmt.Printf("--- FAIL: %s (package)\n%s", pkg, s)

			if !strings.HasSuffix(s, "\n") {
				fmt.Println()
			}
		}
	}

	return nFailedUnique
}

func printFailureSummary(c *jsonlCollector, totalCases, nFailedUnique int) {
	if len(c.failedTests) > 0 {
		fmt.Printf("\n%d failed (%d test cases total: %d passed", nFailedUnique, totalCases, c.nPass)

		if c.nSkip > 0 {
			fmt.Printf(", %d skipped", c.nSkip)
		}

		fmt.Println(")")

		return
	}

	if c.pkgFailed && totalCases > 0 {
		fmt.Printf("\n0 failed (%d test cases total: %d passed", totalCases, c.nPass)

		if c.nSkip > 0 {
			fmt.Printf(", %d skipped", c.nSkip)
		}

		fmt.Println(")")
	}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}

	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}

	return 1
}
