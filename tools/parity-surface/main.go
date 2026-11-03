// Command parity-surface reports the parts of the DynamoDB API that minidyn's server
// silently ignores, by comparing the server package with the AWS SDK:
//
//   - operation: SDK operations with no case in the server's dispatch switch;
//   - request:   fields of server/requests.go that are decoded but never read, so a caller's
//     setting (Select, ReturnValues, ...) has no effect;
//   - response:  SDK output fields (and table/index description fields) the server never
//     sets, so callers always see zero values.
//
// Usage (from the repository root):
//
//	go run ./tools/parity-surface          # print every gap
//	go run ./tools/parity-surface -check   # fail on gaps missing from the allowlist,
//	                                       # and on allowlist entries that are now covered
//
// The allowlist (tools/parity-surface/allowlist.txt) holds one "<kind> <name>" per line;
// text after '#' is a comment, typically the parity issue or GitHub issue that tracks it.
// -check keeps the list honest in both directions: a new SDK field or a server regression
// must be triaged, and a fixed gap must be removed from the list.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

func main() {
	root := flag.String("root", ".", "repository root")
	allowPath := flag.String("allowlist", "tools/parity-surface/allowlist.txt", "known gaps file")
	check := flag.Bool("check", false, "fail on gaps not in the allowlist and on stale allowlist entries")

	flag.Parse()

	findings, err := analyze(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parity-surface: %v\n", err)
		os.Exit(2)
	}

	if !*check {
		printReport(os.Stdout, findings)

		return
	}

	allow, err := readAllowlist(*allowPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parity-surface: %v\n", err)
		os.Exit(2)
	}

	if !runCheck(os.Stdout, findings, allow) {
		os.Exit(1)
	}
}

func printReport(w io.Writer, findings []finding) {
	byKind := map[string][]finding{}
	for _, f := range findings {
		byKind[f.Kind] = append(byKind[f.Kind], f)
	}

	for _, kind := range []string{kindOperation, kindRequest, kindResponse} {
		fs := byKind[kind]
		_, _ = fmt.Fprintf(w, "== %s (%d)\n", kind, len(fs))

		for _, f := range fs {
			_, _ = fmt.Fprintf(w, "%s %-60s %s\n", f.Kind, f.Name, f.Detail)
		}
	}
}

// readAllowlist returns the allowlisted keys ("<kind> <name>").
func readAllowlist(path string) (map[string]bool, error) {
	f, err := os.Open(path) //nolint:gosec // path is a developer-supplied flag
	if err != nil {
		return nil, err
	}

	defer func() { _ = f.Close() }()

	return parseAllowlist(f)
}

func parseAllowlist(r io.Reader) (map[string]bool, error) {
	allow := map[string]bool{}
	sc := bufio.NewScanner(r)

	for line := 1; sc.Scan(); line++ {
		text := sc.Text()
		if i := strings.Index(text, "#"); i >= 0 {
			text = text[:i]
		}

		fields := strings.Fields(text)

		switch len(fields) {
		case 0:
			continue
		case 2:
			allow[fields[0]+" "+fields[1]] = true
		default:
			return nil, fmt.Errorf("allowlist line %d: want \"<kind> <name>\", got %q", line, sc.Text())
		}
	}

	return allow, sc.Err()
}

// runCheck prints new gaps and stale allowlist entries and reports whether there were none.
func runCheck(w io.Writer, findings []finding, allow map[string]bool) bool {
	found := map[string]bool{}

	var fresh []finding

	for _, f := range findings {
		found[f.key()] = true

		if !allow[f.key()] {
			fresh = append(fresh, f)
		}
	}

	var stale []string

	for k := range allow {
		if !found[k] {
			stale = append(stale, k)
		}
	}

	sort.Strings(stale)

	for _, f := range fresh {
		_, _ = fmt.Fprintf(w, "NEW GAP   %s (%s): add a parity test or implement it, then allowlist if deferred\n",
			f.key(), f.Detail)
	}

	for _, k := range stale {
		_, _ = fmt.Fprintf(w, "COVERED   %s: no longer a gap, remove it from the allowlist\n", k)
	}

	if len(fresh) == 0 && len(stale) == 0 {
		_, _ = fmt.Fprintf(w, "OK (%d known gaps allowlisted)\n", len(findings))

		return true
	}

	return false
}
