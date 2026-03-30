// Command run-e2e runs e2e parity tests via go test -json and prints only failures, or OK
// with a test-case count (pass/fail/skip events from test2json).
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
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
	pattern := "TestE2E_"
	extra := os.Args[1:]
	if len(extra) > 0 && !strings.HasPrefix(extra[0], "-") {
		pattern = extra[0]
		extra = extra[1:]
	}

	args := append([]string{"test", "./e2e/...", "-json", "-run", pattern}, extra...)
	cmd := exec.Command("go", args...)
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "run-e2e: %v\n", err)
		os.Exit(1)
	}

	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "run-e2e: %v\n", err)
		os.Exit(1)
	}

	testOut := make(map[string]*strings.Builder)
	pkgOut := make(map[string]*strings.Builder)
	var failedTests []struct {
		key, name string
		out       string
	}
	var pkgFailed bool
	var nPass, nFail, nSkip int

	key := func(pkg, test string) string { return pkg + "\x00" + test }

	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		line := sc.Bytes()
		var ev testEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			continue
		}

		switch ev.Action {
		case "output":
			if ev.Test != "" {
				k := key(ev.Package, ev.Test)
				if testOut[k] == nil {
					testOut[k] = &strings.Builder{}
				}
				testOut[k].WriteString(ev.Output)
			} else if ev.Package != "" {
				if pkgOut[ev.Package] == nil {
					pkgOut[ev.Package] = &strings.Builder{}
				}
				pkgOut[ev.Package].WriteString(ev.Output)
			}
		case "fail":
			if ev.Test != "" {
				nFail++
				k := key(ev.Package, ev.Test)
				out := ""
				if b := testOut[k]; b != nil {
					out = b.String()
				}
				name := ev.Package + " " + ev.Test
				failedTests = append(failedTests, struct {
					key, name string
					out       string
				}{k, name, out})
			} else {
				pkgFailed = true
			}
		case "pass":
			if ev.Test != "" {
				nPass++
				delete(testOut, key(ev.Package, ev.Test))
			}
		case "skip":
			if ev.Test != "" {
				nSkip++
				delete(testOut, key(ev.Package, ev.Test))
			}
		}
	}

	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "run-e2e: reading test output: %v\n", err)
		_ = cmd.Process.Kill()
		os.Exit(1)
	}

	waitErr := cmd.Wait()

	totalCases := nPass + nFail + nSkip

	if len(failedTests) == 0 && !pkgFailed {
		if waitErr != nil {
			fmt.Fprintln(os.Stderr, "FAIL")
			os.Exit(exitCode(waitErr))
		}
		fmt.Printf("OK (%d test cases: %d passed", totalCases, nPass)
		if nSkip > 0 {
			fmt.Printf(", %d skipped", nSkip)
		}
		fmt.Println(")")
		return
	}

	seen := make(map[string]bool)
	var nFailedUnique int
	for _, f := range failedTests {
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

	if pkgFailed {
		for pkg, b := range pkgOut {
			if b.Len() > 0 {
				fmt.Printf("--- FAIL: %s (package)\n%s", pkg, b.String())
				if !strings.HasSuffix(b.String(), "\n") {
					fmt.Println()
				}
			}
		}
	}

	if len(failedTests) > 0 {
		fmt.Printf("\n%d failed (%d test cases total: %d passed", nFailedUnique, totalCases, nPass)
		if nSkip > 0 {
			fmt.Printf(", %d skipped", nSkip)
		}
		fmt.Println(")")
	} else if pkgFailed && totalCases > 0 {
		fmt.Printf("\n0 failed (%d test cases total: %d passed", totalCases, nPass)
		if nSkip > 0 {
			fmt.Printf(", %d skipped", nSkip)
		}
		fmt.Println(")")
	}

	if waitErr != nil {
		os.Exit(exitCode(waitErr))
	}

	if len(failedTests) > 0 || pkgFailed {
		os.Exit(1)
	}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode()
	}
	return 1
}
