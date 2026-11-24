package client_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReadmeSamplesMatchExamples keeps README.md code samples identical to the
// Example functions that go test compiles and runs, in both directions: every
// Example body appears in the README, and every ```go block of the README is an
// Example body, except the import block (its first line starts with "import ").
func TestReadmeSamplesMatchExamples(t *testing.T) {
	readme, err := os.ReadFile("../../README.md")
	require.NoError(t, err)

	funcBody := regexp.MustCompile(`(?s)\nfunc (Example\w+)\(\) \{\n(.*?)\n\}\n`)
	outputComment := regexp.MustCompile(`\n\t// Output:.*(\n\t//.*)*`)
	goBlock := regexp.MustCompile("(?s)```go\n(.*?)```")

	sources := map[string]string{
		"example_test.go":              exampleSource(t, "example_test.go"),
		"../../server/example_test.go": exampleSource(t, "../../server/example_test.go"),
	}

	examples := map[string]bool{}

	for file, src := range sources {
		matches := funcBody.FindAllStringSubmatch(src, -1)
		require.NotEmpty(t, matches, file)

		for _, m := range matches {
			sample := dedent(outputComment.ReplaceAllString(m[2], "")) + "\n"
			examples[sample] = true

			if !strings.Contains(string(readme), sample) {
				t.Errorf("README.md is out of sync with %s in %s; sample:\n%s", m[1], file, sample)
			}
		}
	}

	for _, m := range goBlock.FindAllStringSubmatch(string(readme), -1) {
		if strings.HasPrefix(m[1], "import ") {
			continue
		}

		if !examples[m[1]] {
			t.Errorf("README.md has a go block that is not an Example body:\n%s", m[1])
		}
	}
}

// dedent strips the function body's leading tab from every line.
func dedent(body string) string {
	lines := strings.Split(body, "\n")
	for i, l := range lines {
		lines[i] = strings.TrimPrefix(l, "\t")
	}

	return strings.Join(lines, "\n")
}

func exampleSource(t *testing.T, path string) string {
	t.Helper()

	src, err := os.ReadFile(path) //nolint:gosec // fixed paths inside the repository
	require.NoError(t, err)

	return string(src)
}
