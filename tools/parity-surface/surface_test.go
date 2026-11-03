package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const repoRoot = "../.."

func TestParseAllowlist(t *testing.T) {
	allow, err := parseAllowlist(strings.NewReader(`
# comment
request QueryInput.Select   # 004

operation ListTables
`))
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		"request QueryInput.Select": true,
		"operation ListTables":      true,
	}, allow)

	_, err = parseAllowlist(strings.NewReader("request\n"))
	require.ErrorContains(t, err, "line 1")
}

func TestRunCheck(t *testing.T) {
	findings := []finding{
		{Kind: kindRequest, Name: "QueryInput.Select", Detail: "decoded but never read"},
		{Kind: kindOperation, Name: "ListTables", Detail: "not dispatched"},
	}

	var buf bytes.Buffer

	ok := runCheck(&buf, findings, map[string]bool{
		"request QueryInput.Select": true,
		"operation ListTables":      true,
	})
	require.True(t, ok)
	require.Contains(t, buf.String(), "OK (2 known gaps allowlisted)")

	buf.Reset()

	ok = runCheck(&buf, findings, map[string]bool{
		"request QueryInput.Select":  true,
		"response QueryOutput.Count": true,
	})
	require.False(t, ok)
	require.Contains(t, buf.String(), "NEW GAP   operation ListTables")
	require.Contains(t, buf.String(), "COVERED   response QueryOutput.Count")
}

func TestAnalyzeServer(t *testing.T) {
	findings, err := analyze(repoRoot)
	require.NoError(t, err)

	keys := map[string]bool{}
	for _, f := range findings {
		keys[f.key()] = true
	}

	// Gaps that are known to exist.
	require.True(t, keys["operation CreateBackup"], "CreateBackup is not dispatched")
	require.True(t, keys["request QueryInput.Select"], "Select is never read")
	require.True(t, keys["response QueryOutput.ScannedCount"], "ScannedCount is never set")
	require.True(t, keys["response TableDescription.TableStatus"], "TableStatus is never set")

	// Surface that is implemented must not be reported.
	require.False(t, keys["operation Query"])
	require.False(t, keys["operation ListTables"])
	require.False(t, keys["request QueryInput.KeyConditionExpression"])
	require.False(t, keys["request UpdateItemInput.ReturnValuesOnConditionCheckFailure"])
	require.False(t, keys["response QueryOutput.LastEvaluatedKey"])
	require.False(t, keys["response TableDescription.KeySchema"])
}

// TestAllowlistUpToDate is `go run ./tools/parity-surface -check` as a unit test, so CI
// fails when a gap appears that nobody triaged, or when a fixed gap is still allowlisted.
func TestAllowlistUpToDate(t *testing.T) {
	findings, err := analyze(repoRoot)
	require.NoError(t, err)

	allow, err := readAllowlist("allowlist.txt")
	require.NoError(t, err)

	var buf bytes.Buffer
	if !runCheck(&buf, findings, allow) {
		t.Fatalf("tools/parity-surface/allowlist.txt is out of date:\n%s", buf.String())
	}
}

func TestCollectFieldUses_assignmentIsNotARead(t *testing.T) {
	const src = `package p

type In struct{ A, B, C int }

func f(i *In) int {
	i.A = 1
	i.C++

	return i.B
}

var _ = In{C: 2}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "p.go", src, 0)
	require.NoError(t, err)

	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	_, err = (&types.Config{}).Check("p", fset, []*ast.File{f}, info)
	require.NoError(t, err)

	reads, uses := fieldUses{}, fieldUses{}
	collectFieldUses(f, info, reads, uses)

	require.Equal(t, map[string]bool{"B": true}, reads["p.In"], "only i.B is read")
	require.Equal(t, map[string]bool{"A": true, "B": true, "C": true}, uses["p.In"], "all three are used")
}
