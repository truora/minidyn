package language

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/truora/minidyn/types"
)

func TestExtractPath_identifierAndAlias(t *testing.T) {
	t.Parallel()

	env := NewEnvironment()
	env.Aliases = map[string]string{"#n": "real"}

	l := NewLexer("#n")
	p := NewParser(l)
	exprs := p.ParseProjectionExpression()
	checkParserErrors(t, p)

	path, err := ExtractPath(exprs[0], env)
	if err != nil {
		t.Fatal(err)
	}

	want := []PathElement{{Kind: PathKindMapKey, Key: "real"}}
	if diff := cmp.Diff(want, path); diff != "" {
		t.Fatal(diff)
	}
}

func TestExtractPath_nested(t *testing.T) {
	t.Parallel()

	env := NewEnvironment()

	l := NewLexer("A.B[1]")
	p := NewParser(l)
	exprs := p.ParseProjectionExpression()
	checkParserErrors(t, p)

	path, err := ExtractPath(exprs[0], env)
	if err != nil {
		t.Fatal(err)
	}

	want := []PathElement{
		{Kind: PathKindMapKey, Key: "A"},
		{Kind: PathKindMapKey, Key: "B"},
		{Kind: PathKindListIndex, Index: 1},
	}
	if diff := cmp.Diff(want, path); diff != "" {
		t.Fatal(diff)
	}
}

func TestSetProjectedPath_topLevelAndNested(t *testing.T) {
	t.Parallel()

	target := map[string]*types.Item{}

	s := "hello"
	SetProjectedPath(target, []PathElement{{Kind: PathKindMapKey, Key: "x"}}, types.Item{S: &s})

	inner := "inner"
	SetProjectedPath(target, []PathElement{
		{Kind: PathKindMapKey, Key: "m"},
		{Kind: PathKindMapKey, Key: "k"},
	}, types.Item{S: &inner})

	want := map[string]*types.Item{
		"x": {S: &s},
		"m": {M: map[string]*types.Item{"k": {S: &inner}}},
	}
	if diff := cmp.Diff(want, target); diff != "" {
		t.Fatal(diff)
	}
}

func TestSetProjectedPath_listIndex(t *testing.T) {
	t.Parallel()

	target := map[string]*types.Item{}
	elt := "e"
	SetProjectedPath(target, []PathElement{
		{Kind: PathKindMapKey, Key: "L"},
		{Kind: PathKindListIndex, Index: 2},
	}, types.Item{S: &elt})

	want := map[string]*types.Item{
		"L": {L: []*types.Item{nil, nil, {S: &elt}}},
	}
	if diff := cmp.Diff(want, target); diff != "" {
		t.Fatal(diff)
	}
}
