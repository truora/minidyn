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

func TestExtractPath_listIndex_nonNumericIdentifierEval(t *testing.T) {
	t.Parallel()

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*types.Item{
		"L": {L: []*types.Item{{S: new("a")}, {S: new("b")}, {S: new("c")}}},
		"k": {N: new("2")},
	})
	if err != nil {
		t.Fatal(err)
	}

	l := NewLexer("L[k]")
	p := NewParser(l)
	exprs := p.ParseProjectionExpression()
	checkParserErrors(t, p)

	path, err := ExtractPath(exprs[0], env)
	if err != nil {
		t.Fatal(err)
	}

	want := []PathElement{
		{Kind: PathKindMapKey, Key: "L"},
		{Kind: PathKindListIndex, Index: 2},
	}
	if diff := cmp.Diff(want, path); diff != "" {
		t.Fatal(diff)
	}
}

//go:fix inline
func ptrString(s string) *string {
	return new(s)
}

func TestExtractPathFromIndex_unsupportedIndexType(t *testing.T) {
	t.Parallel()

	ie := &IndexExpression{
		Type:  ObjectTypeString,
		Left:  &Identifier{Value: "x"},
		Index: &Identifier{Value: "y"},
	}

	_, err := extractPathFromIndex(ie, nil)
	if err == nil {
		t.Fatal("expected error for unsupported index type")
	}
}

func TestSetProjectedPath_threeMapSegments(t *testing.T) {
	t.Parallel()

	target := map[string]*types.Item{}
	val := "deep"

	SetProjectedPath(target, []PathElement{
		{Kind: PathKindMapKey, Key: "a"},
		{Kind: PathKindMapKey, Key: "b"},
		{Kind: PathKindMapKey, Key: "c"},
	}, types.Item{S: &val})

	want := map[string]*types.Item{
		"a": {M: map[string]*types.Item{
			"b": {M: map[string]*types.Item{
				"c": {S: &val},
			}},
		}},
	}
	if diff := cmp.Diff(want, target); diff != "" {
		t.Fatal(diff)
	}
}

func TestAppendListIndexSegment_invalidIndexExpression(t *testing.T) {
	t.Parallel()

	_, err := appendListIndexSegment([]PathElement{{Kind: PathKindMapKey, Key: "L"}}, &PrefixExpression{}, NewEnvironment())
	if err == nil {
		t.Fatal("expected error when list index is not an identifier")
	}
}

func TestExtractPath_listIndex_evaluatesToNonNumber(t *testing.T) {
	t.Parallel()

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*types.Item{
		"L": {L: []*types.Item{{S: new("x")}}},
		"k": {S: new("text")},
	})
	if err != nil {
		t.Fatal(err)
	}

	l := NewLexer("L[k]")
	p := NewParser(l)
	exprs := p.ParseProjectionExpression()
	checkParserErrors(t, p)

	_, err = ExtractPath(exprs[0], env)
	if err == nil {
		t.Fatal("expected error when index evaluates to non-number")
	}
}

func TestExtractPathListIndex_reservedWordEval(t *testing.T) {
	t.Parallel()

	env := NewEnvironment()

	_, err := extractPathListIndex(&Identifier{
		Token: Token{Type: IDENT, Literal: "OR"},
		Value: "OR",
	}, env)
	if err == nil {
		t.Fatal("expected reserved word index to error")
	}
}
