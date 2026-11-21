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

func TestLookupProjectedPath(t *testing.T) {
	t.Parallel()

	one, two := "1", "2"
	item := map[string]*types.Item{
		"a": {N: &one},
		"m": {M: map[string]*types.Item{"k": {L: []*types.Item{{N: &two}}}}},
	}
	key := func(k string) PathElement { return PathElement{Kind: PathKindMapKey, Key: k} }
	idx := func(i int64) PathElement { return PathElement{Kind: PathKindListIndex, Index: i} }

	tests := []struct {
		name string
		path []PathElement
		want *string
	}{
		{name: "top level", path: []PathElement{key("a")}, want: &one},
		{name: "nested", path: []PathElement{key("m"), key("k"), idx(0)}, want: &two},
		{name: "empty path", path: nil},
		{name: "list first", path: []PathElement{idx(0)}},
		{name: "missing root", path: []PathElement{key("z")}},
		{name: "missing nested key", path: []PathElement{key("m"), key("z")}},
		{name: "index out of range", path: []PathElement{key("m"), key("k"), idx(1)}},
		{name: "negative index", path: []PathElement{key("m"), key("k"), idx(-1)}},
		{name: "key into scalar", path: []PathElement{key("a"), key("x")}},
	}

	for _, tt := range tests {
		got, ok := LookupProjectedPath(item, tt.path)
		if tt.want == nil {
			if ok {
				t.Fatalf("%s: expected no value", tt.name)
			}

			continue
		}

		if !ok || *got.N != *tt.want {
			t.Fatalf("%s: got %v ok=%v", tt.name, got, ok)
		}
	}
}

func TestRootAttributes(t *testing.T) {
	t.Parallel()

	aliases := map[string]string{"#n": "real", "#m": "other"}

	tests := []struct {
		name string
		expr string
		want []string
	}{
		{name: "Comparison", expr: "a = :v", want: []string{"a"}},
		{name: "FunctionsAndValuesAreNotAttributes", expr: "attribute_exists(a) AND begins_with(b, :v)", want: []string{"a", "b"}},
		{name: "AliasResolved", expr: "#n = :v", want: []string{"real"}},
		{name: "NestedPathKeepsRoot", expr: "a.b.c = :v AND d[0] = :v", want: []string{"a", "d"}},
		{name: "AliasedNestedSegmentIsNotARoot", expr: "a.#m = :v", want: []string{"a"}},
		{name: "OnceInOrderOfAppearance", expr: "b = :v OR a = :v OR b <> :v", want: []string{"b", "a"}},
		{name: "Not", expr: "NOT a = :v", want: []string{"a"}},
		{name: "Between", expr: "a BETWEEN :x AND :y", want: []string{"a"}},
		{name: "In", expr: "a IN (:x, :y)", want: []string{"a"}},
		{name: "SizeOperand", expr: "size(a) > :v", want: []string{"a"}},
		{name: "OperandsOfBoth", expr: "a = b", want: []string{"a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := NewParser(NewLexer(tt.expr))
			cond := p.ParseConditionalExpression()
			checkParserErrors(t, p)

			if diff := cmp.Diff(tt.want, RootAttributes(aliases, cond.Expression)); diff != "" {
				t.Fatal(diff)
			}
		})
	}

	t.Run("Projection", func(t *testing.T) {
		t.Parallel()

		p := NewParser(NewLexer("#n, a.b, c[1], a"))
		exprs := p.ParseProjectionExpression()
		checkParserErrors(t, p)

		if diff := cmp.Diff([]string{"real", "a", "c"}, RootAttributes(aliases, exprs...)); diff != "" {
			t.Fatal(diff)
		}
	})
}

// A projection builds lists with a nil hole for every element it did not select. DynamoDB
// answers with only the selected elements, in list order (#262). SetProjectedPath reports the
// lists it padded, and only those are compacted.
func TestSetProjectedPath_reportsPaddedLists(t *testing.T) {
	t.Parallel()

	s := func(v string) *types.Item { return &types.Item{S: &v} }
	key := func(k string) PathElement { return PathElement{Kind: PathKindMapKey, Key: k} }
	idx := func(i int64) PathElement { return PathElement{Kind: PathKindListIndex, Index: i} }

	projected := map[string]*types.Item{}

	var padded []*types.Item

	for _, tt := range []struct {
		path []PathElement
		val  string
	}{
		{[]PathElement{key("l"), idx(3)}, "d"},
		{[]PathElement{key("l"), idx(1)}, "b"},
		{[]PathElement{key("ll"), idx(1), idx(1)}, "r"},
		{[]PathElement{key("lm"), idx(1), key("k"), idx(1)}, "v"},
	} {
		padded = append(padded, SetProjectedPath(projected, tt.path, *s(tt.val))...)
	}

	if got := SetProjectedPath(projected, []PathElement{key("s")}, *s("x")); len(got) != 0 {
		t.Fatalf("a path without a list index pads nothing, got=%d", len(got))
	}

	if got := SetProjectedPath(projected, []PathElement{key("l"), idx(2)}, *s("c")); len(got) != 0 {
		t.Fatalf("a list that is long enough is not padded, got=%d", len(got))
	}

	CompactPaddedLists(padded)

	want := map[string]*types.Item{
		"l":  {L: []*types.Item{s("b"), s("c"), s("d")}},
		"ll": {L: []*types.Item{{L: []*types.Item{s("r")}}}},
		"lm": {L: []*types.Item{{M: map[string]*types.Item{"k": {L: []*types.Item{s("v")}}}}}},
		"s":  s("x"),
	}
	if diff := cmp.Diff(want, projected); diff != "" {
		t.Fatal(diff)
	}
}

// A selected NULL element is a value, not a hole.
func TestCompactPaddedLists_keepsNullElements(t *testing.T) {
	t.Parallel()

	null := true
	list := &types.Item{L: []*types.Item{nil, {NULL: &null}}}

	CompactPaddedLists([]*types.Item{list})

	if len(list.L) != 1 || list.L[0].NULL == nil {
		t.Fatalf("expected only the NULL element, got=%v", list.L)
	}
}
