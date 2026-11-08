package interpreter

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/truora/minidyn/types"
)

type matchTestCase struct {
	name        string
	input       MatchInput
	output      bool
	expectedErr error
	pending     bool
}

func matchTestCaseVerify(tc matchTestCase, t *testing.T) {
	if tc.pending {
		t.Skipf("%q expression support is not implemented yet", tc.input.Expression)
	}

	interpeter := Language{}

	actual, err := interpeter.Match(tc.input)
	if !errors.Is(err, tc.expectedErr) {
		t.Errorf("%q failed with unexpected error; expected=%v, got=%v", tc.input.Expression, tc.expectedErr, err)
	}

	if actual != tc.output {
		t.Errorf("%q return an unexpected result; expected=%v, got=%v", tc.input.Expression, tc.output, actual)
	}
}

func TestLanguageMatch(t *testing.T) {
	item := map[string]*types.Item{
		"a": {
			S: new("a"),
		},
		"n": {
			N: new("1"),
		},
		"b": {
			BOOL: &boolTrue,
		},
		"txt": {
			S: new("hello world"),
		},
	}

	testCases := []matchTestCase{
		{
			name: "successful",
			input: MatchInput{
				TableName:  "test",
				Expression: ":a = a",
				Item:       item,
				Attributes: map[string]*types.Item{
					":a": {
						S: new("a"),
					},
				},
			},
			output: true,
		},
		{
			name: "parentheses mismatch",
			input: MatchInput{
				TableName:  "test",
				Expression: "attribute_exists(b",
				Item:       item,
				Attributes: map[string]*types.Item{},
			},
			expectedErr: ErrSyntaxError,
		},
		{
			name: "type mismatch on stored value is false",
			input: MatchInput{
				TableName:  "test",
				Expression: "contains(n, :t) OR begins_with(b, :t) OR size(missing_attr) > :one",
				Item:       item,
				Attributes: map[string]*types.Item{
					":t":   {S: new("x")},
					":one": {N: new("1")},
				},
			},
		},
		{
			name: "static operand type error",
			input: MatchInput{
				TableName:  "test",
				Expression: "begins_with(txt, :n)",
				Item:       item,
				Attributes: map[string]*types.Item{
					":n": {N: new("1")},
				},
			},
			expectedErr: ErrSyntaxError,
		},
		{
			name: "empty expression",
			input: MatchInput{
				TableName:      "test",
				Expression:     "",
				Item:           item,
				Attributes:     map[string]*types.Item{},
				ExpressionType: ExpressionTypeConditional,
			},
			expectedErr: ErrSyntaxError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			matchTestCaseVerify(tc, t)
		})
	}
}

func TestLanguageMatch_keyConditionSyntaxErrorPrefix(t *testing.T) {
	interp := Language{}
	_, err := interp.Match(MatchInput{
		TableName:      "test",
		Expression:     "#a != :a",
		ExpressionType: ExpressionTypeKey,
		Item: map[string]*types.Item{
			"a": {S: new("a")},
		},
		Attributes: map[string]*types.Item{
			":a": {S: new("x")},
		},
		Aliases: map[string]string{"#a": "a"},
	})
	if !errors.Is(err, ErrSyntaxError) {
		t.Fatalf("want ErrSyntaxError, got %v", err)
	}

	const wantPrefix = "Invalid KeyConditionExpression: Syntax error;"
	if !strings.Contains(err.Error(), wantPrefix) {
		t.Fatalf("message should contain %q, got %q", wantPrefix, err.Error())
	}
}

type updateTestCase struct {
	name           string
	input          UpdateInput
	output         map[string]*types.Item
	expectedErr    error
	expectedErrMsg string
	pending        bool
}

func updateTestCaseVerify(tc updateTestCase, t *testing.T) {
	if tc.pending {
		t.Skipf("%q expression support is not implemented yet", tc.input.Expression)
	}

	interpeter := Language{}

	err := interpeter.Update(tc.input)
	if tc.expectedErrMsg != "" {
		if err == nil || err.Error() != tc.expectedErrMsg {
			t.Errorf("%q failed with unexpected error; expected=%q, got=%v", tc.input.Expression, tc.expectedErrMsg, err)
		}

		return
	}

	if tc.expectedErr != nil {
		if !errors.Is(err, tc.expectedErr) {
			t.Errorf("%q failed with unexpected error; expected=%v, got=%v", tc.input.Expression, tc.expectedErr, err)
		}

		return
	}

	if !reflect.DeepEqual(tc.input.Item, tc.output) {
		t.Errorf("%q return an unexpected result; expected=%v, got=%v", tc.input.Expression, tc.output, tc.input.Item)
	}
}

func TestLanguageUpdate(t *testing.T) {
	testCases := []updateTestCase{
		{
			name: "successful",
			input: UpdateInput{
				TableName:  "test",
				Expression: "SET #t = :a + :a, a = :a",
				Item: map[string]*types.Item{
					"a": {
						S: new("a"),
					},
				},
				Attributes: map[string]*types.Item{
					":a": {
						N: new("1"),
					},
				},
				Aliases: map[string]string{
					"#t": "two",
				},
			},
			output: map[string]*types.Item{
				"a": {
					N: new("1"),
				},
				"two": {
					N: new("2"),
				},
			},
		},
		{
			name: "runtime validation error",
			input: UpdateInput{
				TableName:  "test",
				Expression: "SET",
				Item: map[string]*types.Item{
					"a": {
						S: new("a"),
					},
				},
				Attributes: map[string]*types.Item{
					":a": {
						N: new("1"),
					},
				},
			},
			expectedErrMsg: "SET expression must have at least one action",
		},
		{
			name: "remove top-level attributes",
			input: UpdateInput{
				TableName:  "test",
				Expression: "REMOVE lease_owner, lease_until",
				Item: map[string]*types.Item{
					"id":          {S: new("001")},
					"lease_owner": {S: new("worker-1")},
					"lease_until": {N: new("1700000000")},
				},
			},
			output: map[string]*types.Item{
				"id": {S: new("001")},
			},
		},
		{
			name: "set and remove with alias",
			input: UpdateInput{
				TableName:  "test",
				Expression: "SET lvl = :lvl REMOVE second_type, #n",
				Item: map[string]*types.Item{
					"id":          {S: new("001")},
					"lvl":         {N: new("5")},
					"name":        {S: new("Bulbasaur")},
					"second_type": {S: new("poison")},
				},
				Attributes: map[string]*types.Item{
					":lvl": {N: new("6")},
				},
				Aliases: map[string]string{
					"#n": "name",
				},
			},
			output: map[string]*types.Item{
				"id":  {S: new("001")},
				"lvl": {N: new("6")},
			},
		},
		{
			name: "remove missing attribute is a no-op",
			input: UpdateInput{
				TableName:  "test",
				Expression: "REMOVE not_there",
				Item: map[string]*types.Item{
					"id": {S: new("001")},
				},
			},
			output: map[string]*types.Item{
				"id": {S: new("001")},
			},
		},
		{
			name: "remove nested attribute",
			input: UpdateInput{
				TableName:  "test",
				Expression: "REMOVE m.a",
				Item: map[string]*types.Item{
					"id": {S: new("001")},
					"m": {M: map[string]*types.Item{
						"a": {S: new("x")},
						"b": {S: new("y")},
					}},
				},
			},
			output: map[string]*types.Item{
				"id": {S: new("001")},
				"m": {M: map[string]*types.Item{
					"b": {S: new("y")},
				}},
			},
		},
		{
			name: "typo",
			input: UpdateInput{
				TableName:  "test",
				Expression: "REMOVE ,",
				Aliases: map[string]string{
					"#t": "two",
				},
			},
			output:      nil,
			expectedErr: ErrSyntaxError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			updateTestCaseVerify(tc, t)
		})
	}
}

func TestLanguageProject(t *testing.T) {
	t.Parallel()

	inner := "v"
	item := map[string]*types.Item{
		"top": {S: new("x")},
		"nested": {
			M: map[string]*types.Item{
				"k": {S: &inner},
			},
		},
	}

	interp := Language{}
	out, err := interp.Project(ProjectInput{
		Expression: "top, nested.k",
		Item:       item,
		Aliases:    nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	topWant := "x"
	want := map[string]*types.Item{
		"top": {S: &topWant},
		"nested": {
			M: map[string]*types.Item{
				"k": {S: &inner},
			},
		},
	}
	if diff := cmp.Diff(want, out); diff != "" {
		t.Fatal(diff)
	}
}

func TestLanguageProject_skipsMissingPath(t *testing.T) {
	t.Parallel()

	item := map[string]*types.Item{
		"present": {S: new("a")},
	}

	interp := Language{}
	out, err := interp.Project(ProjectInput{
		Expression: "present, notThere",
		Item:       item,
	})
	if err != nil {
		t.Fatal(err)
	}

	val := "a"
	want := map[string]*types.Item{
		"present": {S: &val},
	}
	if diff := cmp.Diff(want, out); diff != "" {
		t.Fatal(diff)
	}
}

func TestLanguageProject_rejectsUpdateExpressionSyntax(t *testing.T) {
	t.Parallel()

	// ProjectionExpression only accepts path expressions (comma-separated),
	// not UpdateExpression clauses like SET … = …
	interp := Language{}
	_, err := interp.Project(ProjectInput{
		Expression: "SET #n = :x",
		Item: map[string]*types.Item{
			"n": {S: new("v")},
		},
		Aliases: map[string]string{"#n": "n"},
	})
	if err == nil {
		t.Fatal("expected error when projection string uses SET clause")
	}
	if !errors.Is(err, ErrSyntaxError) {
		t.Fatalf("expected ErrSyntaxError, got %v", err)
	}
}

func BenchmarkLanguageMatch(b *testing.B) {
	li := Language{}
	input := MatchInput{
		TableName:  "test",
		Expression: ":a = a",
		Item:       map[string]*types.Item{"a": {S: new("a")}},
		Attributes: map[string]*types.Item{":a": {S: new("a")}},
	}

	b.ReportAllocs()

	for b.Loop() {
		if _, err := li.Match(input); err != nil {
			b.Fatal(err)
		}
	}
}

// A projected path through a value that is not a map or list is left out, not an error (#143).
func TestLanguageProject_skipsPathThroughOtherTypes(t *testing.T) {
	interp := &Language{}

	out, err := interp.Project(ProjectInput{
		Expression: "id, nul[0], nul.x, s.x, s[0], l.k, m[0], l[9]",
		Item: map[string]*types.Item{
			"id":  {S: new("1")},
			"nul": {NULL: &boolTrue},
			"s":   {S: new("abc")},
			"l":   {L: []*types.Item{{S: new("a")}}},
			"m":   {M: map[string]*types.Item{"k": {S: new("v")}}},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(out) != 1 || out["id"] == nil {
		t.Fatalf("expected only id, got=%v", out)
	}
}
