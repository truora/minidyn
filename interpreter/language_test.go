package interpreter

import (
	"errors"
	"reflect"
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
			name: "type mismatch",
			input: MatchInput{
				TableName:  "test",
				Expression: "contains(txt, :b)",
				Item:       item,
				Attributes: map[string]*types.Item{
					":b": {
						BOOL: &boolTrue,
					},
				},
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

type updateTestCase struct {
	name        string
	input       UpdateInput
	output      map[string]*types.Item
	expectedErr error
	pending     bool
}

func updateTestCaseVerify(tc updateTestCase, t *testing.T) {
	if tc.pending {
		t.Skipf("%q expression support is not implemented yet", tc.input.Expression)
	}

	interpeter := Language{}

	err := interpeter.Update(tc.input)
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
			name: "syntax error",
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
			expectedErr: ErrSyntaxError,
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
