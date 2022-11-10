package interpreter

import (
	"errors"
	"reflect"
	"testing"

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
			S: types.ToString("a"),
		},
		"n": {
			N: types.ToString("1"),
		},
		"b": {
			BOOL: &boolTrue,
		},
		"txt": {
			S: types.ToString("hello world"),
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
						S: types.ToString("a"),
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
						S: types.ToString("a"),
					},
				},
				Attributes: map[string]*types.Item{
					":a": {
						N: types.ToString("1"),
					},
				},
				Aliases: map[string]string{
					"#t": "two",
				},
			},
			output: map[string]*types.Item{
				"a": {
					N: types.ToString("1"),
				},
				"two": {
					N: types.ToString("2"),
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
						S: types.ToString("a"),
					},
				},
				Attributes: map[string]*types.Item{
					":a": {
						N: types.ToString("1"),
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
