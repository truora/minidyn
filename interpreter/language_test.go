package interpreter

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
	item := map[string]*dynamodb.AttributeValue{
		"a": {
			S: aws.String("a"),
		},
		"n": {
			N: aws.String("1"),
		},
		"b": {
			BOOL: aws.Bool(true),
		},
		"txt": {
			S: aws.String("hello world"),
		},
	}

	testCases := []matchTestCase{
		{
			name: "successful",
			input: MatchInput{
				TableName:  "test",
				Expression: ":a = a",
				Item:       item,
				Attributes: map[string]*dynamodb.AttributeValue{
					":a": {
						S: aws.String("a"),
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
				Attributes: map[string]*dynamodb.AttributeValue{},
			},
			expectedErr: ErrSyntaxError,
		},
		{
			name: "type mismatch",
			input: MatchInput{
				TableName:  "test",
				Expression: "contains(txt, :b)",
				Item:       item,
				Attributes: map[string]*dynamodb.AttributeValue{
					":b": {
						BOOL: aws.Bool(true),
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
	output      map[string]*dynamodb.AttributeValue
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
				Expression: "SET two = :a + :a, a = :a",
				Item: map[string]*dynamodb.AttributeValue{
					"a": {
						S: aws.String("a"),
					},
				},
				Attributes: map[string]*dynamodb.AttributeValue{
					":a": {
						N: aws.String("1"),
					},
				},
			},
			output: map[string]*dynamodb.AttributeValue{
				"a": {
					N: aws.String("1"),
				},
				"two": {
					N: aws.String("2"),
				},
			},
		},
		{
			name: "syntax error",
			input: UpdateInput{
				TableName:  "test",
				Expression: "SET",
				Item: map[string]*dynamodb.AttributeValue{
					"a": {
						S: aws.String("a"),
					},
				},
				Attributes: map[string]*dynamodb.AttributeValue{
					":a": {
						N: aws.String("1"),
					},
				},
			},
			expectedErr: ErrSyntaxError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			updateTestCaseVerify(tc, t)
		})
	}
}
