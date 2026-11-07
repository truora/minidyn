package interpreter

import (
	"errors"
	"testing"

	"github.com/truora/minidyn/types"
)

func operandCountMessage(label, function, count string) string {
	return "Invalid " + label + ": Incorrect number of operands for operator or function; " +
		"operator or function: " + function + ", number of operands: " + count
}

func TestLanguageMatch_wrongOperandCount(t *testing.T) {
	item := map[string]*types.Item{"a": {S: new("x")}}
	attrs := map[string]*types.Item{":v": {S: new("x")}}

	cases := []struct {
		expr     string
		function string
		count    string
	}{
		{"contains(a)", "contains", "1"},
		{"begins_with(a)", "begins_with", "1"},
		{"attribute_type(a)", "attribute_type", "1"},
		{"contains(a, :v, :v)", "contains", "3"},
		{"attribute_exists(a, a)", "attribute_exists", "2"},
		{"size(a, a) > :v", "size", "2"},
	}

	labels := map[ExpressionType]string{
		ExpressionTypeConditional: "ConditionExpression",
		ExpressionTypeFilter:      "FilterExpression",
	}

	for _, tc := range cases {
		for typ, label := range labels {
			// The item is irrelevant: an empty one must fail the same way.
			for _, in := range []map[string]*types.Item{item, {}} {
				matched, err := (&Language{}).Match(MatchInput{
					TableName: "t", Expression: tc.expr, ExpressionType: typ, Item: in, Attributes: attrs,
				})
				if err == nil || matched {
					t.Fatalf("%s %q: expected an error, matched=%v", label, tc.expr, matched)
				}

				if want := operandCountMessage(label, tc.function, tc.count); err.Error() != want {
					t.Fatalf("got %q want %q", err.Error(), want)
				}

				if !errors.Is(err, ErrSyntaxError) {
					t.Fatalf("expected error to unwrap to ErrSyntaxError, got %v", err)
				}
			}
		}
	}
}

func TestLanguageUpdate_wrongOperandCount(t *testing.T) {
	cases := []struct {
		expr     string
		function string
		count    string
	}{
		{"SET a = if_not_exists(a)", "if_not_exists", "1"},
		{"SET a = if_not_exists(a, :v, :v)", "if_not_exists", "3"},
		{"SET a = list_append(a)", "list_append", "1"},
		{"SET a = list_append(a, a, a)", "list_append", "3"},
	}

	for _, tc := range cases {
		err := (&Language{}).Update(UpdateInput{
			TableName: "t", Expression: tc.expr, Item: map[string]*types.Item{},
			Attributes: map[string]*types.Item{":v": {S: new("x")}},
		})
		if err == nil {
			t.Fatalf("%q: expected an error", tc.expr)
		}

		if want := operandCountMessage("UpdateExpression", tc.function, tc.count); err.Error() != want {
			t.Fatalf("got %q want %q", err.Error(), want)
		}

		if !errors.Is(err, ErrSyntaxError) {
			t.Fatalf("expected error to unwrap to ErrSyntaxError, got %v", err)
		}
	}
}

func TestValidateFunctionOperandCounts(t *testing.T) {
	err := ValidateFunctionOperandCounts("FilterExpression", "contains(a)")
	if err == nil || err.Error() != operandCountMessage("FilterExpression", "contains", "1") {
		t.Fatalf("unexpected error %v", err)
	}

	if !errors.Is(err, ErrSyntaxError) {
		t.Fatalf("expected error to unwrap to ErrSyntaxError, got %v", err)
	}

	for _, expr := range []string{"", "contains(a, :v)", "a = :v", "contains(a"} {
		if err := ValidateFunctionOperandCounts("FilterExpression", expr); err != nil {
			t.Fatalf("%q: unexpected error %v", expr, err)
		}
	}
}
