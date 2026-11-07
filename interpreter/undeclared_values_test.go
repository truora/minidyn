package interpreter

import (
	"errors"
	"testing"

	"github.com/truora/minidyn/types"
)

func assertUndeclaredValueError(t *testing.T, err error, expressionType, value string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected an error for undeclared %s value %q", expressionType, value)
	}

	want := "Invalid " + expressionType +
		": An expression attribute value used in expression is not defined; attribute value: " + value
	if err.Error() != want {
		t.Fatalf("error message mismatch\n got: %q\nwant: %q", err.Error(), want)
	}

	if !errors.Is(err, ErrSyntaxError) {
		t.Fatalf("expected error to unwrap to ErrSyntaxError, got %v", err)
	}
}

func TestUndeclaredExpressionAttributeValueErrors(t *testing.T) {
	t.Parallel()

	li := Language{}
	item := map[string]*types.Item{"id": {S: new("x")}}
	declared := map[string]*types.Item{":v": {S: new("y")}}

	t.Run("UpdateExpression", func(t *testing.T) {
		t.Parallel()

		err := li.Update(UpdateInput{Expression: "SET a = :nope", Item: item})
		assertUndeclaredValueError(t, err, "UpdateExpression", ":nope")
	})

	t.Run("UpdateExpression with another declared value", func(t *testing.T) {
		t.Parallel()

		err := li.Update(UpdateInput{Expression: "SET a = :nope", Item: item, Attributes: declared})
		assertUndeclaredValueError(t, err, "UpdateExpression", ":nope")
	})

	t.Run("ConditionExpression", func(t *testing.T) {
		t.Parallel()

		_, err := li.Match(MatchInput{Expression: "a = :nope", ExpressionType: ExpressionTypeConditional, Item: item})
		assertUndeclaredValueError(t, err, "ConditionExpression", ":nope")
	})

	t.Run("FilterExpression", func(t *testing.T) {
		t.Parallel()

		_, err := li.Match(MatchInput{
			Expression: "a = :nope", ExpressionType: ExpressionTypeFilter, Item: item, Attributes: declared,
		})
		assertUndeclaredValueError(t, err, "FilterExpression", ":nope")
	})

	t.Run("KeyConditionExpression", func(t *testing.T) {
		t.Parallel()

		_, err := li.Match(MatchInput{Expression: "id = :nope", ExpressionType: ExpressionTypeKey, Item: item})
		assertUndeclaredValueError(t, err, "KeyConditionExpression", ":nope")
	})

	t.Run("an undeclared name is reported before a value", func(t *testing.T) {
		t.Parallel()

		_, err := li.Match(MatchInput{Expression: "#n = :nope", ExpressionType: ExpressionTypeFilter, Item: item})
		assertUndeclaredNameError(t, err, "FilterExpression", "#n")
	})

	t.Run("declared values do not error", func(t *testing.T) {
		t.Parallel()

		if err := li.Update(UpdateInput{Expression: "SET a = :v", Item: item, Attributes: declared}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestValidateExpressionAttributeValuesDeclared(t *testing.T) {
	t.Parallel()

	values := map[string]*types.Item{":v": {S: new("y")}}

	assertUndeclaredValueError(t,
		ValidateExpressionAttributeValuesDeclared("FilterExpression", "a = :v AND b = :x", values),
		"FilterExpression", ":x")

	if err := ValidateExpressionAttributeValuesDeclared("FilterExpression", "a = :v", values); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !HasUndeclaredExpressionAttributeValues("a = :x", func(n string) bool { return values[n] != nil }) {
		t.Fatal("expected :x to be undeclared")
	}

	if HasUndeclaredExpressionAttributeValues("a = :v", func(n string) bool { return values[n] != nil }) {
		t.Fatal("expected :v to be declared")
	}
}
