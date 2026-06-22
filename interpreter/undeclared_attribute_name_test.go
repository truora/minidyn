package interpreter

import (
	"errors"
	"testing"

	"github.com/truora/minidyn/types"
)

func TestUndeclaredExpressionAttributeNameErrors(t *testing.T) {
	t.Parallel()

	li := Language{}
	item := map[string]*types.Item{
		"id": {S: new("x")},
	}

	t.Run("UpdateExpression", func(t *testing.T) {
		t.Parallel()

		err := li.Update(UpdateInput{
			Expression: "SET #a = :v",
			Item:       item,
			Attributes: map[string]*types.Item{":v": {S: new("y")}},
		})
		assertUndeclaredNameError(t, err, "UpdateExpression", "#a")
	})

	t.Run("ConditionExpression", func(t *testing.T) {
		t.Parallel()

		_, err := li.Match(MatchInput{
			Expression:     "attribute_not_exists(#a)",
			ExpressionType: ExpressionTypeConditional,
			Item:           item,
		})
		assertUndeclaredNameError(t, err, "ConditionExpression", "#a")
	})

	t.Run("FilterExpression", func(t *testing.T) {
		t.Parallel()

		_, err := li.Match(MatchInput{
			Expression:     "#a = :v",
			ExpressionType: ExpressionTypeFilter,
			Item:           item,
			Attributes:     map[string]*types.Item{":v": {S: new("y")}},
		})
		assertUndeclaredNameError(t, err, "FilterExpression", "#a")
	})

	t.Run("KeyConditionExpression", func(t *testing.T) {
		t.Parallel()

		_, err := li.Match(MatchInput{
			Expression:     "#a = :v",
			ExpressionType: ExpressionTypeKey,
			Item:           item,
			Attributes:     map[string]*types.Item{":v": {S: new("y")}},
		})
		assertUndeclaredNameError(t, err, "KeyConditionExpression", "#a")
	})

	t.Run("ProjectionExpression", func(t *testing.T) {
		t.Parallel()

		_, err := li.Project(ProjectInput{
			Expression: "#a",
			Item:       item,
		})
		assertUndeclaredNameError(t, err, "ProjectionExpression", "#a")
	})

	t.Run("reports first undeclared name", func(t *testing.T) {
		t.Parallel()

		err := li.Update(UpdateInput{
			Expression: "SET #b = :v REMOVE #c",
			Item:       item,
			Attributes: map[string]*types.Item{":v": {S: new("y")}},
		})
		assertUndeclaredNameError(t, err, "UpdateExpression", "#b")
	})

	t.Run("declared names do not error", func(t *testing.T) {
		t.Parallel()

		err := li.Update(UpdateInput{
			Expression: "SET #a = :v",
			Item:       item,
			Attributes: map[string]*types.Item{":v": {S: new("y")}},
			Aliases:    map[string]string{"#a": "attr"},
		})
		if err != nil {
			t.Fatalf("unexpected error for declared name: %v", err)
		}
	})
}

func assertUndeclaredNameError(t *testing.T, err error, expressionType, name string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected an error for undeclared %s name %q", expressionType, name)
	}

	want := "Invalid " + expressionType +
		": An expression attribute name used in the document path is not defined; attribute name: " + name
	if err.Error() != want {
		t.Fatalf("error message mismatch\n got: %q\nwant: %q", err.Error(), want)
	}

	// Undeclared-name errors must be mapped to ValidationException by the same gating that
	// handles expression syntax errors.
	if !errors.Is(err, ErrSyntaxError) {
		t.Fatalf("expected error to unwrap to ErrSyntaxError, got %v", err)
	}
}
