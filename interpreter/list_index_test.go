package interpreter

import (
	"errors"
	"testing"

	"github.com/truora/minidyn/types"
)

// A list index is a literal. DynamoDB Local rejects a placeholder between the brackets with a
// syntax error that names the token, whatever the expression is and before it checks any
// placeholder (#164). The texts are the ones Local returns.
func TestPlaceholderListIndexIsASyntaxError(t *testing.T) {
	t.Parallel()

	const near = `Syntax error; token: ":i", near: "[:i]"`

	li := Language{}
	item := map[string]*types.Item{"l": {L: []*types.Item{{S: new("a")}, {S: new("b")}}}}
	// :i is not declared and :z is not used: the syntax error still comes first.
	attrs := map[string]*types.Item{":v": {S: new("a")}}

	check := func(t *testing.T, err error, label string) {
		t.Helper()

		want := "Invalid " + label + ": " + near
		if err == nil || err.Error() != want {
			t.Fatalf("want %q, got %v", want, err)
		}

		if !errors.Is(err, ErrSyntaxError) {
			t.Fatalf("%v does not unwrap to ErrSyntaxError", err)
		}
	}

	t.Run("condition", func(t *testing.T) {
		t.Parallel()

		_, err := li.Match(MatchInput{Expression: "l[:i] = :v", ExpressionType: ExpressionTypeConditional, Item: item, Attributes: attrs})
		check(t, err, "ConditionExpression")
	})

	t.Run("filter", func(t *testing.T) {
		t.Parallel()

		_, err := li.Match(MatchInput{Expression: "l[:i] = :v", ExpressionType: ExpressionTypeFilter, Item: item, Attributes: attrs})
		check(t, err, "FilterExpression")
	})

	t.Run("key condition", func(t *testing.T) {
		t.Parallel()

		_, err := li.Match(MatchInput{Expression: "l[:i] = :v", ExpressionType: ExpressionTypeKey, Item: item, Attributes: attrs})
		check(t, err, "KeyConditionExpression")
	})

	t.Run("update", func(t *testing.T) {
		t.Parallel()

		err := li.Update(UpdateInput{Expression: "SET l[:i] = :v", Item: item, Attributes: attrs})
		check(t, err, "UpdateExpression")
	})

	t.Run("projection", func(t *testing.T) {
		t.Parallel()

		_, err := li.Project(ProjectInput{Expression: "l[:i]", Item: item})
		check(t, err, "ProjectionExpression")
	})

	t.Run("up front validators", func(t *testing.T) {
		t.Parallel()

		upd := LabeledExpression{Label: "UpdateExpression", Expression: "SET l[:i] = :v"}
		cond := LabeledExpression{Label: "ConditionExpression", Expression: "l[:i] = :x AND #n = :q"}

		check(t, ValidateExpressionPlaceholders(nil, attrs, upd, cond), "UpdateExpression")
		check(t, ValidateExpressionAttributeNamesDeclared(nil, cond), "ConditionExpression")
		check(t, ValidateExpressionAttributeValuesDeclared(attrs, cond), "ConditionExpression")

		// Without an index problem the placeholders are validated as before.
		err := ValidateExpressionAttributeNamesDeclared(nil, LabeledExpression{Label: "UpdateExpression", Expression: "SET #n = :v"})
		if err == nil || err.Error() != "Invalid UpdateExpression: An expression attribute name used in the document path is not defined; attribute name: #n" {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("front end pre-check", func(t *testing.T) {
		t.Parallel()

		if !HasInvalidListIndex("SET l[:i] = :v") || !HasInvalidListIndex("n = :v AND l[#n] = :v") {
			t.Fatal("an index that is not a literal was not found")
		}

		if HasInvalidListIndex("SET l[0] = :v, m.k[12] = :v") || HasInvalidListIndex("") {
			t.Fatal("a literal index was reported")
		}
	})

	t.Run("a literal index is still valid", func(t *testing.T) {
		t.Parallel()

		ok, err := li.Match(MatchInput{Expression: "l[1] = :v", ExpressionType: ExpressionTypeConditional, Item: item, Attributes: map[string]*types.Item{":v": {S: new("b")}}})
		if err != nil || !ok {
			t.Fatalf("got %v, %v", ok, err)
		}
	})
}
