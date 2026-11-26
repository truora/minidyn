package interpreter

import (
	"errors"
	"fmt"
	"strings"
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

		if !HasInvalidListIndex("SET l[:i] = :v") || !HasInvalidListIndex("n = :v AND l[#n] = :v") || !HasInvalidListIndex("REMOVE l[in]") || !HasInvalidListIndex("l[OR] = :v") ||
			!HasInvalidListIndex("SET l[-1] = :v") || !HasInvalidListIndex("l[]") {
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

// literalIndexFixture is the language, an item with a list, and the attributes the texts of the
// tests below declare: :z is not declared and :v is unused by the texts that carry the index
// problem.
func literalIndexFixture() (Language, map[string]*types.Item, map[string]*types.Item) {
	item := map[string]*types.Item{"l": {L: []*types.Item{{S: new("a")}, {S: new("b")}}}}
	attrs := map[string]*types.Item{":v": {S: new("a")}}

	return Language{}, item, attrs
}

const listIndexRangeMsg = "List index is not within the allowable range; index: [%s]"

// A literal list index is 0 to 2147483647 without a leading zero, as in DynamoDB Local (#164,
// #267). The texts and the order are the ones Local returns for each kind of expression: a
// leading zero is a syntax error that names the token, a literal above the range is
// "List index is not within the allowable range; index: [N]", and both come before the
// placeholder checks. A syntax error elsewhere in the same text wins over the range.
func TestLiteralListIndexPerKindOfExpression(t *testing.T) {
	t.Parallel()

	li, item, attrs := literalIndexFixture()

	type run func(t *testing.T, expr string) error

	kinds := []struct {
		label string
		// expr wraps an index text, such as "2147483648", in an expression of that kind.
		expr func(index string) string
		run  run
	}{
		{"UpdateExpression", func(i string) string { return "SET l[" + i + "] = :v" }, func(t *testing.T, e string) error {
			return li.Update(UpdateInput{Expression: e, Item: item, Attributes: attrs})
		}},
		{"ConditionExpression", func(i string) string { return "l[" + i + "] = :v" }, func(t *testing.T, e string) error {
			_, err := li.Match(MatchInput{Expression: e, ExpressionType: ExpressionTypeConditional, Item: item, Attributes: attrs})

			return err
		}},
		{"FilterExpression", func(i string) string { return "l[" + i + "] = :v" }, func(t *testing.T, e string) error {
			_, err := li.Match(MatchInput{Expression: e, ExpressionType: ExpressionTypeFilter, Item: item, Attributes: attrs})

			return err
		}},
		{"KeyConditionExpression", func(i string) string { return "l[" + i + "] = :v" }, func(t *testing.T, e string) error {
			_, err := li.Match(MatchInput{Expression: e, ExpressionType: ExpressionTypeKey, Item: item, Attributes: attrs})

			return err
		}},
		{"ProjectionExpression", func(i string) string { return "l[" + i + "]" }, func(t *testing.T, e string) error {
			_, err := li.Project(ProjectInput{Expression: e, Item: item})

			return err
		}},
	}

	for _, kind := range kinds {
		// Not parallel: an update changes the item all the kinds share.
		t.Run(kind.label, func(t *testing.T) {
			for _, index := range []string{"2147483648", "4294967296", "9223372036854775807", "9223372036854775808", "99999999999999999999999999"} {
				requireListIndexError(t, kind.run(t, kind.expr(index)), "Invalid "+kind.label+": "+fmt.Sprintf(listIndexRangeMsg, index))
			}

			zeros := map[string]string{
				"007": `token: "0", near: "007"`, "00": `token: "0", near: "00]"`, "01": `token: "1", near: "01]"`,
				"0123": `token: "123", near: "0123]"`, "00000000002147483648": `token: "0", near: "000"`,
			}
			for index, near := range zeros {
				requireListIndexError(t, kind.run(t, kind.expr(index)), "Invalid "+kind.label+": Syntax error; "+near)
			}

			// The boundary, and a zero alone, are valid indexes.
			for _, index := range []string{"0", "1", "2147483647"} {
				if err := kind.run(t, kind.expr(index)); err != nil {
					t.Errorf("%s with index %s: unexpected error %v", kind.label, index, err)
				}
			}
		})
	}
}

// Before the placeholder checks, as DynamoDB Local does (#164).
func TestLiteralListIndexBeforePlaceholderChecks(t *testing.T) {
	t.Parallel()

	li, item, attrs := literalIndexFixture()

	// :z and #n are undefined and :v is declared: the index error is reported first.
	for _, index := range []string{"2147483648", "007"} {
		want := "Invalid ConditionExpression: " + fmt.Sprintf(listIndexRangeMsg, index)
		if index == "007" {
			want = `Invalid ConditionExpression: Syntax error; token: "0", near: "007"`
		}

		_, err := li.Match(MatchInput{
			Expression: "l[" + index + "] = :z AND #n = :v", ExpressionType: ExpressionTypeConditional, Item: item, Attributes: attrs,
		})
		requireListIndexError(t, err, want)
	}
}

// A syntax error elsewhere comes first, as DynamoDB Local does (#164).
func TestLiteralListIndexSyntaxErrorElsewhereFirst(t *testing.T) {
	t.Parallel()

	li, item, attrs := literalIndexFixture()

	err := li.Update(UpdateInput{Expression: "SET l[2147483648] = :v, = x", Item: item, Attributes: attrs})
	if err == nil || strings.Contains(err.Error(), "allowable range") || !errors.Is(err, ErrSyntaxError) {
		t.Fatalf("want a syntax error, got %v", err)
	}

	err = li.Update(UpdateInput{Expression: "SET = x, l[2147483648] = :v", Item: item, Attributes: attrs})
	if err == nil || strings.Contains(err.Error(), "allowable range") || !errors.Is(err, ErrSyntaxError) {
		t.Fatalf("want a syntax error, got %v", err)
	}

	// A placeholder or a leading zero is such a syntax error, before or after the literal.
	for expr, want := range map[string]string{
		"SET l[2147483648] = :v, m[:i] = :v":  `Invalid UpdateExpression: Syntax error; token: ":i", near: "[:i]"`,
		"SET m[:i] = :v, l[2147483648] = :v":  `Invalid UpdateExpression: Syntax error; token: ":i", near: "[:i]"`,
		"SET l[2147483648] = :v, m[007] = :v": `Invalid UpdateExpression: Syntax error; token: "0", near: "007"`,
		"SET m[007] = :v, l[2147483648] = :v": `Invalid UpdateExpression: Syntax error; token: "0", near: "007"`,
	} {
		requireListIndexError(t, li.Update(UpdateInput{Expression: expr, Item: item, Attributes: attrs}), want)
	}
}

// Up front validators, as DynamoDB Local does (#164).
func TestLiteralListIndexUpFrontValidators(t *testing.T) {
	t.Parallel()

	_, _, attrs := literalIndexFixture()

	upd := LabeledExpression{Label: "UpdateExpression", Expression: "SET l[2147483648] = :z"}
	cond := LabeledExpression{Label: "ConditionExpression", Expression: "l[2147483649] = :x AND #n = :q"}
	zero := LabeledExpression{Label: "ConditionExpression", Expression: "l[007] = :x"}
	wantUpd := "Invalid UpdateExpression: " + fmt.Sprintf(listIndexRangeMsg, "2147483648")
	wantCond := "Invalid ConditionExpression: " + fmt.Sprintf(listIndexRangeMsg, "2147483649")

	// Undefined names and values, and unused ones, come after the range error.
	requireListIndexError(t, ValidateExpressionPlaceholders(nil, attrs, upd, cond), wantUpd)
	requireListIndexError(t, ValidateExpressionAttributeNamesDeclared(nil, cond), wantCond)
	requireListIndexError(t, ValidateExpressionAttributeValuesDeclared(attrs, cond), wantCond)
	// The first expression with a problem is the one reported, in the order given.
	requireListIndexError(t, ValidateExpressionPlaceholders(nil, attrs, cond, upd), wantCond)
	// A syntax error, in any expression, comes before a range error.
	requireListIndexError(t, ValidateExpressionPlaceholders(nil, attrs, upd, zero),
		`Invalid ConditionExpression: Syntax error; token: "0", near: "007"`)

	// A syntax error elsewhere in the same text is reported, as the parse reports it, before an
	// undefined value or an unused one.
	for _, e := range []LabeledExpression{
		{Label: "UpdateExpression", Expression: "SET l[2147483648] = :z, = x"},
		{Label: "FilterExpression", Expression: "l[2147483648] = :v AND"},
		{Label: "KeyConditionExpression", Expression: "pk = :v AND l[2147483648] = :v AND"},
		{Label: "ProjectionExpression", Expression: "l[2147483648], , a"},
	} {
		wantParse := parseErrorOf(t, e)

		if strings.Contains(wantParse.Error(), "allowable range") {
			t.Fatalf("%s: the parse gave the range error: %v", e.Label, wantParse)
		}

		requireListIndexError(t, ValidateExpressionAttributeNamesDeclared(nil, e), wantParse.Error())
		requireListIndexError(t, ValidateExpressionAttributeValuesDeclared(attrs, e), wantParse.Error())
		requireListIndexError(t, ValidateExpressionPlaceholders(nil, attrs, e), wantParse.Error())
	}

	// Every kind of expression is checked against its own grammar.
	for _, label := range []string{"FilterExpression", "KeyConditionExpression", "ProjectionExpression"} {
		expr := "l[2147483648]"
		if label != "ProjectionExpression" {
			expr += " = :x"
		}

		requireListIndexError(t,
			ValidateExpressionAttributeValuesDeclared(attrs, LabeledExpression{Label: label, Expression: expr}),
			"Invalid "+label+": "+fmt.Sprintf(listIndexRangeMsg, "2147483648"))
	}
}

// Front end pre-check, as DynamoDB Local does (#164).
func TestLiteralListIndexFrontEndPreCheck(t *testing.T) {
	t.Parallel()

	for _, expr := range []string{"SET l[2147483648] = :v", "n = :v AND l[99999999999999999999] = :v", "SET l[007] = :v", "l[01]"} {
		if !HasInvalidListIndex(expr) {
			t.Errorf("%q: the index problem was not found", expr)
		}
	}

	for _, expr := range []string{"SET l[2147483647] = :v", "l[0]", "m.k[10]"} {
		if HasInvalidListIndex(expr) {
			t.Errorf("%q: a valid index was reported", expr)
		}
	}
}

// Projection check, as DynamoDB Local does (#164).
func TestLiteralListIndexCheckProjection(t *testing.T) {
	t.Parallel()

	_, err := CheckProjection("a, l[2147483648]", nil)
	requireListIndexError(t, err, "Invalid ProjectionExpression: "+fmt.Sprintf(listIndexRangeMsg, "2147483648"))

	_, err = CheckProjection("l[007], #q", nil)
	requireListIndexError(t, err, `Invalid ProjectionExpression: Syntax error; token: "0", near: "007"`)

	if _, err := CheckProjection("l[2147483647]", nil); err != nil {
		t.Fatalf("got %v", err)
	}
}

// requireListIndexError checks the message of a list index error, and that it maps to a
// ValidationException like the other expression errors.
func requireListIndexError(t *testing.T, err error, want string) {
	t.Helper()

	if err == nil || err.Error() != want {
		t.Fatalf("want %q, got %v", want, err)
	}

	if !errors.Is(err, ErrSyntaxError) {
		t.Fatalf("%v does not unwrap to ErrSyntaxError", err)
	}
}

// parseErrorOf returns the error the evaluation of e reports for its syntax error.
func parseErrorOf(t *testing.T, e LabeledExpression) error {
	t.Helper()

	li := Language{}

	var err error

	switch e.Label {
	case "UpdateExpression":
		err = li.Update(UpdateInput{Expression: e.Expression})
	case "ProjectionExpression":
		_, err = CheckProjection(e.Expression, nil)
	case "KeyConditionExpression":
		_, err = li.Match(MatchInput{Expression: e.Expression, ExpressionType: ExpressionTypeKey})
	default:
		_, err = li.Match(MatchInput{Expression: e.Expression, ExpressionType: ExpressionTypeFilter})
	}

	if err == nil {
		t.Fatalf("%s %q: no error", e.Label, e.Expression)
	}

	return err
}

// A token between the brackets that is not a word (l[-1], l[]) is the same up front syntax error
// as l[:i], with the text Local reports, before an unused value (#164).
func TestNonWordListIndexIsASyntaxError(t *testing.T) {
	t.Parallel()

	li := Language{}
	attrs := map[string]*types.Item{":v": {S: new("a")}, ":z": {S: new("a")}}

	err := li.Update(UpdateInput{Expression: "SET l[-1] = :v", Attributes: attrs})
	requireListIndexError(t, err, `Invalid UpdateExpression: Syntax error; token: "-", near: "[-1"`)

	err = ValidateExpressionAttributeValuesDeclared(attrs, LabeledExpression{Label: "UpdateExpression", Expression: "SET l[-1] = :v"})
	requireListIndexError(t, err, `Invalid UpdateExpression: Syntax error; token: "-", near: "[-1"`)

	_, err = li.Match(MatchInput{Expression: "l[] = :v", ExpressionType: ExpressionTypeFilter, Attributes: attrs})
	requireListIndexError(t, err, `Invalid FilterExpression: Syntax error; token: "]", near: "[] ="`)

	_, err = CheckProjection("l[]", nil)
	requireListIndexError(t, err, `Invalid ProjectionExpression: Syntax error; token: "]", near: "[]"`)
}
