package interpreter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/types"
)

func TestExprPlaceholders_scansOnceAndCaches(t *testing.T) {
	expr := "SET #cachedA = :cachedX, #cachedB = :cachedY + :cachedX"
	before := cachedPlaceholders()

	names, values := exprPlaceholders(expr)
	require.Equal(t, []string{"#cachedA", "#cachedB"}, names)
	require.Equal(t, []string{":cachedX", ":cachedY"}, values)
	require.Equal(t, before+1, cachedPlaceholders())

	again, _ := exprPlaceholders(expr)
	require.Same(t, &names[0], &again[0], "the second lookup must reuse the cached scan")
	require.Equal(t, before+1, cachedPlaceholders())
}

func TestParseCondition_reusesCachedPlaceholders(t *testing.T) {
	expr := "#reuse = :reuse"
	names, values := exprPlaceholders(expr)

	parsed := parseCondition(expr)
	require.Same(t, &names[0], &parsed.names[0])
	require.Same(t, &values[0], &parsed.values[0])
}

func TestExprPlaceholders_cacheIsBounded(t *testing.T) {
	for i := range maxParsedExpressions + 10 {
		exprPlaceholders(fmt.Sprintf("#n = :v%d", i))
	}

	require.LessOrEqual(t, cachedPlaceholders(), maxParsedExpressions)
}

func TestValidateExpressionPlaceholders(t *testing.T) {
	t.Parallel()

	aliases := map[string]string{"#a": "a"}
	values := map[string]*types.Item{":v": {S: new("y")}}
	update := LabeledExpression{"UpdateExpression", "SET #a = :v"}

	require.NoError(t, ValidateExpressionPlaceholders(aliases, values, update))
	require.NoError(t, ValidateExpressionPlaceholders(nil, nil, LabeledExpression{"ConditionExpression", ""}))

	t.Run("an undeclared name wins over an undefined value of an earlier expression", func(t *testing.T) {
		t.Parallel()

		err := ValidateExpressionPlaceholders(aliases, values,
			LabeledExpression{"UpdateExpression", "SET #a = :nope"},
			LabeledExpression{"ConditionExpression", "#b = :v"},
		)
		assertUndeclaredNameError(t, err, "ConditionExpression", "#b")
	})

	t.Run("the first undefined value is reported in expression order", func(t *testing.T) {
		t.Parallel()

		err := ValidateExpressionPlaceholders(aliases, values,
			LabeledExpression{"UpdateExpression", "SET #a = :first"},
			LabeledExpression{"ConditionExpression", "#a = :second"},
		)
		assertUndeclaredValueError(t, err, "UpdateExpression", ":first")
	})

	t.Run("names only", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, ValidateExpressionAttributeNamesDeclared(aliases,
			LabeledExpression{"FilterExpression", "#a = :unchecked"}))
		assertUndeclaredNameError(t,
			ValidateExpressionAttributeNamesDeclared(aliases, LabeledExpression{"FilterExpression", "#z = :v"}),
			"FilterExpression", "#z")
	})

	t.Run("values only", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, ValidateExpressionAttributeValuesDeclared(values,
			LabeledExpression{"FilterExpression", "#unchecked = :v"}))
		assertUndeclaredValueError(t,
			ValidateExpressionAttributeValuesDeclared(values, LabeledExpression{"FilterExpression", "a = :x"}),
			"FilterExpression", ":x")
	})
}

func TestHasUndeclaredExpressionAttributeValues(t *testing.T) {
	t.Parallel()

	declared := func(n string) bool { return n == ":v" }

	require.True(t, HasUndeclaredExpressionAttributeValues("a = :x", declared))
	require.False(t, HasUndeclaredExpressionAttributeValues("a = :v", declared))
}
