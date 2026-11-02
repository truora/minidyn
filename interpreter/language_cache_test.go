package interpreter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func cacheTestItem(v string) map[string]*types.Item {
	return map[string]*types.Item{"status": {S: &v}}
}

func TestLanguageMatch_parsesEachExpressionOnce(t *testing.T) {
	li := &Language{}
	active := "active"
	expr := "#s = :v AND attribute_exists(#s) AND :v = :v" // unique to this test

	var first *parsedCondition

	for _, status := range []string{"active", "inactive", "active"} {
		matched, err := li.Match(MatchInput{
			Expression:     expr,
			ExpressionType: ExpressionTypeFilter,
			Item:           cacheTestItem(status),
			Attributes:     map[string]*types.Item{":v": {S: &active}},
			Aliases:        map[string]string{"#s": "status"},
		})
		require.NoError(t, err)
		require.Equal(t, status == "active", matched)

		if first == nil {
			first = parseCondition(expr)
		}

		require.Same(t, first, parseCondition(expr), "the same expression should be parsed once")
	}
}

func TestLanguageMatch_cachedExpressionStillValidatesAliases(t *testing.T) {
	li := &Language{}
	active := "active"
	input := MatchInput{
		Expression:     "#s = :v",
		ExpressionType: ExpressionTypeFilter,
		Item:           cacheTestItem("active"),
		Attributes:     map[string]*types.Item{":v": {S: &active}},
		Aliases:        map[string]string{"#s": "status"},
	}

	_, err := li.Match(input)
	require.NoError(t, err)

	input.Aliases = map[string]string{}

	_, err = li.Match(input)
	require.EqualError(t, err, "Invalid FilterExpression: An expression attribute name used in the document path is not defined; attribute name: #s")
}

func TestLanguageMatch_cachedSyntaxErrorIsReturnedAgain(t *testing.T) {
	li := &Language{}
	input := MatchInput{Expression: "status = ", ExpressionType: ExpressionTypeConditional}

	_, first := li.Match(input)
	require.Error(t, first)

	_, second := li.Match(input)
	require.EqualError(t, second, first.Error())
}

func TestLanguageMatch_cacheIsBounded(t *testing.T) {
	li := &Language{}
	active := "active"

	for i := range maxParsedExpressions + 10 {
		_, err := li.Match(MatchInput{
			Expression:     fmt.Sprintf("#s = :v%d", i),
			ExpressionType: ExpressionTypeFilter,
			Item:           cacheTestItem("active"),
			Attributes:     map[string]*types.Item{fmt.Sprintf(":v%d", i): {S: &active}},
			Aliases:        map[string]string{"#s": "status"},
		})
		require.NoError(t, err)
	}

	require.LessOrEqual(t, cachedConditions(), maxParsedExpressions)
}
