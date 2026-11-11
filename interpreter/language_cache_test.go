package interpreter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/interpreter/language"
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

func TestLanguageUpdate_parsesEachExpressionOnce(t *testing.T) {
	expr := "SET cacheOnce = :v"

	first := parseUpdate(expr)
	require.Same(t, first, parseUpdate(expr))
	require.Equal(t, [][]language.PathElement{{{Kind: language.PathKindMapKey, Key: "cacheOnce"}}},
		UpdateTargetPaths(expr, nil, nil))
	require.Same(t, first, parseUpdate(expr))
}

func TestLanguageUpdate_cachedSyntaxErrorIsReturnedAgain(t *testing.T) {
	li := &Language{}
	input := UpdateInput{Expression: "SET = ="}

	first := li.Update(input)
	require.ErrorIs(t, first, ErrSyntaxError)
	require.EqualError(t, li.Update(input), first.Error())
	require.Empty(t, UpdateTargetPaths(input.Expression, nil, nil))
}

func TestLanguageUpdate_cacheIsBounded(t *testing.T) {
	for i := range maxParsedExpressions + 10 {
		parseUpdate(fmt.Sprintf("SET bounded%d = :v", i))
	}

	require.LessOrEqual(t, cachedUpdates(), maxParsedExpressions)
}

func TestCheckProjection(t *testing.T) {
	aliases := map[string]string{"#n": "name"}

	require.NoError(t, checkProjectionErr("", nil))
	require.NoError(t, checkProjectionErr("a, #n, b.c[1]", aliases))

	err := checkProjectionErr("a,,", aliases)
	require.ErrorIs(t, err, ErrSyntaxError)
	require.Contains(t, err.Error(), "Invalid ProjectionExpression")

	err = checkProjectionErr("a, #x", aliases)
	require.ErrorIs(t, err, ErrSyntaxError)
	require.Equal(t, "Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #x", err.Error())

	// a syntax error is reported before an undeclared name, as Project does
	err = checkProjectionErr("#x,,", nil)
	require.Contains(t, err.Error(), "Invalid ProjectionExpression")
	require.NotContains(t, err.Error(), "not defined")
}

func TestLanguageProject_parsesEachExpressionOnce(t *testing.T) {
	li := &Language{}
	expr := "projOnce1, projOnce2.k" // unique to this test
	item := map[string]*types.Item{"projOnce1": {S: new("x")}}

	first := parseProjection(expr)

	for range 3 {
		out, err := li.Project(ProjectInput{Expression: expr, Item: item})
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.NoError(t, checkProjectionErr(expr, nil))
		require.Same(t, first, parseProjection(expr), "the same expression should be parsed once")
	}
}

func TestLanguageProject_cacheIsBounded(t *testing.T) {
	for i := range maxParsedExpressions + 10 {
		parseProjection(fmt.Sprintf("bounded%d", i))
	}

	require.LessOrEqual(t, cachedProjections(), maxParsedExpressions)
}

// checkProjectionErr is the error CheckProjection reports, without the checked projection.
func checkProjectionErr(expr string, aliases map[string]string) error {
	_, err := CheckProjection(expr, aliases)

	return err
}
