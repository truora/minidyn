package server

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// DynamoDB Local quotes the offending key: Syntax error; key: "#invalid-name".
func TestValidateSyntaxExpressionQuotesKey(t *testing.T) {
	re := regexp.MustCompile("^#[A-Za-z0-9_]+$")

	require.NoError(t, validateSyntaxExpression(re, []string{"#ok"}, invalidExpressionAttributeName))

	err := validateSyntaxExpression(re, []string{"#ok", "#invalid-name"}, invalidExpressionAttributeName)
	require.EqualError(t, err,
		"api error ValidationException: ExpressionAttributeNames contains invalid key: Syntax error; key: \"#invalid-name\"")

	err = validateSyntaxExpression(re, []string{":invalid-value"}, invalidExpressionAttributeValue)
	require.EqualError(t, err,
		"api error ValidationException: ExpressionAttributeValues contains invalid key: Syntax error; key: \":invalid-value\"")
}
