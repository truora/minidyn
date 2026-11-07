package interpreter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateKeyCondition(t *testing.T) {
	aliases := map[string]string{"#p": "pk", "#s": "sk", "#o": "other"}

	tests := []struct {
		expr     string
		rangeKey string
		want     string // "" when valid
		detail   string
	}{
		{"pk = :p", "sk", "", ""},
		{"#p = :p AND #s BETWEEN :a AND :b", "sk", "", ""},
		{"begins_with(sk, :a) AND pk = :p", "sk", "", ""},
		{"(pk = :p) AND (sk >= :a)", "sk", "", ""},
		{"pk = :p", "", "", ""},
		{"pk = ", "sk", "", ""}, // syntax errors are reported by the evaluation
		{"", "sk", "", ""},

		{"sk = :a", "sk", "Query condition missed key schema element", "missing: pk"},
		{"pk = :p AND #o = :a", "sk", "Query condition missed key schema element", "attribute: other"},
		{"pk = :p AND sk = :a", "", "Query condition missed key schema element", "attribute: sk"},
		{"pk > :p", "sk", "Query key condition not supported", "attribute: pk"},
		{"pk = :p AND sk > :a AND sk < :b", "sk", "KeyConditionExpressions must only contain one condition per key", "attribute: sk"},
		{"pk = :p AND sk = :a AND #o = :b", "sk", "Conditions can be of length 1 or 2 only", ""},
		{"pk = :p OR sk = :a", "sk", "Invalid operator used in KeyConditionExpression: OR", ""},
		{"NOT pk = :p", "sk", "Invalid operator used in KeyConditionExpression: NOT", ""},
		{"pk = :p AND sk <> :a", "sk", "Invalid operator used in KeyConditionExpression: <>", ""},
		{"pk = :p AND contains(sk, :a)", "sk", "Invalid operator used in KeyConditionExpression: contains", ""},
		{"pk = :p AND sk IN (:a, :b)", "sk", "Invalid operator used in KeyConditionExpression: IN", ""},
		{"pk = :p AND attribute_exists(sk)", "sk", "Invalid operator used in KeyConditionExpression: attribute_exists", ""},

		{"pk = :p AND :a = :b", "", "Invalid condition in KeyConditionExpression: No key attribute specified", ""},
		{"pk = :p AND :a < :b", "sk", "Invalid condition in KeyConditionExpression: No key attribute specified", ""},
		{":a = :b AND pk = :p", "sk", "Invalid condition in KeyConditionExpression: No key attribute specified", ""},
		{"pk = :p AND sk = pk", "sk", "Invalid condition in KeyConditionExpression: Multiple attribute names used in one condition", ""},
		{"pk = :p AND #s < pk", "sk", "Invalid condition in KeyConditionExpression: Multiple attribute names used in one condition", ""},
		{"pk = :p AND sk BETWEEN pk AND :b", "sk", "Invalid condition in KeyConditionExpression: Multiple attribute names used in one condition", ""},
		{"pk = :p AND begins_with(sk, pk)", "sk", "Invalid condition in KeyConditionExpression: Multiple attribute names used in one condition", ""},
		{"pk = :p AND :a BETWEEN sk AND :b", "sk", "Invalid condition in KeyConditionExpression: BETWEEN operator must have the key attribute as its first operand", ""},
		{"pk = :p AND begins_with(:a, :b)", "sk", "Invalid condition in KeyConditionExpression: begins_with operator must have the key attribute as its first operand", ""},
		{"pk = :p AND size(sk) = :a", "sk", "KeyConditionExpressions cannot contain nested operations", ""},
		{"pk = :p AND :a = size(sk)", "sk", "KeyConditionExpressions cannot contain nested operations", ""},
		{"pk = :p AND sk BETWEEN :a AND size(sk)", "sk", "KeyConditionExpressions cannot contain nested operations", ""},
		{"size(pk) = :p AND sk = :a", "sk", "KeyConditionExpressions cannot contain nested operations", ""},
		// the first failing condition, left to right, is the one reported
		{":a = :b AND size(sk) = :c", "sk", "Invalid condition in KeyConditionExpression: No key attribute specified", ""},
		{"size(sk) = :c AND :a = :b", "sk", "KeyConditionExpressions cannot contain nested operations", ""},
		{"sk = pk AND size(sk) = :c", "sk", "Invalid condition in KeyConditionExpression: Multiple attribute names used in one condition", ""},
		{"pk = :p AND sk <> :a AND sk = pk", "sk", "Invalid operator used in KeyConditionExpression: <>", ""},
		{"pk = :p AND sk = pk AND sk <> :a", "sk", "Invalid condition in KeyConditionExpression: Multiple attribute names used in one condition", ""},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			err := ValidateKeyCondition(tt.expr, aliases, "pk", tt.rangeKey)
			if tt.want == "" {
				require.NoError(t, err)

				return
			}

			var kerr *KeyConditionError
			require.ErrorAs(t, err, &kerr)
			require.Equal(t, tt.want, kerr.Message)
			require.Contains(t, err.Error(), tt.detail)
		})
	}
}

func TestKeyConditionPlaceholders(t *testing.T) {
	aliases := map[string]string{"#p": "pk"}

	require.Equal(t,
		map[string][]string{"pk": {":p"}, "sk": {":a", ":b"}},
		KeyConditionPlaceholders("#p = :p AND sk BETWEEN :a AND :b", aliases))
	require.Equal(t,
		map[string][]string{"pk": {":p"}, "sk": {":a"}},
		KeyConditionPlaceholders(":a < sk AND pk = :p", aliases))
	require.Equal(t,
		map[string][]string{"pk": {":p"}, "sk": {":a"}},
		KeyConditionPlaceholders("pk = :p AND begins_with(sk, :a)", aliases))
	require.Nil(t, KeyConditionPlaceholders("pk = :p OR sk = :a", aliases))
	require.Nil(t, KeyConditionPlaceholders("pk = ", aliases))
}
