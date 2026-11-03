package interpreter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyConditionHashPlaceholder(t *testing.T) {
	aliases := map[string]string{"#p": "pk", "#s": "sk"}

	tests := []struct {
		expr string
		want string
		ok   bool
	}{
		{"pk = :p", ":p", true},
		{"#p = :p", ":p", true},
		{":p = pk", ":p", true},
		{"pk = :p AND sk > :s", ":p", true},
		{"sk > :s AND #p = :p", ":p", true},
		{"(pk = :p) AND begins_with(sk, :s)", ":p", true},
		{"pk = :p AND sk BETWEEN :a AND :b", ":p", true},
		{"pk = :p OR sk = :s", "", false}, // invalid key condition: keep the full walk
		{"sk = :s", "", false},
		{"pk > :p", "", false},
		{"other = :p", "", false},
		{"pk = other", "", false},    // compared to an attribute, not a value
		{"pk = size(sk)", "", false}, // compared to a function result
		{":p = :q", "", false},       // no key attribute at all
		{"#missing = :p", "", false},
		{"pk = ", "", false}, // syntax error
		{"", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			got, ok := KeyConditionHashPlaceholder(tt.expr, aliases, "pk")
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}
