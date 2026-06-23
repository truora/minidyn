package language

import (
	"reflect"
	"testing"
)

func TestUndeclaredExpressionAttributeNames(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		expression string
		aliases    map[string]string
		want       []string
	}{
		{
			name:       "all declared",
			expression: "SET #a = :v",
			aliases:    map[string]string{"#a": "attr"},
			want:       []string{},
		},
		{
			name:       "undeclared single",
			expression: "SET #a = :v",
			aliases:    nil,
			want:       []string{"#a"},
		},
		{
			name:       "undeclared in document path",
			expression: "#a.#b = :v",
			aliases:    map[string]string{"#a": "attr"},
			want:       []string{"#b"},
		},
		{
			name:       "deduplicated first-seen order",
			expression: "#b = :x OR #a = :y OR #b = :z",
			aliases:    nil,
			want:       []string{"#b", "#a"},
		},
		{
			name:       "no placeholders",
			expression: "attr = :v",
			aliases:    nil,
			want:       []string{},
		},
		{
			name:       "empty expression",
			expression: "",
			aliases:    nil,
			want:       []string{},
		},
		{
			name:       "value placeholders ignored",
			expression: "#a = :v",
			aliases:    map[string]string{"#a": "attr"},
			want:       []string{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := UndeclaredExpressionAttributeNames(tc.expression, tc.aliases)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("UndeclaredExpressionAttributeNames(%q) = %v, want %v", tc.expression, got, tc.want)
			}
		})
	}
}
