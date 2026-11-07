package language

import (
	"reflect"
	"testing"
)

func TestUndeclaredExpressionAttributeValues(t *testing.T) {
	t.Parallel()

	declared := func(names ...string) func(string) bool {
		return func(name string) bool {
			for _, n := range names {
				if n == name {
					return true
				}
			}

			return false
		}
	}

	cases := []struct {
		name       string
		expression string
		declared   func(string) bool
		want       []string
	}{
		{"all declared", "SET a = :v", declared(":v"), []string{}},
		{"undeclared single", "SET a = :nope", declared(), []string{":nope"}},
		{"one of two", "a = :v AND b = :nope", declared(":v"), []string{":nope"}},
		{"first-seen order without duplicates", "b = :y OR a = :x OR c = :y", declared(), []string{":y", ":x"}},
		{"function operand", "begins_with(a, :p)", declared(), []string{":p"}},
		{"between", "a BETWEEN :lo AND :hi", declared(":lo"), []string{":hi"}},
		{"name placeholders ignored", "#a = :v", declared(":v"), []string{}},
		{"no placeholders", "a = b", declared(), []string{}},
		{"empty expression", "", declared(), []string{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := UndeclaredExpressionAttributeValues(tc.expression, tc.declared)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
