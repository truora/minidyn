package language

import (
	"reflect"
	"testing"
)

func TestPlaceholders(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		expression string
		names      []string
		values     []string
	}{
		{"names and values", "SET #a = :v", []string{"#a"}, []string{":v"}},
		{"document path", "#a.#b = :v", []string{"#a", "#b"}, []string{":v"}},
		{
			"first-seen order without duplicates", "#b = :y OR #a = :x OR #b = :y OR c = :x",
			[]string{"#b", "#a"},
			[]string{":y", ":x"},
		},
		{"function operand", "begins_with(a, :p)", []string{}, []string{":p"}},
		{"between", "a BETWEEN :lo AND :hi", []string{}, []string{":lo", ":hi"}},
		{"no placeholders", "a = b", []string{}, []string{}},
		{"colon inside a name is not a placeholder", "a:b = :v", []string{}, []string{":v"}},
		{"colon inside a name placeholder", "#a:x = :v", []string{"#a:x"}, []string{":v"}},
		{"empty expression", "", []string{}, []string{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			names, values := Placeholders(tc.expression)
			if !reflect.DeepEqual(names, tc.names) || !reflect.DeepEqual(values, tc.values) {
				t.Fatalf("Placeholders(%q) = %v, %v; want %v, %v", tc.expression, names, values, tc.names, tc.values)
			}
		})
	}
}

func TestScanExpressionInvalidIndex(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"l[:i] = :v":              ":i",
		"SET l[#n] = :v":          "#n",
		"a[0] = :v AND b[:j]":     ":j",
		"l[abc]":                  "abc",
		"m.l[1].k[:z]":            ":z",
		"l[0] = :v":               "",
		"a = :i":                  "",
		"l[12][3] = :v":           "",
		"l[9223372036854775808]":  "",
		"":                        "",
		"a.b[1].c = :v, d[2] = e": "",
	}

	for expression, want := range cases {
		if got := ScanExpression(expression).InvalidIndex; got != want {
			t.Errorf("ScanExpression(%q).InvalidIndex = %q, want %q", expression, got, want)
		}
	}
}
