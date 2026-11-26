package language

import (
	"fmt"
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

// The scan finds, in the same pass as the placeholders, the first list index DynamoDB rejects
// while parsing (a token that is not a literal, or a literal with a leading zero) and the first
// literal above the range. The two are kept apart because a syntax error anywhere wins.
func TestScanExpressionListIndex(t *testing.T) {
	t.Parallel()

	const rangeText = "List index is not within the allowable range; index: [%s]"

	cases := []struct {
		expression string
		syntax     string
		rangeIdx   string
	}{
		{"l[:i] = :v", `token: ":i", near: "[:i]"`, ""},
		{"SET l[#n] = :v", `token: "#n", near: "[#n]"`, ""},
		{"a[0] = :v AND b[:j]", `token: ":j", near: "[:j]"`, ""},
		{"l[abc]", `token: "abc", near: "[abc]"`, ""},
		{"m.l[1].k[:z]", `token: ":z", near: "[:z]"`, ""},
		{"l[OR] = :v", `token: "OR", near: "[OR]"`, ""},
		{"l[and]", `token: "and", near: "[and]"`, ""},
		{"SET l[set] = :v", `token: "set", near: "[set]"`, ""},
		{"REMOVE l[in]", `token: "in", near: "[in]"`, ""},
		{"l[between]", `token: "between", near: "[between]"`, ""},
		{"l[007] = :v", `token: "0", near: "007"`, ""},
		// A token that is not a word: near runs from the bracket to the end of the token after it,
		// as Local words it.
		{"l[-1] = :v", `token: "-", near: "[-1"`, ""},
		{"SET l[+1] = :v", `token: "+", near: "[+1"`, ""},
		{"SET l[] = :v", `token: "]", near: "[] ="`, ""},
		{"l[]", `token: "]", near: "[]"`, ""},
		{"SET l[(] = :v", `token: "(", near: "[(]"`, ""},
		{"l[00]", `token: "0", near: "00]"`, ""},
		{"l[01]", `token: "1", near: "01]"`, ""},
		{"l[0123]", `token: "123", near: "0123]"`, ""},
		{"m[0][007]", `token: "0", near: "007"`, ""},
		{"l[2147483648] = :v", "", "2147483648"},
		{"l[9223372036854775808]", "", "9223372036854775808"},
		{"l[99999999999999999999999999]", "", "99999999999999999999999999"},
		{"l[2147483649] = :v, m[2147483648]", "", "2147483649"},
		{"l[2147483648], m[:i]", `token: ":i", near: "[:i]"`, "2147483648"},
		{"m[007], l[2147483648]", `token: "0", near: "007"`, "2147483648"},
		{"l[0] = :v", "", ""},
		{"l[2147483647]", "", ""},
		{"a = :i", "", ""},
		{"l[12][3] = :v", "", ""},
		{"l[ 0 ] = :v", "", ""},
		{"l[ 2147483648 ]", "", "2147483648"},
		{"", "", ""},
		{"a.b[1].c = :v, d[2] = e", "", ""},
	}

	for _, tt := range cases {
		scan := ScanExpression(tt.expression)

		if scan.IndexSyntax != tt.syntax {
			t.Errorf("ScanExpression(%q).IndexSyntax = %q, want %q", tt.expression, scan.IndexSyntax, tt.syntax)
		}

		want := ""
		if tt.rangeIdx != "" {
			want = fmt.Sprintf(rangeText, tt.rangeIdx)
		}

		if scan.IndexRange != want {
			t.Errorf("ScanExpression(%q).IndexRange = %q, want %q", tt.expression, scan.IndexRange, want)
		}
	}
}
