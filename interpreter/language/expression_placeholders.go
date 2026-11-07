package language

import "strings"

// Placeholders scans the expression once for attribute name placeholders (#name) and
// attribute value placeholders (:value), returning each one once in first-seen order. An
// empty expression yields no results. Lexing is the same for every expression kind.
func Placeholders(expression string) (names, values []string) {
	l := NewLexer(expression)

	seen := map[string]bool{}
	names = []string{}
	values = []string{}

	for tok := l.NextToken(); tok.Type != EOF; tok = l.NextToken() {
		if tok.Type != IDENT || seen[tok.Literal] {
			continue
		}

		switch {
		case strings.HasPrefix(tok.Literal, "#"):
			names = append(names, tok.Literal)
		case strings.HasPrefix(tok.Literal, ":"):
			values = append(values, tok.Literal)
		default:
			continue
		}

		seen[tok.Literal] = true
	}

	return names, values
}
