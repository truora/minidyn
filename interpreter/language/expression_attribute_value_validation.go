package language

import "strings"

// UndeclaredExpressionAttributeValues scans the expression for attribute value placeholders
// (:value) that declared reports as missing from ExpressionAttributeValues, returning each
// undeclared placeholder once in first-seen order. An empty expression yields no results.
func UndeclaredExpressionAttributeValues(expression string, declared func(string) bool) []string {
	l := NewLexer(expression)

	seen := map[string]bool{}
	undeclared := []string{}

	for tok := l.NextToken(); tok.Type != EOF; tok = l.NextToken() {
		if tok.Type != IDENT || !strings.HasPrefix(tok.Literal, ":") {
			continue
		}

		if declared(tok.Literal) || seen[tok.Literal] {
			continue
		}

		seen[tok.Literal] = true
		undeclared = append(undeclared, tok.Literal)
	}

	return undeclared
}
