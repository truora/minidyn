package language

import "strings"

// UndeclaredExpressionAttributeNames scans the expression for attribute name placeholders
// (#name) that are not declared in aliases (ExpressionAttributeNames), returning each
// undeclared placeholder once in first-seen order. An empty expression yields no results.
func UndeclaredExpressionAttributeNames(expression string, aliases map[string]string) []string {
	l := NewLexer(expression)

	seen := map[string]bool{}
	undeclared := []string{}

	for tok := l.NextToken(); tok.Type != EOF; tok = l.NextToken() {
		if tok.Type != IDENT || !strings.HasPrefix(tok.Literal, "#") {
			continue
		}

		if _, ok := aliases[tok.Literal]; ok {
			continue
		}

		if seen[tok.Literal] {
			continue
		}

		seen[tok.Literal] = true
		undeclared = append(undeclared, tok.Literal)
	}

	return undeclared
}
