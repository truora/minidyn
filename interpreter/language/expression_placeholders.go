package language

import "strings"

// ExpressionScan is what one lexer pass finds in an expression text.
type ExpressionScan struct {
	// Names are the #name placeholders, each once, in first-seen order.
	Names []string
	// Values are the :value placeholders, each once, in first-seen order.
	Values []string
	// IndexSyntax is the detail of the first syntax error DynamoDB finds in a list index while
	// parsing: a token between [ and ] that is not an integer literal, or a literal with a
	// leading zero. It is "" when there is none.
	IndexSyntax string
	// IndexRange is the error for the first literal list index above DynamoDB's range, or "".
	// DynamoDB reports it after the syntax checks.
	IndexRange string
}

// ScanExpression lexes the expression once. An empty expression yields an empty scan. Lexing is
// the same for every expression kind.
func ScanExpression(expression string) ExpressionScan {
	l := NewLexer(expression)

	seen := map[string]bool{}
	scan := ExpressionScan{Names: []string{}, Values: []string{}}
	afterBracket := false

	for tok := l.NextToken(); tok.Type != EOF; tok = l.NextToken() {
		wasAfterBracket := afterBracket
		afterBracket = tok.Type == LBRACKET

		if !isWordToken(tok) {
			continue
		}

		if wasAfterBracket {
			scan.noteListIndex(tok.Literal)
		}

		if tok.Type != IDENT {
			continue
		}

		if seen[tok.Literal] {
			continue
		}

		switch {
		case strings.HasPrefix(tok.Literal, "#"):
			scan.Names = append(scan.Names, tok.Literal)
		case strings.HasPrefix(tok.Literal, ":"):
			scan.Values = append(scan.Values, tok.Literal)
		default:
			continue
		}

		seen[tok.Literal] = true
	}

	return scan
}

// noteListIndex keeps the first syntax error and the first range error among the list indexes.
func (s *ExpressionScan) noteListIndex(token string) {
	syntax, rangeErr := classifyListIndex(token)

	if s.IndexSyntax == "" {
		s.IndexSyntax = syntax
	}

	if s.IndexRange == "" {
		s.IndexRange = rangeErr
	}
}

// Placeholders scans the expression once for attribute name placeholders (#name) and
// attribute value placeholders (:value), returning each one once in first-seen order. An
// empty expression yields no results.
func Placeholders(expression string) (names, values []string) {
	scan := ScanExpression(expression)

	return scan.Names, scan.Values
}
