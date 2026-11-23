package language

import (
	"fmt"
	"strconv"
)

// maxListIndex is the largest list index DynamoDB accepts.
const maxListIndex = 1<<31 - 1

// classifyListIndex says what DynamoDB Local does with token, the text between the brackets of a
// list path, as the one place that knows: syntax is the detail of the syntax error it reports
// while parsing (a token that is not digits, or a literal with a leading zero), and rangeErr the
// detail of the error it reports once the whole text parsed (a literal above 2147483647). Both
// are empty for a valid index.
func classifyListIndex(token string) (syntax, rangeErr string) {
	switch {
	case !isListIndexLiteral(token):
		return ListIndexSyntaxDetail(token), ""
	case len(token) > 1 && token[0] == '0':
		return leadingZeroDetail(token), ""
	}

	if n, err := strconv.ParseInt(token, 10, 64); err != nil || n > maxListIndex {
		return "", ListIndexRangeMessage(token)
	}

	return "", ""
}

// ListIndexRangeMessage is DynamoDB Local's error for a literal list index above the range,
// which has no token or position.
func ListIndexRangeMessage(literal string) string {
	return fmt.Sprintf("List index is not within the allowable range; index: [%s]", literal)
}

// leadingZeroDetail is the syntax error detail for a literal such as 007. Local reads the first
// zero as the whole index, so the offending token is what follows it, and near joins the zero,
// that token and the one after it: l[01] gives token "1", near "01]"; l[007] gives token "0",
// near "007", as Local's lexer reads a zero as a number of its own and any other digits as one.
// The token after the literal is taken to be the closing bracket, so the whitespace between a
// literal and its bracket is not part of near.
func leadingZeroDetail(literal string) string {
	rest := literal[1:]
	token, next := rest, "]"

	if rest[0] == '0' {
		token = "0"

		if len(rest) > 1 {
			next = rest[1:]
			if rest[1] == '0' {
				next = "0"
			}
		}
	}

	return fmt.Sprintf("token: %q, near: %q", token, "0"+token+next)
}
