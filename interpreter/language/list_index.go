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

// isWordToken reports whether tok is a word: a name, a placeholder, a number or a keyword such
// as OR or SET. DynamoDB's lexer does not tell a keyword from a name between brackets, so every
// word there is a token for classifyListIndex.
func isWordToken(tok Token) bool {
	switch tok.Type {
	case IDENT, AND, OR, NOT, BETWEEN, IN, SET, REMOVE, ADD, DELETE:
		return true
	}

	return false
}

// classifyListIndexToken is classifyListIndex for the first token after a bracket, whatever it
// is: a word is classified by its text, and any other token, such as the - of l[-1] or the ] of
// l[], is a syntax error. near is how Local words the context of that token: the source from the
// bracket to the end of the token after it.
func classifyListIndexToken(tok Token, near string) (syntax, rangeErr string) {
	if isWordToken(tok) {
		return classifyListIndex(tok.Literal)
	}

	return fmt.Sprintf("token: %q, near: %q", tok.Literal, near), ""
}

// isListIndexLiteral reports whether literal is made of digits only, as DynamoDB requires
// between the brackets. Whether the number is a valid index is classifyListIndex's question.
func isListIndexLiteral(literal string) bool {
	if literal == "" {
		return false
	}

	for i := range len(literal) {
		if literal[i] < '0' || literal[i] > '9' {
			return false
		}
	}

	return true
}

// ListIndexSyntaxDetail is the detail of the syntax error for token between brackets, as
// DynamoDB Local words it.
func ListIndexSyntaxDetail(token string) string {
	return fmt.Sprintf("token: %q, near: %q", token, "["+token+"]")
}

// listIndexLiteral converts the literal index of a path, such as the 3 of l[3], for the evaluator
// and ExtractPath, which read the same text as the parser and cannot disagree with it: the text
// is classified by classifyListIndex, and reason is DynamoDB's message for an index it refuses.
// The parser already rejects a text that is not a plain literal in range, so a reason can only
// come from a tree built without it, and it is the range message whatever the text is. The
// caller builds the error, as an assignment names the field and a read does not.
func listIndexLiteral(literal string) (idx int64, reason string) {
	if syntax, rangeErr := classifyListIndex(literal); syntax != "" || rangeErr != "" {
		return 0, ListIndexRangeMessage(literal)
	}

	n, _ := strconv.ParseInt(literal, 10, 64)

	return n, ""
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
