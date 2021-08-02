package language

// TokenType represents the type of the token
type TokenType string

// Token represents a token in of the DynamoDB's expression language
type Token struct {
	Type    TokenType
	Literal string
}

const (
	// ILLEGAL illegal token
	ILLEGAL TokenType = "ILLEGAL"
	// EOF end of the file(input)
	EOF TokenType = "EOF"

	// IDENT identifier operand or function
	IDENT TokenType = "IDENT"

	// LT logical comparator less than
	LT = "<"
	// LTE logical comparator less than or equal
	LTE = "<="
	// GT logical comparator greater than
	GT = ">"
	// GTE logical comparator greater than or equal
	GTE = ">="
	// EQ logical comparator equal
	EQ = "="
	// NotEQ logical comparator not equal
	NotEQ = "<>"

	// COMMA delimiter used with IN keyword
	COMMA TokenType = ","

	// LPAREN left parentheses delimiter
	LPAREN TokenType = "("
	// RPAREN right parentheses delimiter
	RPAREN TokenType = ")"

	// AND logical evaluation keyword
	AND = "AND"
	// OR logical evaluation keyword
	OR = "OR"
	// NOT logical evaluation keyword
	NOT = "NOT"
	// BETWEEN compare operand against a range
	BETWEEN = "BETWEEN"
	// IN compare operand against list of values
	IN = "IN"
)

var keywords = map[string]TokenType{
	"AND":     AND,
	"OR":      OR,
	"NOT":     NOT,
	"BETWEEN": BETWEEN,
	"IN":      IN,
}

// LookupIdent checks if the ident is a keyword
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}

	return IDENT
}
