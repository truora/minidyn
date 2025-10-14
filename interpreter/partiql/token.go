package partiql

// TokenType represents the type of token
type TokenType string

// Token represents a lexical token
type Token struct {
	Type    TokenType
	Literal string
}

// Token types for PartiQL
const (
	// Special tokens
	ILLEGAL TokenType = "ILLEGAL"
	EOF     TokenType = "EOF"

	// Identifiers and literals
	IDENT  TokenType = "IDENT"  // table_name, attribute_name
	STRING TokenType = "STRING" // "string value" or 'string value'
	NUMBER TokenType = "NUMBER" // 123, 123.45
	PARAM  TokenType = "PARAM"  // ? or :name

	// Operators
	ASTERISK TokenType = "*"
	COMMA    TokenType = ","
	DOT      TokenType = "."

	EQ    TokenType = "="
	NotEQ TokenType = "<>"
	LT    TokenType = "<"
	GT    TokenType = ">"
	LTE   TokenType = "<="
	GTE   TokenType = ">="

	LPAREN   TokenType = "("
	RPAREN   TokenType = ")"
	LBRACKET TokenType = "["
	RBRACKET TokenType = "]"
	LBRACE   TokenType = "{"
	RBRACE   TokenType = "}"

	COLON     TokenType = ":"
	SEMICOLON TokenType = ";"

	// Keywords
	SELECT   TokenType = "SELECT"
	FROM     TokenType = "FROM"
	WHERE    TokenType = "WHERE"
	INSERT   TokenType = "INSERT"
	INTO     TokenType = "INTO"
	VALUE    TokenType = "VALUE"
	UPDATE   TokenType = "UPDATE"
	SET      TokenType = "SET"
	DELETE   TokenType = "DELETE"
	AND      TokenType = "AND"
	OR       TokenType = "OR"
	NOT      TokenType = "NOT"
	BETWEEN  TokenType = "BETWEEN"
	IN       TokenType = "IN"
	IS       TokenType = "IS"
	NULL     TokenType = "NULL"
	MISSING  TokenType = "MISSING"
	AS       TokenType = "AS"
	ORDER    TokenType = "ORDER"
	BY       TokenType = "BY"
	ASC      TokenType = "ASC"
	DESC     TokenType = "DESC"
	LIMIT    TokenType = "LIMIT"
	TRUE     TokenType = "TRUE"
	FALSE    TokenType = "FALSE"
	CONTAINS TokenType = "CONTAINS"
)

var keywords = map[string]TokenType{
	"SELECT":   SELECT,
	"FROM":     FROM,
	"WHERE":    WHERE,
	"INSERT":   INSERT,
	"INTO":     INTO,
	"VALUE":    VALUE,
	"UPDATE":   UPDATE,
	"SET":      SET,
	"DELETE":   DELETE,
	"AND":      AND,
	"OR":       OR,
	"NOT":      NOT,
	"BETWEEN":  BETWEEN,
	"IN":       IN,
	"IS":       IS,
	"NULL":     NULL,
	"MISSING":  MISSING,
	"AS":       AS,
	"ORDER":    ORDER,
	"BY":       BY,
	"ASC":      ASC,
	"DESC":     DESC,
	"LIMIT":    LIMIT,
	"TRUE":     TRUE,
	"FALSE":    FALSE,
	"CONTAINS": CONTAINS,
}

// LookupIdent checks if the identifier is a keyword
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}

	return IDENT
}
