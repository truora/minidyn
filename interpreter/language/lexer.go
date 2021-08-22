package language

// Lexer DynamoDB expression lexer
type Lexer struct {
	input    string
	position int
	// current position in input (points to current char)
	readPosition int
	// current reading position in input (after current char)
	ch byte // current char under examination
}

var singleChar = map[byte]TokenType{
	'=': EQ,
	'(': LPAREN,
	')': RPAREN,
	',': COMMA,
	'+': PLUS,
	'-': MINUS,
}

var especialChars = map[byte]bool{
	'_': true,
	':': true,
	'#': true,
}

// NewLexer creates a new lexer
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()

	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}

	l.position = l.readPosition
	l.readPosition++
}

func (l *Lexer) manageLessThanToken() Token {
	var tok Token

	switch l.peekChar() {
	case '>':
		ch := l.ch
		l.readChar()

		tok = Token{Type: NotEQ, Literal: string(ch) + string(l.ch)}
	case '=':
		ch := l.ch
		l.readChar()

		tok = Token{Type: LTE, Literal: string(ch) + string(l.ch)}
	default:
		tok = newToken(LT, l.ch)
	}

	return tok
}

func (l *Lexer) manageGreaterThanToken() Token {
	var tok Token

	switch l.peekChar() {
	case '=':
		ch := l.ch
		l.readChar()

		tok = Token{Type: GTE, Literal: string(ch) + string(l.ch)}
	default:
		tok = newToken(GT, l.ch)
	}

	return tok
}

// NextToken look up for the next token
func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	single, ok := singleChar[l.ch]
	if ok {
		tok = newToken(single, l.ch)
		l.readChar()

		return tok
	}

	switch l.ch {
	case '<':
		tok = l.manageLessThanToken()
	case '>':
		tok = l.manageGreaterThanToken()
	case 0:
		tok.Literal = ""
		tok.Type = EOF
	default:
		if isIdentifierLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = LookupIdent(tok.Literal)

			return tok
		}

		tok = newToken(ILLEGAL, l.ch)
	}

	l.readChar()

	return tok
}

func (l *Lexer) readIdentifier() string {
	position := l.position

	for isIdentifierLetter(l.ch) {
		l.readChar()
	}

	return l.input[position:l.position]
}

func isIdentifierLetter(ch byte) bool {
	return isLetter(ch) || '0' <= ch && ch <= '9' || especialChars[ch]
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z'
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func newToken(tokenType TokenType, ch byte) Token {
	return Token{Type: tokenType, Literal: string(ch)}
}

func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}

	return l.input[l.readPosition]
}
