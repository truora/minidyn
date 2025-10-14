package partiql

import "fmt"

// Parser represents a PartiQL parser
type Parser struct {
	l              *Lexer
	curToken       Token
	peekToken      Token
	errors         []string
	prefixParseFns map[TokenType]prefixParseFn
	infixParseFns  map[TokenType]infixParseFn
}

type (
	prefixParseFn func() Expression
	infixParseFn  func(Expression) Expression
)

// Precedence levels
const (
	_ int = iota
	LOWEST
	OR_PRECEDENCE      // OR
	AND_PRECEDENCE     // AND
	EQUALS             // = or <>
	LESSGREATER        // < > <= >=
	BETWEEN_PRECEDENCE // BETWEEN
	IN_PRECEDENCE      // IN
	PREFIX             // NOT
	CALL               // function()
	INDEX              // array[index] or map.field
)

var precedences = map[TokenType]int{
	EQ:      EQUALS,
	NotEQ:   EQUALS,
	LT:      LESSGREATER,
	GT:      LESSGREATER,
	LTE:     LESSGREATER,
	GTE:     LESSGREATER,
	AND:     AND_PRECEDENCE,
	OR:      OR_PRECEDENCE,
	BETWEEN: BETWEEN_PRECEDENCE,
	IN:      IN_PRECEDENCE,
	DOT:     INDEX,
	LBRACKET: INDEX,
	LPAREN:  CALL,
}

// NewParser creates a new Parser
func NewParser(l *Lexer) *Parser {
	p := &Parser{
		l:      l,
		errors: []string{},
	}

	p.prefixParseFns = make(map[TokenType]prefixParseFn)
	p.registerPrefix(IDENT, p.parseIdentifier)
	p.registerPrefix(STRING, p.parseStringLiteral)
	p.registerPrefix(NUMBER, p.parseNumberLiteral)
	p.registerPrefix(PARAM, p.parseParameter)
	p.registerPrefix(TRUE, p.parseBoolean)
	p.registerPrefix(FALSE, p.parseBoolean)
	p.registerPrefix(NULL, p.parseNull)
	p.registerPrefix(NOT, p.parsePrefixExpression)
	p.registerPrefix(LPAREN, p.parseGroupedExpression)
	p.registerPrefix(LBRACE, p.parseMapLiteral)
	p.registerPrefix(LBRACKET, p.parseListLiteral)

	p.infixParseFns = make(map[TokenType]infixParseFn)
	p.registerInfix(EQ, p.parseInfixExpression)
	p.registerInfix(NotEQ, p.parseInfixExpression)
	p.registerInfix(LT, p.parseInfixExpression)
	p.registerInfix(GT, p.parseInfixExpression)
	p.registerInfix(LTE, p.parseInfixExpression)
	p.registerInfix(GTE, p.parseInfixExpression)
	p.registerInfix(AND, p.parseInfixExpression)
	p.registerInfix(OR, p.parseInfixExpression)
	p.registerInfix(BETWEEN, p.parseBetweenExpression)
	p.registerInfix(IN, p.parseInExpression)
	p.registerInfix(DOT, p.parseAttributePath)
	p.registerInfix(LBRACKET, p.parseIndexAccess)
	p.registerInfix(LPAREN, p.parseFunctionCall)

	// Read two tokens, so curToken and peekToken are both set
	p.nextToken()
	p.nextToken()

	return p
}

// Errors returns parser errors
func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) addError(msg string) {
	p.errors = append(p.errors, msg)
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

// ParseStatement parses a PartiQL statement
func (p *Parser) ParseStatement() Statement {
	switch p.curToken.Type {
	case SELECT:
		return p.parseSelectStatement()
	case INSERT:
		return p.parseInsertStatement()
	case UPDATE:
		return p.parseUpdateStatement()
	case DELETE:
		return p.parseDeleteStatement()
	default:
		p.addError(fmt.Sprintf("unexpected token: %s", p.curToken.Type))
		return nil
	}
}

func (p *Parser) parseSelectStatement() *SelectStatement {
	stmt := &SelectStatement{Token: p.curToken}

	p.nextToken() // move past SELECT

	// Parse projection (attributes or *)
	stmt.Projection = p.parseProjection()

	// Expect FROM
	if !p.expectPeek(FROM) {
		return nil
	}

	p.nextToken() // move past FROM

	// Parse table name
	if p.curToken.Type != IDENT && p.curToken.Type != STRING {
		p.addError(fmt.Sprintf("expected table name, got %s", p.curToken.Type))
		return nil
	}
	stmt.TableName = p.curToken.Literal

	p.nextToken()

	// Parse WHERE clause if present
	if p.curToken.Type == WHERE {
		p.nextToken()
		stmt.Where = p.parseExpression(LOWEST)
	}

	// Parse LIMIT clause if present
	if p.curToken.Type == LIMIT {
		p.nextToken()
		if p.curToken.Type != NUMBER {
			p.addError("expected number after LIMIT")
			return nil
		}
		// Convert string to int64
		var limit int64
		fmt.Sscanf(p.curToken.Literal, "%d", &limit)
		stmt.Limit = &limit
		p.nextToken()
	}

	return stmt
}

func (p *Parser) parseProjection() []Expression {
	projection := []Expression{}

	if p.curToken.Type == ASTERISK {
		projection = append(projection, &Identifier{Token: p.curToken, Value: "*"})
		p.nextToken()
		return projection
	}

	// Parse comma-separated list of expressions
	projection = append(projection, p.parseExpression(LOWEST))

	for p.peekToken.Type == COMMA {
		p.nextToken() // move to COMMA
		p.nextToken() // move past COMMA
		projection = append(projection, p.parseExpression(LOWEST))
	}

	return projection
}

func (p *Parser) parseInsertStatement() *InsertStatement {
	stmt := &InsertStatement{Token: p.curToken}

	if !p.expectPeek(INTO) {
		return nil
	}

	p.nextToken() // move past INTO

	// Parse table name
	if p.curToken.Type != IDENT && p.curToken.Type != STRING {
		p.addError(fmt.Sprintf("expected table name, got %s", p.curToken.Type))
		return nil
	}
	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(VALUE) {
		return nil
	}

	p.nextToken() // move past VALUE

	// Parse the value (should be a map literal)
	stmt.Value = p.parseExpression(LOWEST)

	return stmt
}

func (p *Parser) parseUpdateStatement() *UpdateStatement {
	stmt := &UpdateStatement{Token: p.curToken}

	p.nextToken() // move past UPDATE

	// Parse table name
	if p.curToken.Type != IDENT && p.curToken.Type != STRING {
		p.addError(fmt.Sprintf("expected table name, got %s", p.curToken.Type))
		return nil
	}
	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(SET) {
		return nil
	}

	p.nextToken() // move past SET

	// Parse SET clauses
	stmt.SetClauses = p.parseSetClauses()

	// Parse WHERE clause if present
	if p.curToken.Type == WHERE {
		p.nextToken()
		stmt.Where = p.parseExpression(LOWEST)
	}

	return stmt
}

func (p *Parser) parseSetClauses() []SetClause {
	clauses := []SetClause{}

	// Parse first SET clause
	attr := p.parseExpression(LOWEST)

	if !p.expectPeek(EQ) {
		return clauses
	}

	p.nextToken() // move past =

	value := p.parseExpression(LOWEST)

	clauses = append(clauses, SetClause{Attribute: attr, Value: value})

	// Parse additional SET clauses
	for p.peekToken.Type == COMMA {
		p.nextToken() // move to COMMA
		p.nextToken() // move past COMMA

		attr = p.parseExpression(LOWEST)

		if !p.expectPeek(EQ) {
			return clauses
		}

		p.nextToken() // move past =

		value = p.parseExpression(LOWEST)

		clauses = append(clauses, SetClause{Attribute: attr, Value: value})
	}

	return clauses
}

func (p *Parser) parseDeleteStatement() *DeleteStatement {
	stmt := &DeleteStatement{Token: p.curToken}

	if !p.expectPeek(FROM) {
		return nil
	}

	p.nextToken() // move past FROM

	// Parse table name
	if p.curToken.Type != IDENT && p.curToken.Type != STRING {
		p.addError(fmt.Sprintf("expected table name, got %s", p.curToken.Type))
		return nil
	}
	stmt.TableName = p.curToken.Literal

	p.nextToken()

	// Parse WHERE clause if present
	if p.curToken.Type == WHERE {
		p.nextToken()
		stmt.Where = p.parseExpression(LOWEST)
	}

	return stmt
}

// Expression parsing

func (p *Parser) parseExpression(precedence int) Expression {
	prefix := p.prefixParseFns[p.curToken.Type]
	if prefix == nil {
		p.addError(fmt.Sprintf("no prefix parse function for %s", p.curToken.Type))
		return nil
	}

	leftExp := prefix()

	for !p.peekTokenIs(EOF) && !p.peekTokenIs(SEMICOLON) && precedence < p.peekPrecedence() {
		infix := p.infixParseFns[p.peekToken.Type]
		if infix == nil {
			return leftExp
		}

		p.nextToken()
		leftExp = infix(leftExp)
	}

	return leftExp
}

func (p *Parser) parseIdentifier() Expression {
	return &Identifier{Token: p.curToken, Value: p.curToken.Literal}
}

func (p *Parser) parseStringLiteral() Expression {
	return &StringLiteral{Token: p.curToken, Value: p.curToken.Literal}
}

func (p *Parser) parseNumberLiteral() Expression {
	return &NumberLiteral{Token: p.curToken, Value: p.curToken.Literal}
}

func (p *Parser) parseParameter() Expression {
	return &ParameterExpression{Token: p.curToken, Name: p.curToken.Literal}
}

func (p *Parser) parseBoolean() Expression {
	return &BooleanLiteral{Token: p.curToken, Value: p.curToken.Type == TRUE}
}

func (p *Parser) parseNull() Expression {
	return &NullLiteral{Token: p.curToken}
}

func (p *Parser) parsePrefixExpression() Expression {
	expression := &PrefixExpression{
		Token:    p.curToken,
		Operator: p.curToken.Literal,
	}

	p.nextToken()
	expression.Right = p.parseExpression(PREFIX)

	return expression
}

func (p *Parser) parseGroupedExpression() Expression {
	p.nextToken()

	exp := p.parseExpression(LOWEST)

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return exp
}

func (p *Parser) parseInfixExpression(left Expression) Expression {
	expression := &InfixExpression{
		Token:    p.curToken,
		Operator: p.curToken.Literal,
		Left:     left,
	}

	precedence := p.curPrecedence()
	p.nextToken()
	expression.Right = p.parseExpression(precedence)

	return expression
}

func (p *Parser) parseBetweenExpression(left Expression) Expression {
	expression := &BetweenExpression{
		Token: p.curToken,
		Value: left,
	}

	p.nextToken()
	expression.Lower = p.parseExpression(LOWEST)

	if !p.expectPeek(AND) {
		return nil
	}

	p.nextToken()
	expression.Upper = p.parseExpression(LOWEST)

	return expression
}

func (p *Parser) parseInExpression(left Expression) Expression {
	expression := &InExpression{
		Token: p.curToken,
		Value: left,
	}

	if !p.expectPeek(LPAREN) {
		return nil
	}

	expression.Values = p.parseExpressionList(RPAREN)

	return expression
}

func (p *Parser) parseAttributePath(left Expression) Expression {
	path := &AttributePath{
		Token: p.curToken,
		Base:  left,
		Path:  []PathElement{},
	}

	p.nextToken()

	// Parse the field name
	field := p.parseExpression(INDEX)
	path.Path = append(path.Path, PathElement{Type: "field", Value: field})

	return path
}

func (p *Parser) parseIndexAccess(left Expression) Expression {
	path := &AttributePath{
		Token: p.curToken,
		Base:  left,
		Path:  []PathElement{},
	}

	p.nextToken()

	// Parse the index
	index := p.parseExpression(LOWEST)
	path.Path = append(path.Path, PathElement{Type: "index", Value: index})

	if !p.expectPeek(RBRACKET) {
		return nil
	}

	return path
}

func (p *Parser) parseFunctionCall(left Expression) Expression {
	fn, ok := left.(*Identifier)
	if !ok {
		p.addError("expected function name")
		return nil
	}

	call := &FunctionCall{
		Token:    p.curToken,
		Function: fn.Value,
	}

	call.Arguments = p.parseExpressionList(RPAREN)

	return call
}

func (p *Parser) parseMapLiteral() Expression {
	mapLit := &MapLiteral{
		Token: p.curToken,
		Pairs: make(map[Expression]Expression),
	}

	for !p.peekTokenIs(RBRACE) {
		p.nextToken()

		key := p.parseExpression(LOWEST)

		if !p.expectPeek(COLON) {
			return nil
		}

		p.nextToken()

		value := p.parseExpression(LOWEST)

		mapLit.Pairs[key] = value

		if !p.peekTokenIs(RBRACE) && !p.expectPeek(COMMA) {
			return nil
		}
	}

	if !p.expectPeek(RBRACE) {
		return nil
	}

	return mapLit
}

func (p *Parser) parseListLiteral() Expression {
	listLit := &ListLiteral{
		Token:    p.curToken,
		Elements: []Expression{},
	}

	listLit.Elements = p.parseExpressionList(RBRACKET)

	return listLit
}

func (p *Parser) parseExpressionList(end TokenType) []Expression {
	list := []Expression{}

	if p.peekTokenIs(end) {
		p.nextToken()
		return list
	}

	p.nextToken()
	list = append(list, p.parseExpression(LOWEST))

	for p.peekTokenIs(COMMA) {
		p.nextToken()
		p.nextToken()
		list = append(list, p.parseExpression(LOWEST))
	}

	if !p.expectPeek(end) {
		return nil
	}

	return list
}

// Helper functions

func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}

	p.addError(fmt.Sprintf("expected next token to be %s, got %s instead", t, p.peekToken.Type))
	return false
}

func (p *Parser) curPrecedence() int {
	if p, ok := precedences[p.curToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) peekPrecedence() int {
	if p, ok := precedences[p.peekToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) registerPrefix(tokenType TokenType, fn prefixParseFn) {
	p.prefixParseFns[tokenType] = fn
}

func (p *Parser) registerInfix(tokenType TokenType, fn infixParseFn) {
	p.infixParseFns[tokenType] = fn
}
