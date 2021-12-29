package language

import (
	"fmt"
)

// Parser represent the interpreter parser
type Parser struct {
	l         *Lexer
	curToken  Token
	peekToken Token
	errors    []string

	unsupported bool

	prefixParseFns map[TokenType]prefixParseFn
	infixParseFns  map[TokenType]infixParseFn
}

type (
	prefixParseFn func() Expression
	infixParseFn  func(Expression) Expression
)

const (
	_ int = iota
	precedenceValueLowset
	precedenceValueOR                // OR
	precedenceValueAND               // AND
	precedenceValueNOT               // NOT
	precedenceValueEqualComparators  // = <>
	precedenceValueBetweenComparator // BETWEEN
	precedenceValueComparators       // < <= > >=
	precedenceValueOperators         // + -
	precedenceValueCall              // myFunction(X)
	precedenceValueINDEX             // [] .
	precedenceValueInComparator      // IN
)

var precedences = map[TokenType]int{
	EQ:       precedenceValueEqualComparators,
	NotEQ:    precedenceValueEqualComparators,
	BETWEEN:  precedenceValueBetweenComparator,
	LT:       precedenceValueComparators,
	GT:       precedenceValueComparators,
	LTE:      precedenceValueComparators,
	GTE:      precedenceValueComparators,
	AND:      precedenceValueAND,
	OR:       precedenceValueOR,
	PLUS:     precedenceValueOperators,
	MINUS:    precedenceValueOperators,
	LPAREN:   precedenceValueCall,
	LBRACKET: precedenceValueINDEX,
	DOT:      precedenceValueINDEX,
	IN:       precedenceValueInComparator,
}

// NewParser creates a new parser
func NewParser(l *Lexer) *Parser {
	p := &Parser{
		l:      l,
		errors: []string{},
	}

	p.prefixParseFns = map[TokenType]prefixParseFn{}
	p.registerPrefix(IDENT, p.parseIdentifier)
	p.registerPrefix(NOT, p.parsePrefixExpression)
	p.registerPrefix(LPAREN, p.parseGroupedExpression)

	p.infixParseFns = make(map[TokenType]infixParseFn)
	p.registerInfix(EQ, p.parseInfixExpression)
	p.registerInfix(NotEQ, p.parseInfixExpression)
	p.registerInfix(LBRACKET, p.parseIndexExpression)
	p.registerInfix(DOT, p.parseIndexExpression)
	p.registerInfix(BETWEEN, p.parseBetweenExpression)
	p.registerInfix(LT, p.parseInfixExpression)
	p.registerInfix(GT, p.parseInfixExpression)
	p.registerInfix(LTE, p.parseInfixExpression)
	p.registerInfix(GTE, p.parseInfixExpression)
	p.registerInfix(AND, p.parseInfixExpression)
	p.registerInfix(OR, p.parseInfixExpression)
	p.registerInfix(LPAREN, p.parseCallExpression)
	p.registerInfix(IN, p.parseInExpression)

	// Read two tokens, so curToken and peekToken are both set
	p.nextToken()
	p.nextToken()

	return p
}

// NewUpdateParser creates a new parser for update expressions
func NewUpdateParser(l *Lexer) *Parser {
	p := &Parser{
		l:      l,
		errors: []string{},
	}

	p.prefixParseFns = map[TokenType]prefixParseFn{}
	p.registerPrefix(IDENT, p.parseIdentifier)

	p.registerPrefix(LPAREN, p.parseGroupedExpression)
	p.registerPrefix(SET, p.parseUpdateActionExpression)
	p.registerPrefix(ADD, p.parseUpdateActionExpression)
	p.registerPrefix(REMOVE, p.parseUpdateActionExpression)
	p.registerPrefix(DELETE, p.parseUpdateActionExpression)

	p.infixParseFns = make(map[TokenType]infixParseFn)
	p.registerInfix(LBRACKET, p.parseIndexExpression)
	p.registerInfix(DOT, p.parseIndexExpression)
	p.registerInfix(LPAREN, p.parseCallExpression)

	p.registerInfix(PLUS, p.parseInfixExpression)
	p.registerInfix(MINUS, p.parseInfixExpression)

	// Read two tokens, so curToken and peekToken are both set
	p.nextToken()
	p.nextToken()

	return p
}

// IsUnsupportedExpression return if the parsed expression is not a supported feature
func (p *Parser) IsUnsupportedExpression() bool {
	return p.unsupported
}

func (p *Parser) parseIdentifier() Expression {
	return &Identifier{Token: p.curToken, Value: p.curToken.Literal}
}

// Errors returns the errors found while parsing
func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

// ParseConditionalExpression tokenizes a ConditionalExpression
func (p *Parser) ParseConditionalExpression() *ConditionalExpression {
	stmt := &ConditionalExpression{Token: p.curToken}

	if p.curToken.Type == IDENT && p.peekToken.Type == EOF {
		msg := fmt.Sprintf("Syntax error; token: <EOF>, near: %q", p.curToken.Literal)
		p.errors = append(p.errors, msg)

		return stmt
	}

	for p.curToken.Type != EOF {
		stmt.Expression = p.parseExpression(precedenceValueLowset)

		p.nextToken()
	}

	return stmt
}

func (p *Parser) parseGroupedExpression() Expression {
	p.nextToken()
	exp := p.parseExpression(precedenceValueLowset)

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return exp
}

func (p *Parser) noPrefixParseFnError(t TokenType) {
	msg := fmt.Sprintf("no prefix parse function for %s found", t)
	p.errors = append(p.errors, msg)
}

func (p *Parser) parseExpression(precedence int) Expression {
	prefix, ok := p.prefixParseFns[p.curToken.Type]
	if !ok {
		p.noPrefixParseFnError(p.curToken.Type)

		return nil
	}

	leftExp := prefix()

	for !p.peekTokenIs(EOF) && precedence < p.peekPrecedence() {
		infix := p.infixParseFns[p.peekToken.Type]
		if infix == nil {
			return leftExp
		}

		p.nextToken()

		leftExp = infix(leftExp)
	}

	return leftExp
}

func (p *Parser) parsePrefixExpression() Expression {
	expression := &PrefixExpression{
		Token:    p.curToken,
		Operator: p.curToken.Literal,
	}

	p.nextToken()
	expression.Right = p.parseExpression(precedenceValueNOT)

	return expression
}

func (p *Parser) parseInfixExpression(left Expression) Expression {
	expression := &InfixExpression{
		Token:    p.curToken,
		Operator: p.curToken.Literal,
		Left:     left,
	}

	precedence := precedenceValueLowset
	if p, ok := precedences[p.curToken.Type]; ok {
		precedence = p
	}

	p.nextToken()
	expression.Right = p.parseExpression(precedence)

	return expression
}

func (p *Parser) parseCallExpression(function Expression) Expression {
	exp := &CallExpression{Token: p.curToken, Function: function}

	exp.Arguments = p.parseCallArguments()

	return exp
}

func (p *Parser) parseIndexExpression(left Expression) Expression {
	expression := &IndexExpression{Token: p.curToken, Left: left, Type: ObjectTypeList}

	p.nextToken()

	expression.Index = p.parseIdentifier()

	if expression.Token.Type == DOT {
		expression.Type = ObjectTypeMap
		return expression
	}

	if !p.expectPeek(RBRACKET) {
		return nil
	}

	return expression
}

func (p *Parser) parseBetweenExpression(left Expression) Expression {
	expression := &BetweenExpression{
		Token: p.curToken,
		Left:  left,
		Range: [2]Expression{},
	}

	p.nextToken()
	expression.Range[0] = p.parseIdentifier()

	if !p.expectPeek(AND) {
		return nil
	}

	p.nextToken()
	expression.Range[1] = p.parseIdentifier()

	return expression
}

func (p *Parser) parseInExpression(left Expression) Expression {
	p.nextToken()

	return &InExpression{
		Token: p.curToken,
		Left:  left,
		Range: p.parseCallArguments(),
	}
}

func (p *Parser) parseCallArguments() []Expression {
	args := []Expression{}

	if p.peekTokenIs(RPAREN) {
		p.nextToken()
		return args
	}

	p.nextToken()
	args = append(args, p.parseExpression(precedenceValueLowset))

	for p.peekTokenIs(COMMA) {
		p.nextToken()
		p.nextToken()
		args = append(args, p.parseExpression(precedenceValueLowset))
	}

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return args
}

// ParseUpdateExpression it tokenizes the update expression and returns an UpdateStatement
func (p *Parser) ParseUpdateExpression() *UpdateStatement {
	stmt := &UpdateStatement{Token: p.curToken}

	for p.curToken.Type != EOF {
		stmt.Expression = p.parseExpression(precedenceValueLowset)

		p.nextToken()
	}

	return stmt
}

func (p *Parser) parseUnsupportedExpression() Expression {
	msg := fmt.Sprintf("the %s expression is not supported yet", p.curToken.Type)
	p.errors = append(p.errors, msg)

	p.unsupported = true

	return nil
}

func (p *Parser) parseUpdateActionExpression() Expression {
	expression := &UpdateExpression{
		Token:       p.curToken,
		Expressions: p.parseActions(p.curToken),
	}

	return expression
}

func (p *Parser) parseAction(token Token) *ActionExpression {
	action := &ActionExpression{
		Token: token,
		Left:  p.parseExpression(precedenceValueLowset),
	}

	if token.Type == SET && !p.expectPeek(EQ) {
		return nil
	}

	if token.Type != REMOVE {
		p.nextToken()

		action.Right = p.parseExpression(precedenceValueLowset)
	}

	return action
}

func (p *Parser) parseActions(token Token) []Expression {
	actions := []Expression{}

	if p.peekTokenIs(EOF) {
		return actions
	}

	p.nextToken()

	actions = append(actions, p.parseAction(token))

	for p.peekTokenIs(COMMA) {
		p.nextToken()
		p.nextToken()

		actions = append(actions, p.parseAction(token))
	}

	if !p.expectPeek(EOF) {
		return nil
	}

	return actions
}

// helpers

func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) expectPeek(t TokenType) bool {
	if !p.peekTokenIs(t) {
		p.peekError(t)

		return false
	}

	p.nextToken()

	return true
}

func (p *Parser) peekError(t TokenType) {
	msg := fmt.Sprintf("expected next token to be %s, got %s instead",
		t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
}

func (p *Parser) registerPrefix(tokenType TokenType, fn prefixParseFn) {
	p.prefixParseFns[tokenType] = fn
}

func (p *Parser) registerInfix(tokenType TokenType, fn infixParseFn) {
	p.infixParseFns[tokenType] = fn
}

func (p *Parser) peekPrecedence() int {
	if p, ok := precedences[p.peekToken.Type]; ok {
		return p
	}

	return precedenceValueLowset
}
