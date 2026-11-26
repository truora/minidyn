package language

import (
	"fmt"
	"slices"
)

// Parser represent the interpreter parser
type Parser struct {
	l         *Lexer
	curToken  Token
	peekToken Token
	// prevStart is where the token before curToken starts in the input, curEnd where curToken
	// ends and peekEnd where peekToken ends, to quote the context of a list index error.
	prevStart, curStart, peekStart, curEnd, peekEnd int
	errors                                          []string

	unsupported bool

	// indexRange is the error for the first literal list index above DynamoDB's range. It is
	// kept out of errors because DynamoDB reports it only when the text has no syntax error.
	indexRange string

	// indexSyntax is the detail of the list index syntax error when it is the first error of
	// the parse, which is the one DynamoDB reports; empty when the first error is another one.
	indexSyntax string

	// updateClauseHeaders tracks SET/ADD/REMOVE/DELETE clause headers for the
	// current ParseUpdateExpression (each keyword at most once, per DynamoDB).
	updateClauseHeaders map[TokenType]bool

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

// checkListIndex applies DynamoDB's rules for a list index to the token between brackets: a
// token that is not digits, or a literal with a leading zero, is a syntax error with the text
// Local reports; a literal above the range is only recorded (IndexRangeError).
func (p *Parser) checkListIndex() {
	// near runs from the bracket to the end of the token after the index.
	end := p.curEnd
	if p.peekToken.Type != EOF {
		end = p.peekEnd
	}

	syntax, rangeErr := classifyListIndexToken(p.curToken, p.l.input[p.prevStart:end])

	if syntax != "" {
		if len(p.errors) == 0 {
			p.indexSyntax = syntax
		}

		p.errors = append(p.errors, "Syntax error; "+syntax)
	}

	if rangeErr != "" && p.indexRange == "" {
		p.indexRange = rangeErr
	}
}

// IndexSyntaxError returns the detail ("token: ..., near: ...") of the syntax error of a list
// index when it is the first error of the parse, as DynamoDB reports only the first, or "".
func (p *Parser) IndexSyntaxError() string {
	return p.indexSyntax
}

// IndexRangeError returns the error DynamoDB reports for the first literal list index above
// 2147483647, or "". It is only meaningful when Errors is empty: DynamoDB reports a syntax
// error anywhere in the text first.
func (p *Parser) IndexRangeError() string {
	return p.indexRange
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
	p.prevStart, p.curStart, p.curEnd = p.curStart, p.peekStart, p.peekEnd
	p.peekToken, p.peekStart, p.peekEnd = p.l.nextSpan()
}

// ParseProjectionExpression parses a comma-separated list of path expressions (ProjectionExpression).
// Whitespace may appear between paths. An empty input yields an empty slice and no errors.
func (p *Parser) ParseProjectionExpression() []Expression {
	var expressions []Expression

	if p.curToken.Type == EOF {
		return expressions
	}

	expressions = append(expressions, p.parseExpression(precedenceValueLowset))

	for p.peekTokenIs(COMMA) {
		p.nextToken()
		p.nextToken()
		expressions = append(expressions, p.parseExpression(precedenceValueLowset))
	}

	if !p.peekTokenIs(EOF) {
		p.peekError(EOF)
	}

	return expressions
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
		Operator: string(p.curToken.Type),
	}

	p.nextToken()
	expression.Right = p.parseExpression(precedenceValueNOT)

	return expression
}

func (p *Parser) parseInfixExpression(left Expression) Expression {
	expression := &InfixExpression{
		Token:    p.curToken,
		Operator: string(p.curToken.Type),
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

	// DynamoDB takes only an integer literal as a list index: a placeholder, a name or a keyword
	// is a syntax error (#164), so the parse carries it and no expression is evaluated to find it.
	if expression.Token.Type == LBRACKET && p.curToken.Type != EOF {
		p.checkListIndex()
	}

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
		p.errors = append(p.errors, "a function call needs at least one operand")

		return nil
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
	p.updateClauseHeaders = make(map[TokenType]bool)

	stmt := &UpdateStatement{Token: p.curToken}

	if p.curToken.Type == EOF {
		return stmt
	}

	stmt.Expression = p.parseExpression(precedenceValueLowset)

	if !p.peekTokenIs(EOF) {
		msg := fmt.Sprintf("unexpected token after update expression: %s", p.peekToken.Type)
		p.errors = append(p.errors, msg)
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

	if p.updateClauseHeaders == nil {
		p.updateClauseHeaders = make(map[TokenType]bool)
	}

	if p.updateClauseHeaders[token.Type] {
		msg := fmt.Sprintf("duplicate update clause keyword: %s", token.Literal)
		p.errors = append(p.errors, msg)

		return nil
	}

	p.updateClauseHeaders[token.Type] = true

	if p.peekTokenIs(EOF) {
		return actions
	}

	p.nextToken()

	actions = append(actions, p.parseAction(token))
	updateTokens := []TokenType{SET, ADD, REMOVE, DELETE}

	for p.peekTokenIs(COMMA) || p.tokenIsOneOf(updateTokens) {
		if p.peekTokenIs(COMMA) {
			p.nextToken()
			p.nextToken()

			actions = append(actions, p.parseAction(token))

			continue
		}

		p.nextToken()

		otherUpdate := p.parseUpdateActionExpression()
		if updateExpression, ok := otherUpdate.(*UpdateExpression); ok {
			actions = append(actions, updateExpression.Expressions...)
		}
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

func (p *Parser) tokenIsOneOf(ts []TokenType) bool {
	return slices.ContainsFunc(ts, p.peekTokenIs)
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
