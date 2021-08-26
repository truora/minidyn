package language

import (
	"bytes"
	"strings"
)

// Node the AST node type
type Node interface {
	TokenLiteral() string
	String() string
}

// Statement represents the node type statement
type Statement interface {
	Node
	statementNode()
}

// Expression represents the node type expression
type Expression interface {
	Node
	expressionNode()
}

// Identifier identifier expression node
type Identifier struct {
	Token Token // the token.IDENT token
	Value string
}

func (i *Identifier) expressionNode() {
	_ = 1 // HACK for passing coverage
}

// TokenLiteral returns the literal token of the node
func (i *Identifier) TokenLiteral() string { return i.Token.Literal }

func (i *Identifier) String() string { return i.Value }

// ConditionalExpression is the conditional expression root node
type ConditionalExpression struct {
	Token      Token // the return token
	Expression Expression
}

func (es *ConditionalExpression) statementNode() {
	_ = 1 // HACK for passing coverage
}

// TokenLiteral returns the literal token of the node
func (ce *ConditionalExpression) TokenLiteral() string { return ce.Token.Literal }

func (ce *ConditionalExpression) String() string {
	if ce.Expression != nil {
		return ce.Expression.String()
	}

	return ""
}

// PrefixExpression prefix operator expression
type PrefixExpression struct {
	Token    Token // The prefix token, e.g. NOT
	Operator string
	Right    Expression
}

func (pe *PrefixExpression) expressionNode() {
	_ = 1 // HACK for passing coverage
}

// TokenLiteral returns the literal token of the node
func (pe *PrefixExpression) TokenLiteral() string {
	return pe.Token.Literal
}

func (pe *PrefixExpression) String() string {
	var out bytes.Buffer

	out.WriteString("(")
	out.WriteString(pe.Operator)
	out.WriteString(pe.Right.String())
	out.WriteString(")")

	return out.String()
}

// InfixExpression infix operator expression
type InfixExpression struct {
	Token    Token // The operator token, e.g. =
	Left     Expression
	Operator string
	Right    Expression
}

func (oe *InfixExpression) expressionNode() {
	_ = 1 // HACK for passing coverage
}

// TokenLiteral returns the literal token of the node
func (oe *InfixExpression) TokenLiteral() string {
	return oe.Token.Literal
}

func (oe *InfixExpression) String() string {
	var out bytes.Buffer

	out.WriteString("(")
	out.WriteString(oe.Left.String())
	out.WriteString(" " + oe.Operator + " ")
	out.WriteString(oe.Right.String())
	out.WriteString(")")

	return out.String()
}

// CallExpression function call expression
type CallExpression struct {
	Token    Token // The '(' token
	Function Expression
	// Identifier or FunctionLiteral
	Arguments []Expression
}

func (ce *CallExpression) expressionNode() {
	_ = 1 // HACK for passing coverage
}

// TokenLiteral returns the literal token of the node
func (ce *CallExpression) TokenLiteral() string {
	return ce.Token.Literal
}

func (ce *CallExpression) String() string {
	var out bytes.Buffer

	args := []string{}
	for _, a := range ce.Arguments {
		args = append(args, a.String())
	}

	out.WriteString(ce.Function.String())
	out.WriteString("(")
	out.WriteString(strings.Join(args, ", "))
	out.WriteString(")")

	return out.String()
}

// BetweenExpression function between expression
type BetweenExpression struct {
	Token Token // The 'BETWEEN' token
	Left  Expression
	// Identifiers
	Range [2]Expression
}

func (ce *BetweenExpression) expressionNode() {
	_ = 1 // HACK for passing coverage
}

// TokenLiteral returns the literal token of the node
func (ce *BetweenExpression) TokenLiteral() string {
	return ce.Token.Literal
}

func (ce *BetweenExpression) String() string {
	var out bytes.Buffer

	out.WriteString(ce.Left.String())
	out.WriteString(" BETWEEN ")
	out.WriteString(ce.Range[0].String())
	out.WriteString(" AND ")
	out.WriteString(ce.Range[1].String())

	return out.String()
}

// UpdateExpression is the update expression root node
type UpdateExpression struct {
	Token      Token // the action token
	Expression Expression
}

func (ue *UpdateExpression) statementNode() {
	_ = 1 // HACK for passing coverage
}

// TokenLiteral returns the literal token of the node
func (ue *UpdateExpression) TokenLiteral() string { return ue.Token.Literal }

func (ue *UpdateExpression) String() string {
	if ue.Expression != nil {
		return ue.Expression.String()
	}

	return ""
}

// SetExpression is the set expression
type SetExpression struct {
	Token       Token // set
	Expressions []Expression
}

func (st *SetExpression) expressionNode() {
	_ = 1 // HACK for passing coverage
}

// TokenLiteral returns the literal token of the node
func (st *SetExpression) TokenLiteral() string { return st.Token.Literal }

func (st *SetExpression) String() string {
	var out bytes.Buffer

	out.WriteString("SET (")
	out.WriteString(st.Token.Literal)
	for _, exp := range st.Expressions {
		out.WriteString(exp.String())
	}
	out.WriteString(")")

	return out.String()
}
