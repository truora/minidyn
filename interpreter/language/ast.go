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

// IndexExpression access list index expression
type IndexExpression struct {
	Token Token // The [ token
	Left  Expression
	Index Expression
	Type  ObjectType
}

func (ie *IndexExpression) expressionNode() {
	_ = 1 // HACK for passing coverage
}

func (ie *IndexExpression) TokenLiteral() string { return ie.Token.Literal }

func (ie *IndexExpression) String() string {
	var out bytes.Buffer
	out.WriteString("(")
	out.WriteString(ie.Left.String())
	out.WriteString("[")
	out.WriteString(ie.Index.String())
	out.WriteString("])")

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

// UpdateStatement is the update expression root node
type UpdateStatement struct {
	Token      Token // the action token
	Expression Expression
}

func (us *UpdateStatement) statementNode() {
	_ = 1 // HACK for passing coverage
}

// TokenLiteral returns the literal token of the node
func (us *UpdateStatement) TokenLiteral() string { return us.Token.Literal }

func (us *UpdateStatement) String() string {
	if us.Expression != nil {
		return us.Expression.String()
	}

	return ""
}

// UpdateExpression is the update expression
type UpdateExpression struct {
	Token       Token // set
	Expressions []Expression
}

func (ue *UpdateExpression) expressionNode() {
	_ = 1 // HACK for passing coverage
}

// TokenLiteral returns the literal token of the node
func (ue *UpdateExpression) TokenLiteral() string { return ue.Token.Literal }

func (ue *UpdateExpression) String() string {
	var out bytes.Buffer

	out.WriteString("(")
	for _, exp := range ue.Expressions {
		out.WriteString(exp.String())
	}
	out.WriteString(")")

	return out.String()
}

// ActionExpression is the action expression of an update expression
type ActionExpression struct {
	Token Token // ADD REMOVE DELETE
	Left  Expression
	Right Expression
}

func (st *ActionExpression) expressionNode() {
	_ = 1 // HACK for passing coverage
}

// TokenLiteral returns the literal token of the node
func (st *ActionExpression) TokenLiteral() string { return st.Token.Literal }

func (st *ActionExpression) String() string {
	var out bytes.Buffer

	out.WriteString(st.TokenLiteral())

	out.WriteString(" (")

	out.WriteString(st.Left.String())

	sep := ", "
	if st.Token.Type == SET {
		sep = " = "
	}

	out.WriteString(sep)

	out.WriteString(st.Right.String())

	out.WriteString(")")

	return out.String()
}

// InExpression function in expression
type InExpression struct {
	Token Token // The 'IN' token
	Left  Expression
	// Identifiers
	Range []Expression
}

func (ine *InExpression) expressionNode() {
	_ = 1 // HACK for passing coverage
}

// TokenLiteral returns the literal token of the node
func (ine *InExpression) TokenLiteral() string { return ine.Token.Literal }

func (ine *InExpression) String() string {
	var out bytes.Buffer

	out.WriteString("(")
	out.WriteString(ine.Left.String())

	out.WriteString(" IN (")

	for i, e := range ine.Range {
		out.WriteString(e.TokenLiteral())

		if i < len(ine.Range)-1 {
			out.WriteString(", ")
		}
	}

	out.WriteString("))")

	return out.String()
}
