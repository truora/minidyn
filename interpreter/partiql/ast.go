package partiql

// Node represents a node in the AST
type Node interface {
	TokenLiteral() string
	String() string
}

// Statement represents a SQL statement
type Statement interface {
	Node
	statementNode()
}

// Expression represents an expression
type Expression interface {
	Node
	expressionNode()
}

// SelectStatement represents a SELECT query
type SelectStatement struct {
	Token      Token // the SELECT token
	Projection []Expression
	TableName  string
	Where      Expression
	Limit      *int64
}

func (s *SelectStatement) statementNode()       {}
func (s *SelectStatement) TokenLiteral() string { return s.Token.Literal }
func (s *SelectStatement) String() string       { return "SELECT statement" }

// InsertStatement represents an INSERT statement
type InsertStatement struct {
	Token     Token // the INSERT token
	TableName string
	Value     Expression // typically a MapLiteral
}

func (i *InsertStatement) statementNode()       {}
func (i *InsertStatement) TokenLiteral() string { return i.Token.Literal }
func (i *InsertStatement) String() string       { return "INSERT statement" }

// UpdateStatement represents an UPDATE statement
type UpdateStatement struct {
	Token      Token // the UPDATE token
	TableName  string
	SetClauses []SetClause
	Where      Expression
}

func (u *UpdateStatement) statementNode()       {}
func (u *UpdateStatement) TokenLiteral() string { return u.Token.Literal }
func (u *UpdateStatement) String() string       { return "UPDATE statement" }

// SetClause represents a SET clause in UPDATE
type SetClause struct {
	Attribute Expression
	Value     Expression
}

// DeleteStatement represents a DELETE statement
type DeleteStatement struct {
	Token     Token // the DELETE token
	TableName string
	Where     Expression
}

func (d *DeleteStatement) statementNode()       {}
func (d *DeleteStatement) TokenLiteral() string { return d.Token.Literal }
func (d *DeleteStatement) String() string       { return "DELETE statement" }

// Identifier represents an identifier
type Identifier struct {
	Token Token
	Value string
}

func (i *Identifier) expressionNode()      {}
func (i *Identifier) TokenLiteral() string { return i.Token.Literal }
func (i *Identifier) String() string       { return i.Value }

// StringLiteral represents a string literal
type StringLiteral struct {
	Token Token
	Value string
}

func (s *StringLiteral) expressionNode()      {}
func (s *StringLiteral) TokenLiteral() string { return s.Token.Literal }
func (s *StringLiteral) String() string       { return s.Value }

// NumberLiteral represents a number literal
type NumberLiteral struct {
	Token Token
	Value string
}

func (n *NumberLiteral) expressionNode()      {}
func (n *NumberLiteral) TokenLiteral() string { return n.Token.Literal }
func (n *NumberLiteral) String() string       { return n.Value }

// BooleanLiteral represents a boolean literal
type BooleanLiteral struct {
	Token Token
	Value bool
}

func (b *BooleanLiteral) expressionNode()      {}
func (b *BooleanLiteral) TokenLiteral() string { return b.Token.Literal }
func (b *BooleanLiteral) String() string {
	if b.Value {
		return "true"
	}
	return "false"
}

// NullLiteral represents a NULL literal
type NullLiteral struct {
	Token Token
}

func (n *NullLiteral) expressionNode()      {}
func (n *NullLiteral) TokenLiteral() string { return n.Token.Literal }
func (n *NullLiteral) String() string       { return "NULL" }

// ParameterExpression represents a parameter (? or :name)
type ParameterExpression struct {
	Token Token
	Name  string // empty for ?, or the name for :name
}

func (p *ParameterExpression) expressionNode()      {}
func (p *ParameterExpression) TokenLiteral() string { return p.Token.Literal }
func (p *ParameterExpression) String() string       { return p.Token.Literal }

// InfixExpression represents a binary expression
type InfixExpression struct {
	Token    Token
	Left     Expression
	Operator string
	Right    Expression
}

func (i *InfixExpression) expressionNode()      {}
func (i *InfixExpression) TokenLiteral() string { return i.Token.Literal }
func (i *InfixExpression) String() string       { return "infix expression" }

// PrefixExpression represents a unary expression
type PrefixExpression struct {
	Token    Token
	Operator string
	Right    Expression
}

func (p *PrefixExpression) expressionNode()      {}
func (p *PrefixExpression) TokenLiteral() string { return p.Token.Literal }
func (p *PrefixExpression) String() string       { return "prefix expression" }

// BetweenExpression represents a BETWEEN expression
type BetweenExpression struct {
	Token Token
	Value Expression
	Lower Expression
	Upper Expression
}

func (b *BetweenExpression) expressionNode()      {}
func (b *BetweenExpression) TokenLiteral() string { return b.Token.Literal }
func (b *BetweenExpression) String() string       { return "BETWEEN expression" }

// InExpression represents an IN expression
type InExpression struct {
	Token  Token
	Value  Expression
	Values []Expression
}

func (i *InExpression) expressionNode()      {}
func (i *InExpression) TokenLiteral() string { return i.Token.Literal }
func (i *InExpression) String() string       { return "IN expression" }

// AttributePath represents a path to an attribute (e.g., user.name or user[0])
type AttributePath struct {
	Token    Token
	Base     Expression
	Path     []PathElement
	IsQuoted bool
}

func (a *AttributePath) expressionNode()      {}
func (a *AttributePath) TokenLiteral() string { return a.Token.Literal }
func (a *AttributePath) String() string       { return "attribute path" }

// PathElement represents an element in an attribute path
type PathElement struct {
	Type  string // "field" or "index"
	Value Expression
}

// MapLiteral represents a map/object literal { 'key': value, ... }
type MapLiteral struct {
	Token Token
	Pairs map[Expression]Expression
}

func (m *MapLiteral) expressionNode()      {}
func (m *MapLiteral) TokenLiteral() string { return m.Token.Literal }
func (m *MapLiteral) String() string       { return "map literal" }

// ListLiteral represents a list literal [value1, value2, ...]
type ListLiteral struct {
	Token    Token
	Elements []Expression
}

func (l *ListLiteral) expressionNode()      {}
func (l *ListLiteral) TokenLiteral() string { return l.Token.Literal }
func (l *ListLiteral) String() string       { return "list literal" }

// FunctionCall represents a function call like attribute_exists(attr)
type FunctionCall struct {
	Token     Token
	Function  string
	Arguments []Expression
}

func (f *FunctionCall) expressionNode()      {}
func (f *FunctionCall) TokenLiteral() string { return f.Token.Literal }
func (f *FunctionCall) String() string       { return f.Function + "()" }
