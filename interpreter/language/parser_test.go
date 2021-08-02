package language

import (
	"testing"
)

func TestIdentifierExpression(t *testing.T) {
	input := "foobar"
	l := NewLexer(input)
	p := NewParser(l)

	program := p.ParseDynamoExpression()
	checkParserErrors(t, p)

	stmt, ok := program.Statement.(*ExpressionStatement)
	if !ok {
		t.Fatalf("program.Statement is not ExpressionStatement. got=%T", program.Statement)
	}

	ident, ok := stmt.Expression.(*Identifier)
	if !ok {
		t.Fatalf("exp not *Identifier. got=%T", stmt.Expression)
	}

	if ident.Value != "foobar" {
		t.Errorf("ident.Value not %s. got=%s", "foobar", ident.Value)
	}

	if ident.TokenLiteral() != "foobar" {
		t.Errorf("ident.TokenLiteral not %s. got=%s", "foobar",
			ident.TokenLiteral())
	}
}

func TestParsingPrefixExpressions(t *testing.T) {
	prefixTests := []struct {
		input    string
		operator string
		value    interface{}
	}{
		{"NOT :b", "NOT", ":b"},
	}

	for _, tt := range prefixTests {
		l := NewLexer(tt.input)
		p := NewParser(l)
		program := p.ParseDynamoExpression()
		checkParserErrors(t, p)

		stmt, ok := program.Statement.(*ExpressionStatement)
		if !ok {
			t.Fatalf("program.Statement is not ExpressionStatement. got=%T", program.Statement)
		}

		exp, ok := stmt.Expression.(*PrefixExpression)
		if !ok {
			t.Fatalf("stmt is not PrefixExpression. got=%T", stmt.Expression)
		}

		if exp.Operator != tt.operator {
			t.Fatalf("exp.Operator is not '%s'. got=%s",
				tt.operator, exp.Operator)
		}

		if !testLiteralExpression(t, exp.Right, tt.value) {
			return
		}
	}
}

func TestParsingInfixExpressions(t *testing.T) {
	infixTests := []struct {
		input      string
		leftValue  interface{}
		operator   string
		rightValue interface{}
	}{
		{"#a = :a", "#a", "=", ":a"},
		{"#a <> :a", "#a", "<>", ":a"},
		{"#a > :a", "#a", ">", ":a"},
		{"#a <= :a", "#a", "<=", ":a"},
		{"#a AND :a", "#a", "AND", ":a"},
		{"#a OR :a", "#a", "OR", ":a"},
	}

	for _, tt := range infixTests {
		l := NewLexer(tt.input)
		p := NewParser(l)
		program := p.ParseDynamoExpression()
		checkParserErrors(t, p)

		stmt, ok := program.Statement.(*ExpressionStatement)
		if !ok {
			t.Fatalf("program.Statement is not ExpressionStatement. got=%T", program.Statement)
		}

		if !testInfixExpression(t, stmt.Expression, tt.leftValue,
			tt.operator, tt.rightValue) {
			return
		}
	}
}

func TestParsingBetweenExpression(t *testing.T) {
	betweenTests := []struct {
		input     string
		leftValue interface{}
		min       interface{}
		max       interface{}
	}{
		{"#b BETWEEN :a AND :c", "#b", ":a", ":c"},
	}

	for _, tt := range betweenTests {
		l := NewLexer(tt.input)
		p := NewParser(l)
		program := p.ParseDynamoExpression()
		checkParserErrors(t, p)

		stmt, ok := program.Statement.(*ExpressionStatement)
		if !ok {
			t.Fatalf("program.Statement is not ExpressionStatement. got=%T", program.Statement)
		}

		opExp, ok := stmt.Expression.(*BetweenExpression)
		if !ok {
			t.Fatalf("exp is not BetweenExpression. got=%T(%s)", stmt.Expression, stmt.Expression)
		}

		if !testBetweenExpression(t, opExp, tt.leftValue, tt.min, tt.max) {
			return
		}
	}
}

func testBetweenExpression(t *testing.T, opExp *BetweenExpression, left, min, max interface{}) bool {
	if !testLiteralExpression(t, opExp.Left, left) {
		return true
	}

	if !testLiteralExpression(t, opExp.Range[0], min) {
		return true
	}

	if !testLiteralExpression(t, opExp.Range[1], max) {
		return true
	}

	return false
}

func TestOperatorPrecedenceParsing(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			":a > :b = #a < #b",
			"((:a > :b) = (#a < #b))",
		},
		{
			":a < :b <> #a > #b",
			"((:a < :b) <> (#a > #b))",
		},
		{
			"NOT(:a = #a)",
			"(NOT(:a = #a))",
		},
		{
			"a OR b AND c",
			"(a OR (b AND c))",
		},
		{
			":a AND :x < :y",
			"(:a AND (:x < :y))",
		},
		{
			"attribute_exists(:b) AND begins_with(:b, #s) OR #c",
			"((attribute_exists(:b) AND begins_with(:b, #s)) OR #c)",
		},
		{
			":a = size(:s)",
			"(:a = size(:s))",
		},
		{
			":a > size(:s)",
			"(:a > size(:s))",
		},
		{
			":a > size(:s) OR size(:c) = :a",
			"((:a > size(:s)) OR (size(:c) = :a))",
		},
		{
			"NOT :a > size(:s) OR size(:c) = :a",
			"((NOT(:a > size(:s))) OR (size(:c) = :a))",
		},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		p := NewParser(l)
		program := p.ParseDynamoExpression()
		checkParserErrors(t, p)

		actual := program.String()
		if actual != tt.expected {
			t.Errorf("expected=%q, got=%q", tt.expected, actual)
		}
	}
}

func TestCallExpressionParsing(t *testing.T) {
	input := "begins_with(:a, #s)"
	l := NewLexer(input)
	p := NewParser(l)
	program := p.ParseDynamoExpression()

	checkParserErrors(t, p)

	stmt, ok := program.Statement.(*ExpressionStatement)
	if !ok {
		t.Fatalf("stmt is not ast.ExpressionStatement. got=%T",
			program.Statement)
	}

	exp, ok := stmt.Expression.(*CallExpression)
	if !ok {
		t.Fatalf("stmt.Expression is not ast.CallExpression. got=%T",
			stmt.Expression)
	}

	if !testIdentifier(t, exp.Function, "begins_with") {
		return
	}

	if len(exp.Arguments) != 2 {
		t.Fatalf("wrong length of arguments. got=%d", len(exp.Arguments))
	}

	testLiteralExpression(t, exp.Arguments[0], ":a")
	testLiteralExpression(t, exp.Arguments[1], "#s")
}

func TestParsingErrors(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			"=a",
			"no prefix parse function for = found",
		},
		{
			"size(a",
			"expected next token to be ), got EOF instead",
		},
		{
			"b BETWEEN a c",
			"expected next token to be AND, got IDENT instead",
		},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		p := NewParser(l)
		p.ParseDynamoExpression()

		if len(p.errors) == 0 {
			t.Errorf("no errors found")
			return
		}

		actual := p.errors[0]
		if actual != tt.expected {
			t.Errorf("expected=%q, got=%q", tt.expected, actual)
		}
	}
}

func testIdentifier(t *testing.T, exp Expression, value string) bool {
	ident, ok := exp.(*Identifier)
	if !ok {
		t.Errorf("exp not *Identifier. got=%T", exp)
		return false
	}

	if ident.Value != value {
		t.Errorf("ident.Value not %s. got=%s", value, ident.Value)
		return false
	}

	if ident.TokenLiteral() != value {
		t.Errorf("ident.TokenLiteral not %s. got=%s", value,
			ident.TokenLiteral())
		return false
	}

	return true
}

func checkParserErrors(t *testing.T, p *Parser) {
	errors := p.Errors()
	if len(errors) == 0 {
		return
	}

	t.Errorf("parser has %d errors", len(errors))

	for _, msg := range errors {
		t.Errorf("parser error: %q", msg)
	}

	t.FailNow()
}

func testLiteralExpression(t *testing.T, exp Expression, expected interface{}) bool {
	v, ok := expected.(string)
	if ok {
		return testIdentifier(t, exp, v)
	}

	t.Errorf("type of exp not handled. got=%T", exp)

	return false
}

func testInfixExpression(t *testing.T, exp Expression, left interface{}, operator string, right interface{}) bool {
	opExp, ok := exp.(*InfixExpression)
	if !ok {
		t.Errorf("exp is not OperatorExpression. got=%T(%s)", exp, exp)
		return false
	}

	if !testLiteralExpression(t, opExp.Left, left) {
		return false
	}

	if opExp.Operator != operator {
		t.Errorf("exp.Operator is not '%s'. got=%q", operator, opExp.Operator)
		return false
	}

	if !testLiteralExpression(t, opExp.Right, right) {
		return false
	}

	return true
}

func BenchmarkParser(b *testing.B) {
	for n := 0; n < b.N; n++ {
		l := NewLexer(`attribute_exists(:b) AND begins_with(:b, #s) OR #c`)
		p := NewParser(l)
		p.ParseDynamoExpression()

		if len(p.errors) != 0 {
			b.Fatal("errors found")
		}
	}
}
