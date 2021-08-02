package language

import (
	"testing"
)

type testCase struct {
	expectedType    TokenType
	expectedLiteral string
}

func TestNextToken(t *testing.T) {
	table := map[string][]testCase{
		`v1`: []testCase{
			{IDENT, "v1"},
		},
		`a = b AND c`: []testCase{
			{IDENT, "a"},
			{EQ, "="},
			{IDENT, "b"},
			{AND, "AND"},
			{IDENT, "c"},
		},
		`a <> b`: []testCase{
			{IDENT, "a"},
			{NotEQ, "<>"},
			{IDENT, "b"},
		},
		`attribute_exists(:a)`: []testCase{
			{IDENT, "attribute_exists"},
			{LPAREN, "("},
			{IDENT, ":a"},
			{RPAREN, ")"},
		},
		`begins_with(:a, #s)`: []testCase{
			{IDENT, "begins_with"},
			{LPAREN, "("},
			{IDENT, ":a"},
			{COMMA, ","},
			{IDENT, "#s"},
			{RPAREN, ")"},
		},
		`contains(:a, #s)`: []testCase{
			{IDENT, "contains"},
			{LPAREN, "("},
			{IDENT, ":a"},
			{COMMA, ","},
			{IDENT, "#s"},
			{RPAREN, ")"},
		},
		`a <= b AND b >= c`: []testCase{
			{IDENT, "a"},
			{LTE, "<="},
			{IDENT, "b"},
			{AND, "AND"},
			{IDENT, "b"},
			{GTE, ">="},
			{IDENT, "c"},
		},
		`a IN (b, c)`: []testCase{
			{IDENT, "a"},
			{IN, "IN"},
			{LPAREN, "("},
			{IDENT, "b"},
			{COMMA, ","},
			{IDENT, "c"},
			{RPAREN, ")"},
		},
		`NOT a`: []testCase{
			{NOT, "NOT"},
			{IDENT, "a"},
		},
		`a >`: []testCase{
			{IDENT, "a"},
			{GT, ">"},
		},
		`b BETWEEN a AND c`: []testCase{
			{IDENT, "b"},
			{BETWEEN, "BETWEEN"},
			{IDENT, "a"},
			{AND, "AND"},
			{IDENT, "c"},
		},
	}

	for input, tests := range table {
		l := NewLexer(input)

		for i, tt := range tests {
			tok := l.NextToken()

			if tok.Type != tt.expectedType {
				t.Fatalf("for %s: tests[%d] - token type wrong. expected=%q, got=%q",
					input, i, tt.expectedType, tok.Type)
			}

			if tok.Literal != tt.expectedLiteral {
				t.Fatalf("for %s: tests[%d] - literal wrong. expected=%q, got=%q",
					input, i, tt.expectedLiteral, tok.Literal)
			}
		}
	}
}

func BenchmarkLexer(b *testing.B) {
	expected := []testCase{
		{IDENT, "a"},
		{LTE, "<="},
		{IDENT, "b"},
		{AND, "AND"},
		{IDENT, "b"},
		{GTE, ">="},
		{IDENT, "c"},
	}

	for n := 0; n < b.N; n++ {
		l := NewLexer(`a <= b AND b >= c`)

		for i, tt := range expected {
			tok := l.NextToken()

			if tok.Type != tt.expectedType {
				b.Fatalf("(%d) - token type wrong. expected=%q, got=%q", i, tt.expectedType, tok.Type)
			}

			if tok.Literal != tt.expectedLiteral {
				b.Fatalf("(%d) - token type wrong. expected=%q, got=%q", i, tt.expectedLiteral, tok.Literal)
			}
		}
	}
}
