package language

import (
	"testing"
)

func TestParseProjectionExpression(t *testing.T) {
	t.Parallel()

	l := NewLexer("A.B[0], C, #name")
	p := NewParser(l)
	exprs := p.ParseProjectionExpression()
	checkParserErrors(t, p)

	if len(exprs) != 3 {
		t.Fatalf("expected 3 paths, got %d", len(exprs))
	}

	if exprs[0].String() != "((A[B])[0])" {
		t.Errorf("first path: got %q", exprs[0].String())
	}

	ident1, ok := exprs[1].(*Identifier)
	if !ok || ident1.Value != "C" {
		t.Errorf("second path: got %#v", exprs[1])
	}

	ident2, ok := exprs[2].(*Identifier)
	if !ok || ident2.Value != "#name" {
		t.Errorf("third path: got %#v", exprs[2])
	}
}

func TestParseProjectionExpression_empty(t *testing.T) {
	t.Parallel()

	l := NewLexer("")
	p := NewParser(l)
	exprs := p.ParseProjectionExpression()
	checkParserErrors(t, p)

	if len(exprs) != 0 {
		t.Fatalf("expected 0 paths, got %d", len(exprs))
	}
}

func TestParseProjectionExpression_singlePath(t *testing.T) {
	t.Parallel()

	l := NewLexer("foo.bar")
	p := NewParser(l)
	exprs := p.ParseProjectionExpression()
	checkParserErrors(t, p)

	if len(exprs) != 1 {
		t.Fatalf("expected 1 path, got %d", len(exprs))
	}
}
