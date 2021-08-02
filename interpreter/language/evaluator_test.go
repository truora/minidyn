package language

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestEval(t *testing.T) {
	tests := []struct {
		input    string
		expected Object
	}{
		{":a", TRUE},
		{":b", FALSE},
		{"NOT :a", FALSE},
		{"NOT :b", TRUE},
		{"NOT NOT :a", TRUE},
		{":x = :y", FALSE},
		{":a = :b", FALSE},
		{":a AND :b", FALSE},
		{":a OR :b", TRUE},
		// Numbers
		{":s = :b", FALSE},
		{":x < :y", TRUE},
		{":x <= :y", TRUE},
		{":x < :nullField", FALSE},
		{":nullField < :x", FALSE},
		{":x > :y", FALSE},
		{":x >= :z", FALSE},
		{":a <> :b", TRUE},
		{":x <> :y", TRUE},
		{":x = :nullField", FALSE},
		{":nullField = :x", FALSE},
		// Strings
		{":s = :s", TRUE},
		{":s <> :b", TRUE},
		{":s <> :s", FALSE},
		{":txtA < :txtB", TRUE},
		{":txtA <= :txtB", TRUE},
		{":txtB = :nullField", FALSE},
		{":txtA > :txtB", FALSE},
		{":txtA >= :txtC", FALSE},
		{":txtA <> :txtB", TRUE},
		{":txtA = :txtA", TRUE},
		{":txtB = :nullField", FALSE},
		{":nullField = :txtB", FALSE},
		// Binaries
		{":binA < :binB", TRUE},
		{":binA <= :binB", TRUE},
		{":binB = :nullField", FALSE},
		{":binA > :binB", FALSE},
		{":binA >= :binC", FALSE},
		{":binA <> :binB", TRUE},
		{":binA = :binA", TRUE},
		{":binA = :nullField", FALSE},
		{":nullField = :binA", FALSE},
		// NULL
		{":otherNil = :nil", TRUE},
		{":otherNil <> :nil", FALSE},
		{":notFound = :nil", FALSE},
		{":notFound <> :nil", TRUE},
		// BETWEEN
		{":y BETWEEN :x AND :z", TRUE},
		{":txtB BETWEEN :txtA AND :txtC", TRUE},
		{":nullField = :txtB", FALSE},
		{":binB BETWEEN :binA AND :binC", TRUE},
		// Map
		{":hashA = :hashB", FALSE},
		{":hashA = :hashA", TRUE},
		// List
		{":listA = :listB", FALSE},
		{":listA = :listA", TRUE},
		// StringSet
		{":strSetA = :strSetB", FALSE},
		{":strSetA = :strSetA", TRUE},
		// BinarySet
		{":binSetA = :binSetB", FALSE},
		{":binSetA = :binSetA", TRUE},
		// NumberSet
		{":numSetA = :numSetB", FALSE},
		{":numSetA = :numSetA", TRUE},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":a":        &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		":b":        &dynamodb.AttributeValue{BOOL: aws.Bool(false)},
		":s":        &dynamodb.AttributeValue{S: aws.String("HELLO WORLD!")},
		":x":        &dynamodb.AttributeValue{N: aws.String("24")},
		":y":        &dynamodb.AttributeValue{N: aws.String("25")},
		":z":        &dynamodb.AttributeValue{N: aws.String("26")},
		":txtA":     &dynamodb.AttributeValue{S: aws.String("a")},
		":txtB":     &dynamodb.AttributeValue{S: aws.String("b")},
		":txtC":     &dynamodb.AttributeValue{S: aws.String("c")},
		":binA":     &dynamodb.AttributeValue{B: []byte("a")},
		":binB":     &dynamodb.AttributeValue{B: []byte("b")},
		":binC":     &dynamodb.AttributeValue{B: []byte("c")},
		":nil":      &dynamodb.AttributeValue{NULL: aws.Bool(true)},
		":otherNil": &dynamodb.AttributeValue{NULL: aws.Bool(true)},
		":hashA": &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				":a": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
			},
		},
		":hashB": &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				":b": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
			},
		},
		":listA": &dynamodb.AttributeValue{
			L: []*dynamodb.AttributeValue{
				{S: aws.String("a")},
				{S: aws.String("b")},
				{S: aws.String("c")},
			},
		},
		":listB": &dynamodb.AttributeValue{
			L: []*dynamodb.AttributeValue{
				{S: aws.String("x")},
				{S: aws.String("y")},
				{S: aws.String("z")},
			},
		},
		":strSetA": &dynamodb.AttributeValue{
			SS: []*string{aws.String("a"), aws.String("a"), aws.String("b")},
		},
		":strSetB": &dynamodb.AttributeValue{
			SS: []*string{aws.String("x"), aws.String("x"), aws.String("y")},
		},
		":binSetA": &dynamodb.AttributeValue{
			BS: [][]byte{[]byte("a"), []byte("a"), []byte("b")},
		},
		":binSetB": &dynamodb.AttributeValue{
			BS: [][]byte{[]byte("x"), []byte("x"), []byte("y")},
		},
		":numSetA": &dynamodb.AttributeValue{
			NS: []*string{aws.String("1"), aws.String("2"), aws.String("4")},
		},
		":numSetB": &dynamodb.AttributeValue{
			NS: []*string{aws.String("10"), aws.String("10"), aws.String("11")},
		},
	})
	if err != nil {
		t.Fatalf("error adding attributes %#v", err)
	}

	for _, tt := range tests {
		evaluated := testEval(t, tt.input, env)
		if evaluated != tt.expected {
			t.Errorf("result has wrong value for %q. got=%v, want=%v", tt.input, evaluated, tt.expected)
		}
	}
}

func TestEvalFunctions(t *testing.T) {
	tests := []struct {
		input    string
		expected Object
	}{
		{"size(:s)", &Number{Value: 12}},
		{"size(:bin)", &Number{Value: 3}},
		{"attribute_exists(:n)", FALSE},
		{"attribute_not_exists(:n)", TRUE},
		{"begins_with(:s, :prefix)", TRUE},
		{"contains(:s, :subtext)", TRUE},
		{"contains(:list, :element)", TRUE},
		{"contains(:strSet, :element)", TRUE},
		{"contains(:binSet, :bin)", TRUE},
		{"contains(:numSet, :num)", TRUE},
		{"attribute_type(:s, :type)", TRUE},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":s":       &dynamodb.AttributeValue{S: aws.String("HELLO WORLD!")},
		":type":    &dynamodb.AttributeValue{S: aws.String("S")},
		":bin":     &dynamodb.AttributeValue{B: []byte{10, 10, 10}},
		":prefix":  &dynamodb.AttributeValue{S: aws.String("HELLO")},
		":subtext": &dynamodb.AttributeValue{S: aws.String("ELL")},
		":element": &dynamodb.AttributeValue{S: aws.String("a")},
		":num":     &dynamodb.AttributeValue{N: aws.String("1")},
		":list": &dynamodb.AttributeValue{
			L: []*dynamodb.AttributeValue{
				{S: aws.String("a")},
				{S: aws.String("b")},
				{S: aws.String("c")},
			},
		},
		":strSet": &dynamodb.AttributeValue{
			SS: []*string{aws.String("a"), aws.String("a"), aws.String("b")},
		},
		":binSet": &dynamodb.AttributeValue{
			BS: [][]byte{[]byte{10, 10, 10}},
		},
		":numSet": &dynamodb.AttributeValue{
			NS: []*string{aws.String("1"), aws.String("2"), aws.String("4")},
		},
	})
	if err != nil {
		panic(err)
	}

	for _, tt := range tests {
		evaluated := testEval(t, tt.input, env)
		if evaluated.Inspect() != tt.expected.Inspect() {
			t.Errorf("result has wrong value in %q. got=%v, want=%v", tt.input, evaluated, tt.expected)
		}
	}
}

func testEval(t *testing.T, input string, env *Environment) Object {
	l := NewLexer(input)
	p := NewParser(l)
	program := p.ParseDynamoExpression()

	if len(p.errors) != 0 {
		t.Fatalf("parsing %q failed: %s", input, strings.Join(p.errors, ";\n"))
	}

	return Eval(program, env)
}

func TestErrorHandling(t *testing.T) {
	tests := []struct {
		input           string
		expectedMessage string
	}{
		{
			":x > :a",
			"type mismatch: N > BOOL",
		},
		{
			":a < :x",
			"type mismatch: BOOL < N",
		},
		{
			":a < :b",
			"unknown operator: BOOL < BOOL",
		},
		{
			":x AND :y",
			"unknown operator: N AND N",
		},
		{
			"NOT :x",
			"unknown operator: NOT N",
		},
		{
			"size(:a)",
			"type not supported: size BOOL",
		},
		{
			"undefined(:a)",
			"function not found: undefined",
		},
		{
			"NOT :nil",
			"unknown operator: NOT NULL",
		},
		{
			"NOT :notfound",
			"unknown operator: NOT NULL",
		},
		{
			"size(:notfound) AND :a",
			"type not supported: size NULL",
		},
		{
			":a AND size(:notfound)",
			"type not supported: size NULL",
		},
		{
			"NOT size(:notfound)",
			"type not supported: size NULL",
		},
		{
			":y BETWEEN :a AND :b",
			"unexpected type: \":a\" should be a comparable type(N,S,B) got \"BOOL\"",
		},
		{
			":y BETWEEN :x AND :str",
			"mismatch type: BETWEEN operands must have the same type",
		},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":a":   &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		":b":   &dynamodb.AttributeValue{BOOL: aws.Bool(false)},
		":x":   &dynamodb.AttributeValue{N: aws.String("24")},
		":y":   &dynamodb.AttributeValue{N: aws.String("25")},
		":z":   &dynamodb.AttributeValue{N: aws.String("26")},
		":str": &dynamodb.AttributeValue{S: aws.String("TEXT")},
		":nil": &dynamodb.AttributeValue{NULL: aws.Bool(true)},
	})
	if err != nil {
		panic(err)
	}

	for i, tt := range tests {
		evaluated := testEval(t, tt.input, env)

		errObj, ok := evaluated.(*Error)
		if !ok {
			t.Errorf("(%d) no error object returned for %s. got=%T(%+v)", i, tt.input, evaluated, evaluated)
			continue
		}

		if errObj.Message != tt.expectedMessage {
			t.Errorf("wrong error message for %s. expected=%q, got=%q", tt.input, tt.expectedMessage, errObj.Message)
		}
	}
}

func TestIsError(t *testing.T) {
	b := isError(nil)
	if b {
		t.Fatal("expected to be false")
	}

	err := newError("testing")

	b = isError(err)
	if !b {
		t.Fatal("expected to be true")
	}
}

func TestIsNumber(t *testing.T) {
	if isNumber(nil) {
		t.Fatal("expected to be false")
	}

	num := Number{Value: 10}
	if !isNumber(&num) {
		t.Fatal("expected to be true")
	}
}

func TestIsString(t *testing.T) {
	if isString(nil) {
		t.Fatal("expected to be false")
	}

	str := String{Value: "txt"}
	if !isString(&str) {
		t.Fatal("expected to be true")
	}
}

func BenchmarkEval(b *testing.B) {
	input := ":a OR :b"

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":a": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		":b": &dynamodb.AttributeValue{BOOL: aws.Bool(false)},
	})
	if err != nil {
		b.Fatal(err)
	}

	for n := 0; n < b.N; n++ {
		l := NewLexer(input)
		p := NewParser(l)
		program := p.ParseDynamoExpression()

		if len(p.errors) != 0 {
			b.Fatalf("parsing %s failed: %v", input, p.errors)
		}

		evaluated := Eval(program, env)
		if evaluated != TRUE {
			b.Fatal("expected to be true")
		}
	}
}
