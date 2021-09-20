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
		{":hashA.:a", TRUE},
		{":nestedMap.lvl1.lvl2", TRUE},
		// List
		{":listA = :listB", FALSE},
		{":listA = :listA", TRUE},
		{":listA[0] = :listA[0]", TRUE},
		{":listB[0] = :listA[0]", FALSE},
		{":listB[:listIndex] = :listA[:listIndex]", FALSE},
		{":matrix[0][0] = :listA[0]", TRUE},
		{":matrix[0][1] = :txtB", TRUE},
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
		":matrix": &dynamodb.AttributeValue{
			L: []*dynamodb.AttributeValue{
				&dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
					&dynamodb.AttributeValue{S: aws.String("a")},
					&dynamodb.AttributeValue{S: aws.String("b")},
				}},
				&dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
					&dynamodb.AttributeValue{S: aws.String("c")},
				}},
			},
		},
		":listIndex": &dynamodb.AttributeValue{N: aws.String("0")},
		":nestedMap": &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"lvl1": &dynamodb.AttributeValue{
					M: map[string]*dynamodb.AttributeValue{
						"lvl2": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
					},
				},
			},
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
		{"size(:s) = :sSize", TRUE},
		{"size(:bin) = :binSize", TRUE},
		{"attribute_exists(:n)", FALSE},
		{"attribute_exists(h.notFound)", FALSE},
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
		":sSize":   &dynamodb.AttributeValue{N: aws.String("12")},
		":type":    &dynamodb.AttributeValue{S: aws.String("S")},
		":bin":     &dynamodb.AttributeValue{B: []byte{10, 10, 10}},
		":binSize": &dynamodb.AttributeValue{N: aws.String("3")},
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

func TestEvalUpdate(t *testing.T) {
	tests := []struct {
		input    string
		envField string
		expected Object
	}{
		{"SET :x = :val", ":x", &String{Value: "text"}},
		{"SET :w = :val", ":w", &String{Value: "text"}},
		{"SET :two = :one + :one", ":two", &Number{Value: 2}},
		{"SET :zero = :one - :one", ":zero", &Number{Value: 0}},
		{"SET :zero = :one - :one", ":zero", &Number{Value: 0}},
		{"SET :newTwo = if_not_exists(not_found, :one) + :one", ":newTwo", &Number{Value: 2}},
		{"SET :three = if_not_exists(:two, :one) + :one", ":three", &Number{Value: 3}},
		{"SET :list[1] = :one", ":list", &List{Value: []Object{&Number{Value: 0}, &Number{Value: 1}}}},
		{"SET :list[0] = :one", ":list", &List{Value: []Object{&Number{Value: 1}, &Number{Value: 1}}}},
		{
			"SET :matrix[0][0] = :one",
			":matrix", &List{Value: []Object{&List{Value: []Object{&Number{Value: 1}}}}},
		},
		{
			"SET :hash.a = :one",
			":hash", &Map{Value: map[string]Object{"a": &Number{Value: 1}}},
		},
		{
			"SET :hash.:mapField = :one",
			":hash", &Map{Value: map[string]Object{"a": &Number{Value: 1}, "key": &Number{Value: 1}}},
		},
		{
			"SET :four = if_not_exists(:hash.not_found, :one) + :three",
			":four", &Number{Value: 4},
		},
		{
			"SET :nestedMap.lvl1.lvl2 = :nestedMap.lvl1.lvl2 + :one",
			":nestedMap", &Map{Value: map[string]Object{"lvl1": &Map{Value: map[string]Object{"lvl2": &Number{Value: 1}}}}},
		},
		{
			"SET :nestedMap.#pos = #pos + :one",
			":nestedMap", &Map{Value: map[string]Object{"lvl1": &Map{Value: map[string]Object{"lvl2": &Number{Value: 2}}}}},
		},
		{
			"SET :nestedMap.#secondLevel = #pos + :one",
			":nestedMap", &Map{Value: map[string]Object{"lvl1": &Map{Value: map[string]Object{"lvl2": &Number{Value: 3}}}}},
		},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":x":    &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		":val":  &dynamodb.AttributeValue{S: aws.String("text")},
		":one":  &dynamodb.AttributeValue{N: aws.String("1")},
		":list": &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{&dynamodb.AttributeValue{N: aws.String("0")}}},
		":hash": &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"a": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
			},
		},
		":mapField": &dynamodb.AttributeValue{S: aws.String("key")},
		":matrix": &dynamodb.AttributeValue{
			L: []*dynamodb.AttributeValue{
				&dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{&dynamodb.AttributeValue{N: aws.String("0")}}},
			},
		},
		":nestedMap": &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"lvl1": &dynamodb.AttributeValue{
					M: map[string]*dynamodb.AttributeValue{
						"lvl2": &dynamodb.AttributeValue{N: aws.String("0")},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("error adding attributes %#v", err)
	}

	env.Aliases = map[string]string{
		"#pos":         ":nestedMap.lvl1.lvl2",
		"#secondLevel": "lvl1.lvl2",
	}

	for _, tt := range tests {
		result := testEvalUpdate(t, tt.input, env)
		if isError(result) {
			t.Fatalf("error evaluating update %q, env=%s, %s", tt.input, env.String(), result.Inspect())
		}

		result = env.Get(tt.envField)

		if result.ToDynamoDB() == tt.expected.ToDynamoDB() {
			t.Errorf("result has wrong value for %q. got=%v, want=%v", tt.envField, result.Inspect(), tt.expected.Inspect())
		}
	}
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
			"invalid function name; function: undefined",
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
		{
			":y.:str",
			"index operator not supported for \"N\"",
		},
		{
			"ROLE BETWEEN :x AND :str",
			"reserved word ROLE found in expression",
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

func TestEvalReservedKeywords(t *testing.T) {
	tests := []struct {
		input           string
		expectedMessage string
	}{
		{
			"size = :x",
			"reserved word SIZE found in expression",
		},
		{
			"hash = :x",
			"reserved word HASH found in expression",
		},
		{
			":obj.size = :x",
			"",
		},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":y":   &dynamodb.AttributeValue{N: aws.String("25")},
		":str": &dynamodb.AttributeValue{S: aws.String("TEXT")},
		":obj": &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"a": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
			},
		},
	})
	if err != nil {
		panic(err)
	}

	for i, tt := range tests {
		evaluated := testEval(t, tt.input, env)

		if tt.expectedMessage == "" {
			errObj, ok := evaluated.(*Error)
			if ok {
				t.Errorf("(%d) error not expected for %q. got=%s", i, tt.input, errObj.Message)
			}

			continue
		}

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

func TestUpdateEvalSyntaxError(t *testing.T) {
	tests := []struct {
		input           string
		expectedMessage string
	}{
		{
			":x > :a",
			"unknown operator: >",
		},
		{
			"SET",
			"SET expression must have at least one action",
		},
		{
			"SET x = size(:val)",
			"the function is not allowed in an update expression; function: size",
		},
		{
			"SET :one + :one = :val",
			"invalid assignation to: ((:one + :one) = :val)",
		},
		{
			"SET x = :val + :val",
			"invalid operation: S + S",
		},
		{
			"SET x = :val - :val",
			"invalid operation: S - S",
		},
		{
			"SET x = :val - :one + :one",
			"invalid operation: S - N",
		},
		{
			"SET x = :one + (:one - :val)",
			"invalid operation: N - S",
		},
		{
			"SET h.bar = :one",
			"index assignation for \"NULL\" type is not supported",
		},
		{
			"SET notFound.bar = :one",
			"index assignation for \"NULL\" type is not supported",
		},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":x":   &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
		":val": &dynamodb.AttributeValue{S: aws.String("text")},
		":one": &dynamodb.AttributeValue{N: aws.String("1")},
		":h": &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"a": &dynamodb.AttributeValue{BOOL: aws.Bool(true)},
			},
		},
	})
	if err != nil {
		panic(err)
	}

	for i, tt := range tests {
		evaluated := testEvalUpdate(t, tt.input, env)

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
		conditional := p.ParseConditionalExpression()

		if len(p.errors) != 0 {
			b.Fatalf("parsing %s failed: %v", input, p.errors)
		}

		evaluated := Eval(conditional, env)
		if evaluated != TRUE {
			b.Fatal("expected to be true")
		}
	}
}

func testEval(t *testing.T, input string, env *Environment) Object {
	l := NewLexer(input)
	p := NewParser(l)
	conditional := p.ParseConditionalExpression()

	if len(p.errors) != 0 {
		t.Fatalf("parsing %q failed: %s", input, strings.Join(p.errors, ";\n"))
	}

	return Eval(conditional, env)
}

func testEvalUpdate(t *testing.T, input string, env *Environment) Object {
	l := NewLexer(input)
	p := NewParser(l)
	update := p.ParseUpdateExpression()

	if len(p.errors) != 0 {
		t.Fatalf("parsing %q failed: %s", input, strings.Join(p.errors, ";\n"))
	}

	return EvalUpdate(update, env)
}
