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
		// Booleans
		{":a = :a", TRUE},
		{"NOT :a = :a", FALSE},
		{"NOT NOT :a = :a", TRUE},
		{":a = :b", FALSE},
		{":a = :a AND :b = :b", TRUE},
		{":a = :a AND :a = :b", FALSE},
		{":a = :a OR :a = :b", TRUE},
		{":x = :y", FALSE},
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
		{"#alias_field_name <> :txtA", TRUE},
		{"#alias_field_name = :txtA", FALSE},
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

	env.Aliases = map[string]string{
		"#alias_field_name": "field_name",
	}

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":a":        {BOOL: aws.Bool(true)},
		":b":        {BOOL: aws.Bool(false)},
		":s":        {S: aws.String("HELLO WORLD!")},
		":x":        {N: aws.String("24")},
		":y":        {N: aws.String("25")},
		":z":        {N: aws.String("26")},
		":txtA":     {S: aws.String("a")},
		":txtB":     {S: aws.String("b")},
		":txtC":     {S: aws.String("c")},
		":binA":     {B: []byte("a")},
		":binB":     {B: []byte("b")},
		":binC":     {B: []byte("c")},
		":nil":      {NULL: aws.Bool(true)},
		":otherNil": {NULL: aws.Bool(true)},
		":hashA": {
			M: map[string]*dynamodb.AttributeValue{
				":a": {BOOL: aws.Bool(true)},
			},
		},
		":hashB": {
			M: map[string]*dynamodb.AttributeValue{
				":b": {BOOL: aws.Bool(true)},
			},
		},
		":listA": {
			L: []*dynamodb.AttributeValue{
				{S: aws.String("a")},
				{S: aws.String("b")},
				{S: aws.String("c")},
			},
		},
		":listB": {
			L: []*dynamodb.AttributeValue{
				{S: aws.String("x")},
				{S: aws.String("y")},
				{S: aws.String("z")},
			},
		},
		":strSetA": {
			SS: []*string{aws.String("a"), aws.String("a"), aws.String("b")},
		},
		":strSetB": {
			SS: []*string{aws.String("x"), aws.String("x"), aws.String("y")},
		},
		":binSetA": {
			BS: [][]byte{[]byte("a"), []byte("a"), []byte("b")},
		},
		":binSetB": {
			BS: [][]byte{[]byte("x"), []byte("x"), []byte("y")},
		},
		":numSetA": {
			NS: []*string{aws.String("1"), aws.String("2"), aws.String("4")},
		},
		":numSetB": {
			NS: []*string{aws.String("10"), aws.String("10"), aws.String("11")},
		},
		":matrix": {
			L: []*dynamodb.AttributeValue{
				{L: []*dynamodb.AttributeValue{
					{S: aws.String("a")},
					{S: aws.String("b")},
				}},
				{L: []*dynamodb.AttributeValue{
					{S: aws.String("c")},
				}},
			},
		},
		":listIndex": {N: aws.String("0")},
		":nestedMap": {
			M: map[string]*dynamodb.AttributeValue{
				"lvl1": {
					M: map[string]*dynamodb.AttributeValue{
						"lvl2": {BOOL: aws.Bool(true)},
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
		":s":       {S: aws.String("HELLO WORLD!")},
		":sSize":   {N: aws.String("12")},
		":type":    {S: aws.String("S")},
		":bin":     {B: []byte{10, 10, 10}},
		":binSize": {N: aws.String("3")},
		":prefix":  {S: aws.String("HELLO")},
		":subtext": {S: aws.String("ELL")},
		":element": {S: aws.String("a")},
		":num":     {N: aws.String("1")},
		":list": {
			L: []*dynamodb.AttributeValue{
				{S: aws.String("a")},
				{S: aws.String("b")},
				{S: aws.String("c")},
			},
		},
		":strSet": {
			SS: []*string{aws.String("a"), aws.String("a"), aws.String("b")},
		},
		":binSet": {
			BS: [][]byte{{10, 10, 10}},
		},
		":numSet": {
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

func startEvalUpdateEnv(t *testing.T) *Environment {
	env := NewEnvironment()

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":x":    {BOOL: aws.Bool(true)},
		":val":  {S: aws.String("text")},
		":one":  {N: aws.String("1")},
		":bin":  {B: []byte("c")},
		":list": {L: []*dynamodb.AttributeValue{{N: aws.String("0")}}},
		":hash": {
			M: map[string]*dynamodb.AttributeValue{
				"a": {BOOL: aws.Bool(true)},
			},
		},
		":mapField": {S: aws.String("key")},
		":matrix": {
			L: []*dynamodb.AttributeValue{
				{L: []*dynamodb.AttributeValue{{N: aws.String("0")}}},
			},
		},
		":nestedMap": {
			M: map[string]*dynamodb.AttributeValue{
				"lvl1": {
					M: map[string]*dynamodb.AttributeValue{
						"lvl2": {N: aws.String("0")},
					},
				},
			},
		},

		":strSet": {
			SS: []*string{aws.String("a"), aws.String("a"), aws.String("b")},
		},
		":a": {
			S: aws.String("a"),
		},
		":binSet": {
			BS: [][]byte{[]byte("a"), []byte("b")},
		},
		":binA": {
			B: []byte("a"),
		},
		":numSet": {
			NS: []*string{aws.String("2"), aws.String("4")},
		},
		":two": {
			N: aws.String("2"),
		},
	})
	if err != nil {
		t.Fatalf("error adding attributes %#v", err)
	}

	env.Aliases = map[string]string{
		"#pos":         ":nestedMap.lvl1.lvl2",
		"#secondLevel": "lvl1.lvl2",
		"#invalid":     ":one.:one",
	}

	return env
}

func TestEvalUpdateError(t *testing.T) {
	tests := []struct {
		input         string
		envField      string
		expectedError Object
	}{
		{
			"SET #invalid = 1",
			"#invalid",
			newError("index operator not supported for \"N\""),
		},
	}

	env := startEvalUpdateEnv(t)

	for _, tt := range tests {
		result := env.Get(tt.envField)

		if !isError(result) {
			t.Fatalf("expected error missing evaluating update %q, env=%s, %s", tt.input, env.String(), result.Inspect())
		}

		if result.Inspect() != tt.expectedError.Inspect() {
			t.Errorf("unexpected error for %q in %q. got=%v, want=%v", tt.envField, tt.input, result.Inspect(), tt.expectedError.Inspect())
		}
	}
}

func TestEvalSetUpdate(t *testing.T) {
	tests := []struct {
		input    string
		envField string
		expected Object
		keepEnv  bool
	}{
		{"SET :x = :val", ":x", &String{Value: "text"}, true},
		{"SET :w = :val", ":w", &String{Value: "text"}, true},
		{"SET :two = :one + :one", ":two", &Number{Value: 2}, true},
		{"SET :zero = :one - :one", ":zero", &Number{Value: 0}, true},
		{"SET :zero = :one - :one", ":zero", &Number{Value: 0}, true},
		{"SET :newTwo = if_not_exists(not_found, :one) + :one", ":newTwo", &Number{Value: 2}, true},
		{"SET :three = if_not_exists(:two, :one) + :one", ":three", &Number{Value: 3}, true},
		{"SET :list[1] = :one", ":list", &List{Value: []Object{&Number{Value: 0}, &Number{Value: 1}}}, true},
		{"SET :list[0] = :one", ":list", &List{Value: []Object{&Number{Value: 1}, &Number{Value: 1}}}, true},
		{
			"SET :matrix[0][0] = :one",
			":matrix",
			&List{Value: []Object{&List{Value: []Object{&Number{Value: 1}}}}},
			false,
		},
		{
			"SET :hash.a = :one",
			":hash",
			&Map{Value: map[string]Object{"a": &Number{Value: 1}}},
			false,
		},
		{
			"SET :hash.:mapField = :one",
			":hash",
			&Map{Value: map[string]Object{"a": &Boolean{Value: true}, "key": &Number{Value: 1}}},
			false,
		},
		{
			"SET :two = if_not_exists(:hash.not_found, :one) + :one",
			":two",
			&Number{Value: 2},
			false,
		},
		{
			"SET :nestedMap.lvl1.lvl2 = :nestedMap.lvl1.lvl2 + :one",
			":nestedMap",
			&Map{Value: map[string]Object{"lvl1": &Map{Value: map[string]Object{"lvl2": &Number{Value: 1}}}}},
			false,
		},
		{
			"SET :nestedMap.#pos = #pos + :one",
			":nestedMap",
			&Map{Value: map[string]Object{"lvl1": &Map{Value: map[string]Object{"lvl2": &Number{Value: 0}}}, ":nestedMap.lvl1.lvl2": &Number{Value: 1}}},
			false,
		},
		{
			"SET :nestedMap.#secondLevel = #pos + :one",
			":nestedMap",
			&Map{Value: map[string]Object{"lvl1": &Map{Value: map[string]Object{"lvl2": &Number{Value: 0}}}, "lvl1.lvl2": &Number{Value: 1}}},
			false,
		},
	}

	env := startEvalUpdateEnv(t)

	for _, tt := range tests {
		if !tt.keepEnv {
			env = startEvalUpdateEnv(t)
		}

		result := testEvalUpdate(t, tt.input, env)
		if isError(result) {
			t.Fatalf("error evaluating update %q, env=%s, %s", tt.input, env.String(), result.Inspect())
		}

		result = env.Get(tt.envField)

		if result.Inspect() != tt.expected.Inspect() {
			t.Errorf("result has wrong value for %q in %q. got=%v, want=%v", tt.envField, tt.input, result.Inspect(), tt.expected.Inspect())
		}
	}
}

func TestEvalAddUpdate(t *testing.T) {
	tests := []struct {
		input    string
		envField string
		expected Object
		keepEnv  bool
	}{
		{"ADD :one :one", ":one", &Number{Value: 2}, false},
		{"ADD :numSet :one", ":numSet", &NumberSet{Value: map[float64]bool{1: true, 2: true, 4: true}}, false},
		{"ADD :binSet :bin", ":binSet", &BinarySet{Value: [][]byte{[]byte("a"), []byte("b"), []byte("c")}}, false},
		{"ADD :strSet :val", ":strSet", &StringSet{Value: map[string]bool{"a": true, "b": true, "text": true}}, false},
		{"ADD newVal :val", ":val", &String{Value: "text"}, false},
	}

	env := startEvalUpdateEnv(t)

	for _, tt := range tests {
		if !tt.keepEnv {
			env = startEvalUpdateEnv(t)
		}

		result := testEvalUpdate(t, tt.input, env)
		if isError(result) {
			t.Fatalf("error evaluating update %q, env=%s, %s", tt.input, env.String(), result.Inspect())
		}

		result = env.Get(tt.envField)

		if result.Inspect() != tt.expected.Inspect() {
			t.Errorf("result has wrong value for %q in %q. got=%v, want=%v", tt.envField, tt.input, result.Inspect(), tt.expected.Inspect())
		}
	}
}

func TestEvalRemoveUpdate(t *testing.T) {
	tests := []struct {
		input    string
		envField string
		expected Object
		keepEnv  bool
	}{
		{"DELETE :binSet :binA", ":binSet", &BinarySet{Value: [][]byte{[]byte("b")}}, false},
		{"DELETE :strSet :a", ":strSet", &StringSet{Value: map[string]bool{"b": true}}, false},
		{"DELETE :numSet :two", ":numSet", &NumberSet{Value: map[float64]bool{4: true}}, false},
	}

	env := startEvalUpdateEnv(t)

	for _, tt := range tests {
		if !tt.keepEnv {
			env = startEvalUpdateEnv(t)
		}

		result := testEvalUpdate(t, tt.input, env)
		if isError(result) {
			t.Fatalf("error evaluating update %q, env=%s, %s", tt.input, env.String(), result.Inspect())
		}

		result = env.Get(tt.envField)

		if result.Inspect() != tt.expected.Inspect() {
			t.Errorf("result has wrong value for %q in %q. got=%v, want=%v", tt.envField, tt.input, result.Inspect(), tt.expected.Inspect())
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
			"syntax error; token: :x",
		},
		{
			"NOT :x",
			"syntax error; token: :x",
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
			"syntax error; token: :nil",
		},
		{
			"NOT :notfound",
			"syntax error; token: :notfound",
		},
		{
			"size(:notfound) AND :a = :b",
			"type not supported: size NULL",
		},
		{
			":a = :b AND size(:notfound)",
			"type not supported: size NULL",
		},
		{
			"size(:notfound) AND :a",
			"syntax error; token: :a",
		},
		{
			":a AND size(:notfound)",
			"syntax error; token: :a",
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
		{
			"list_append(:list,:x)",
			"the function is not allowed in an condition expression; function: list_append",
		},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":a":   {BOOL: aws.Bool(true)},
		":b":   {BOOL: aws.Bool(false)},
		":x":   {N: aws.String("24")},
		":y":   {N: aws.String("25")},
		":z":   {N: aws.String("26")},
		":str": {S: aws.String("TEXT")},
		":nil": {NULL: aws.Bool(true)},
		":list": {L: []*dynamodb.AttributeValue{
			{S: aws.String("a")},
		}},
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
		":y":   {N: aws.String("25")},
		":str": {S: aws.String("TEXT")},
		":obj": {
			M: map[string]*dynamodb.AttributeValue{
				"a": {BOOL: aws.Bool(true)},
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

func TestEvalErrors(t *testing.T) {
	env := NewEnvironment()

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":x":   {BOOL: aws.Bool(true)},
		":val": {S: aws.String("text")},
		":one": {N: aws.String("1")},
		":h": {
			M: map[string]*dynamodb.AttributeValue{
				"a": {BOOL: aws.Bool(true)},
			},
		},
		":voidVal": {
			NULL: aws.Bool(true),
		},
		":list": {L: []*dynamodb.AttributeValue{
			{S: aws.String("a")},
		}},
	})
	if err != nil {
		panic(err)
	}

	testCases := []struct {
		inputs   []interface{}
		function func(...interface{}) Object
		expected Object
	}{
		{
			inputs: []interface{}{&String{Value: "hello"}},
			function: func(args ...interface{}) Object {
				arg := args[0].(Object)
				return evalBangOperatorExpression(arg)
			},
			expected: newError("unknown operator: NOT S"),
		},
		{
			inputs: []interface{}{&IndexExpression{
				Token: Token{Type: LBRACKET, Literal: "["},
				Left: &Identifier{
					Token: Token{Type: IDENT, Literal: "a"},
					Value: "a",
				},
				Index: &Identifier{
					Token: Token{Type: LPAREN, Literal: "("},
					Value: ":i",
				}}, env},
			function: func(args ...interface{}) Object {
				arg := args[0].(*IndexExpression)
				arg1 := args[1].(*Environment)
				_, _, err := evalIndexPositions(arg, arg1)
				return err
			},
			expected: newError("index operator not supported: got \":i\""),
		},
		{
			inputs: []interface{}{
				&ActionExpression{
					Token: Token{Type: DELETE, Literal: "DELETE"},
					Left:  &Identifier{Value: ":r", Token: Token{Type: IDENT, Literal: ":x"}},
					Right: &Identifier{Value: ":r", Token: Token{Type: IDENT, Literal: ":x"}},
				},
				env,
			},
			function: func(args ...interface{}) Object {
				arg := args[0].(*ActionExpression)
				arg1 := args[1].(*Environment)
				return evalActionDelete(arg, arg1)
			},
			expected: NULL,
		},
	}

	for i, tt := range testCases {
		result := tt.function(tt.inputs...)

		if result.Inspect() != tt.expected.Inspect() {
			t.Errorf("(%d) wrong result value. expected=%q, got=%q", i, tt.expected, result)
		}
	}
}

func TestUpdateEvalSyntaxError(t *testing.T) {
	tests := []struct {
		input           string
		expectedMessage string
	}{
		{
			":one + :one",
			"invalid update expression: (:one + :one)",
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
			"invalid assignation to: SET ((:one + :one) = :val)",
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
		{
			"ADD :voidVal :one",
			"an operand in the update expression has an incorrect data type",
		},
		{
			"SET :x[1] = :one",
			"index operator not supported for \"BOOL\"",
		},
		{
			"SET :list[:x] = :val",
			"access index with [] only support N as index : got \"BOOL\"",
		},
		{
			"SET :val = sizze(:list)",
			"invalid function name; function: sizze",
		},
		{
			"SET :val = size(:x)",
			"the function is not allowed in an update expression; function: size",
		},
		{
			"DELETE :x :list",
			"an operand in the update expression has an incorrect data type",
		},
		{
			"DELETE :list sizze(:x)",
			"invalid function name; function: sizze",
		},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":x":   {BOOL: aws.Bool(true)},
		":val": {S: aws.String("text")},
		":one": {N: aws.String("1")},
		":h": {
			M: map[string]*dynamodb.AttributeValue{
				"a": {BOOL: aws.Bool(true)},
			},
		},
		":voidVal": {
			NULL: aws.Bool(true),
		},
		":list": {L: []*dynamodb.AttributeValue{
			{S: aws.String("a")},
		}},
	})
	if err != nil {
		panic(err)
	}

	for i, tt := range tests {
		evaluated := testEvalUpdate(t, tt.input, env)

		errObj, ok := evaluated.(*Error)
		if !ok {
			t.Errorf("(%d) no error object returned for %q. got=%T(%+v)", i, tt.input, evaluated, evaluated)
			continue
		}

		if errObj.Message != tt.expectedMessage {
			t.Errorf("wrong error message for %q. expected=%q, got=%q", tt.input, tt.expectedMessage, errObj.Message)
		}
	}
}

func BenchmarkEval(b *testing.B) {
	input := ":a OR :b"

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*dynamodb.AttributeValue{
		":a": {BOOL: aws.Bool(true)},
		":b": {BOOL: aws.Bool(false)},
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
	p := NewUpdateParser(l)
	update := p.ParseUpdateExpression()

	if len(p.errors) != 0 {
		t.Fatalf("parsing %q failed: %s", input, strings.Join(p.errors, ";\n"))
	}

	return EvalUpdate(update, env)
}
