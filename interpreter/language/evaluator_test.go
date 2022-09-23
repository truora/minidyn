package language

import (
	"strings"
	"testing"

	"github.com/truora/minidyn/types"
)

var (
	boolTrue  = true
	boolFalse = false
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
		// IN
		{":my_undefined_field IN (:y, :z", FALSE},
		{":x IN (:y, :z", FALSE},
		{":x IN (:y, :x", TRUE},
		{":binA IN (:nil, :x", FALSE},
		{":binA IN (:nil, :x, :binA", TRUE},
		{":listA IN (:nil, :x, :binA", FALSE},
		{":listA IN (:nil, :x, :listA", TRUE},
		{":numSetA IN (:numSetB, :listA", FALSE},
		{":numSetA IN (:numSetB, :numSetA", TRUE},
	}

	env := NewEnvironment()

	env.Aliases = map[string]string{
		"#alias_field_name": "field_name",
	}

	err := env.AddAttributes(map[string]types.Item{
		":a":        {BOOL: &boolTrue},
		":b":        {BOOL: &boolFalse},
		":s":        {S: "HELLO WORLD!"},
		":x":        {N: "24"},
		":y":        {N: "25"},
		":z":        {N: "26"},
		":txtA":     {S: "a"},
		":txtB":     {S: "b"},
		":txtC":     {S: "c"},
		":binA":     {B: []byte("a")},
		":binB":     {B: []byte("b")},
		":binC":     {B: []byte("c")},
		":nil":      {NULL: &boolTrue},
		":otherNil": {NULL: &boolTrue},
		":hashA": {
			M: map[string]types.Item{
				":a": {BOOL: &boolTrue},
			},
		},
		":hashB": {
			M: map[string]types.Item{
				":b": {BOOL: &boolTrue},
			},
		},
		":listA": {
			L: []types.Item{
				{S: "a"},
				{S: "b"},
				{S: "c"},
			},
		},
		":listB": {
			L: []types.Item{
				{S: "x"},
				{S: "y"},
				{S: "z"},
			},
		},
		":strSetA": {
			SS: []string{"a", "a", "b"},
		},
		":strSetB": {
			SS: []string{"x", "x", "y"},
		},
		":binSetA": {
			BS: [][]byte{[]byte("a"), []byte("a"), []byte("b")},
		},
		":binSetB": {
			BS: [][]byte{[]byte("x"), []byte("x"), []byte("y")},
		},
		":numSetA": {
			NS: []string{"1", "2", "4"},
		},
		":numSetB": {
			NS: []string{"10", "10", "11"},
		},
		":matrix": {
			L: []types.Item{
				{L: []types.Item{
					{S: "a"},
					{S: "b"},
				}},
				{L: []types.Item{
					{S: "c"},
				}},
			},
		},
		":listIndex": {N: "0"},
		":nestedMap": {
			M: map[string]types.Item{
				"lvl1": {
					M: map[string]types.Item{
						"lvl2": {BOOL: &boolTrue},
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

	err := env.AddAttributes(map[string]types.Item{
		":s":       {S: "HELLO WORLD!"},
		":sSize":   {N: "12"},
		":type":    {S: "S"},
		":bin":     {B: []byte{10, 10, 10}},
		":binSize": {N: "3"},
		":prefix":  {S: "HELLO"},
		":subtext": {S: "ELL"},
		":element": {S: "a"},
		":num":     {N: "1"},
		":list": {
			L: []types.Item{
				{S: "a"},
				{S: "b"},
				{S: "c"},
			},
		},
		":strSet": {
			SS: []string{"a", "a", "b"},
		},
		":binSet": {
			BS: [][]byte{{10, 10, 10}},
		},
		":numSet": {
			NS: []string{"1", "2", "4"},
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

	err := env.AddAttributes(map[string]types.Item{
		":x":    {BOOL: &boolTrue},
		":val":  {S: "text"},
		":one":  {N: "1"},
		":bin":  {B: []byte("c")},
		":list": {L: []types.Item{{N: "0"}}},
		":hash": {
			M: map[string]types.Item{
				"a": {BOOL: &boolTrue},
			},
		},
		":mapField": {S: "key"},
		":matrix": {
			L: []types.Item{
				{L: []types.Item{{N: "0"}}},
			},
		},
		":nestedMap": {
			M: map[string]types.Item{
				"lvl1": {
					M: map[string]types.Item{
						"lvl2": {N: "0"},
					},
				},
			},
		},

		":strSet": {
			SS: []string{"a", "a", "b"},
		},
		":a": {
			S: "a",
		},
		":binSet": {
			BS: [][]byte{[]byte("a"), []byte("b")},
		},
		":binA": {
			B: []byte("a"),
		},
		":numSet": {
			NS: []string{"2", "4"},
		},
		":two": {
			N: "2",
		},
		":tools": {L: []types.Item{
			{S: "Chisel"},
			{S: "Hammer"},
			{S: "Nails"},
			{S: "Screwdriver"},
			{S: "Hacksaw"},
		}},
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
		{"SET :x = :val", ":x", &String{Value: "text"}, boolTrue},
		{"SET :w = :val", ":w", &String{Value: "text"}, boolTrue},
		{"SET :two = :one + :one", ":two", &Number{Value: 2}, boolTrue},
		{"SET :zero = :one - :one", ":zero", &Number{Value: 0}, boolTrue},
		{"SET :zero = :one - :one", ":zero", &Number{Value: 0}, boolTrue},
		{"SET :newTwo = if_not_exists(not_found, :one + :one", ":newTwo", &Number{Value: 2}, boolTrue},
		{"SET :three = if_not_exists(:two, :one + :one", ":three", &Number{Value: 3}, boolTrue},
		{"SET :list[1] = :one", ":list", &List{Value: []Object{&Number{Value: 0}, &Number{Value: 1}}}, boolTrue},
		{"SET :list[0] = :one", ":list", &List{Value: []Object{&Number{Value: 1}, &Number{Value: 1}}}, boolTrue},
		{
			"SET :matrix[0][0] = :one",
			":matrix",
			&List{Value: []Object{&List{Value: []Object{&Number{Value: 1}}}}},
			boolFalse,
		},
		{
			"SET :hash.a = :one",
			":hash",
			&Map{Value: map[string]Object{"a": &Number{Value: 1}}},
			boolFalse,
		},
		{
			"SET :hash.:mapField = :one",
			":hash",
			&Map{Value: map[string]Object{"a": &Boolean{Value: boolTrue}, "key": &Number{Value: 1}}},
			boolFalse,
		},
		{
			"SET :two = if_not_exists(:hash.not_found, :one + :one",
			":two",
			&Number{Value: 2},
			boolFalse,
		},
		{
			"SET :nestedMap.lvl1.lvl2 = :nestedMap.lvl1.lvl2 + :one",
			":nestedMap",
			&Map{Value: map[string]Object{"lvl1": &Map{Value: map[string]Object{"lvl2": &Number{Value: 1}}}}},
			boolFalse,
		},
		{
			"SET :nestedMap.#pos = #pos + :one",
			":nestedMap",
			&Map{Value: map[string]Object{"lvl1": &Map{Value: map[string]Object{"lvl2": &Number{Value: 0}}}, ":nestedMap.lvl1.lvl2": &Number{Value: 1}}},
			boolFalse,
		},
		{
			"SET :nestedMap.#secondLevel = #pos + :one",
			":nestedMap",
			&Map{Value: map[string]Object{"lvl1": &Map{Value: map[string]Object{"lvl2": &Number{Value: 0}}}, "lvl1.lvl2": &Number{Value: 1}}},
			boolFalse,
		},
		{"SET :x = :val REMOVE :val", ":x", &String{Value: "text"}, boolTrue},
		{"SET :x = :val REMOVE :val", ":val", NULL, boolTrue},
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
		{"ADD :one :one", ":one", &Number{Value: 2}, boolFalse},
		{"ADD :numSet :one", ":numSet", &NumberSet{Value: map[float64]bool{1: boolTrue, 2: boolTrue, 4: boolTrue}}, boolFalse},
		{"ADD :binSet :bin", ":binSet", &BinarySet{Value: [][]byte{[]byte("a"), []byte("b"), []byte("c")}}, boolFalse},
		{"ADD :strSet :val", ":strSet", &StringSet{Value: map[string]bool{"a": boolTrue, "b": boolTrue, "text": boolTrue}}, boolFalse},
		{"ADD newVal :val", ":val", &String{Value: "text"}, boolFalse},
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
		{"REMOVE :binSet", ":binSet", NULL, boolFalse},
		{"REMOVE :binSet,:a", ":a", NULL, boolFalse},
		{"REMOVE :tools[1], :tools[2]", ":tools", &List{Value: []Object{&String{Value: "Chisel"}, &String{Value: "Screwdriver"}, &String{Value: "Hacksaw"}}}, boolFalse},
		{"REMOVE :nestedMap.lvl1.lvl2", ":nestedMap", &Map{Value: map[string]Object{"lvl1": &Map{Value: map[string]Object{}}}}, boolFalse},
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

func TestEvalDELETEUpdate(t *testing.T) {
	tests := []struct {
		input    string
		envField string
		expected Object
		keepEnv  bool
	}{
		{"DELETE :binSet :binA", ":binSet", &BinarySet{Value: [][]byte{[]byte("b")}}, boolFalse},
		{"DELETE :strSet :a", ":strSet", &StringSet{Value: map[string]bool{"b": boolTrue}}, boolFalse},
		{"DELETE :numSet :two", ":numSet", &NumberSet{Value: map[float64]bool{4: boolTrue}}, boolFalse},
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
			"size(:a",
			"type not supported: size BOOL",
		},
		{
			"undefined(:a",
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
			"size(:notfound AND :a = :b",
			"type not supported: size NULL",
		},
		{
			":a = :b AND size(:notfound",
			"type not supported: size NULL",
		},
		{
			"size(:notfound AND :a",
			"syntax error; token: :a",
		},
		{
			":a AND size(:notfound",
			"syntax error; token: :a",
		},
		{
			"NOT size(:notfound",
			"type not supported: size NULL",
		},
		{
			":y BETWEEN :a AND :b",
			"unexpected type: \":a\" should be a comparable type(N,S,B got \"BOOL\"",
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
			"list_append(:list,:x",
			"the function is not allowed in an condition expression; function: list_append",
		},
		{
			"ROLE IN (:x, :str",
			"reserved word ROLE found in expression",
		},
		{
			":x IN (ROLE, :str",
			"reserved word ROLE found in expression",
		},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]types.Item{
		":a":   {BOOL: &boolTrue},
		":b":   {BOOL: &boolFalse},
		":x":   {N: "24"},
		":y":   {N: "25"},
		":z":   {N: "26"},
		":str": {S: "TEXT"},
		":nil": {NULL: &boolTrue},
		":list": {L: []types.Item{
			{S: "a"},
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

func TestEvalUpdateReservedKeywords(t *testing.T) {
	tests := []struct {
		input           string
		expectedMessage string
	}{
		{
			"SET status = :status",
			"reserved word STATUS found in expression",
		},
		{
			"REMOVE status,keys,hash",
			"reserved word STATUS found in expression",
		},
		{
			"ADD avg 5",
			"reserved word AVG found in expression",
		},
		{
			"DELETE keys :keys",
			"reserved word KEYS found in expression",
		},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]types.Item{
		":status": {S: "healthy"},
		":keys":   {SS: []string{"Key", "Another Key"}},
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

	err := env.AddAttributes(map[string]types.Item{
		":y":   {N: "25"},
		":str": {S: "TEXT"},
		":obj": {
			M: map[string]types.Item{
				"a": {BOOL: &boolTrue},
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
		t.Fatal("expected to be boolFalse")
	}

	err := newError("testing")

	b = isError(err)
	if !b {
		t.Fatal("expected to be boolTrue")
	}
}

func TestIsNumber(t *testing.T) {
	if isNumber(nil) {
		t.Fatal("expected to be boolFalse")
	}

	num := Number{Value: 10}
	if !isNumber(&num) {
		t.Fatal("expected to be boolTrue")
	}
}

func TestIsString(t *testing.T) {
	if isString(nil) {
		t.Fatal("expected to be boolFalse")
	}

	str := String{Value: "txt"}
	if !isString(&str) {
		t.Fatal("expected to be boolTrue")
	}
}

func TestEvalErrors(t *testing.T) {
	env := NewEnvironment()

	err := env.AddAttributes(map[string]types.Item{
		":x":   {BOOL: &boolTrue},
		":val": {S: "text"},
		":one": {N: "1"},
		":h": {
			M: map[string]types.Item{
				"a": {BOOL: &boolTrue},
			},
		},
		":voidVal": {
			NULL: &boolTrue,
		},
		":list": {L: []types.Item{
			{S: "a"},
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

	err := env.AddAttributes(map[string]types.Item{
		":x":   {BOOL: &boolTrue},
		":val": {S: "text"},
		":one": {N: "1"},
		":h": {
			M: map[string]types.Item{
				"a": {BOOL: &boolTrue},
			},
		},
		":voidVal": {
			NULL: &boolTrue,
		},
		":list": {L: []types.Item{
			{S: "a"},
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

	err := env.AddAttributes(map[string]types.Item{
		":a": {BOOL: &boolTrue},
		":b": {BOOL: &boolFalse},
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
			b.Fatal("expected to be boolTrue")
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
