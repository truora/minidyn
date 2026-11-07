package language

import (
	"fmt"
	"sort"
	"strconv"
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
		{":a = :a or :a = :b", TRUE},
		{":a = :a and :b = :b", TRUE},
		{"not :a = :a", FALSE},
		{":a = :a Or :a = :b", TRUE},
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
		{":my_undefined_field IN (:y, :z)", FALSE},
		{":x IN (:y, :z)", FALSE},
		{":x IN (:y, :x)", TRUE},
		{":binA IN (:nil, :x)", FALSE},
		{":binA IN (:nil, :x, :binA)", TRUE},
		{":listA IN (:nil, :x, :binA)", FALSE},
		{":listA IN (:nil, :x, :listA)", TRUE},
		{":numSetA IN (:numSetB, :listA)", FALSE},
		{":numSetA IN (:numSetB, :numSetA)", TRUE},
	}

	env := NewEnvironment()

	env.Aliases = map[string]string{
		"#alias_field_name": "field_name",
	}

	err := env.AddAttributes(map[string]*types.Item{
		":a":        {BOOL: &boolTrue},
		":b":        {BOOL: &boolFalse},
		":s":        {S: new("HELLO WORLD!")},
		":x":        {N: new("24")},
		":y":        {N: new("25")},
		":z":        {N: new("26")},
		":txtA":     {S: new("a")},
		":txtB":     {S: new("b")},
		":txtC":     {S: new("c")},
		":binA":     {B: []byte("a")},
		":binB":     {B: []byte("b")},
		":binC":     {B: []byte("c")},
		":nil":      {NULL: &boolTrue},
		":otherNil": {NULL: &boolTrue},
		":hashA": {
			M: map[string]*types.Item{
				":a": {BOOL: &boolTrue},
			},
		},
		":hashB": {
			M: map[string]*types.Item{
				":b": {BOOL: &boolTrue},
			},
		},
		":listA": {
			L: []*types.Item{
				{S: new("a")},
				{S: new("b")},
				{S: new("c")},
			},
		},
		":listB": {
			L: []*types.Item{
				{S: new("x")},
				{S: new("y")},
				{S: new("z")},
			},
		},
		":strSetA": {
			SS: []*string{new("a"), new("a"), new("b")},
		},
		":strSetB": {
			SS: []*string{new("x"), new("x"), new("y")},
		},
		":binSetA": {
			BS: [][]byte{[]byte("a"), []byte("a"), []byte("b")},
		},
		":binSetB": {
			BS: [][]byte{[]byte("x"), []byte("x"), []byte("y")},
		},
		":numSetA": {
			NS: []*string{new("1"), new("2"), new("4")},
		},
		":numSetB": {
			NS: []*string{new("10"), new("10"), new("11")},
		},
		":matrix": {
			L: []*types.Item{
				{L: []*types.Item{
					{S: new("a")},
					{S: new("b")},
				}},
				{L: []*types.Item{
					{S: new("c")},
				}},
			},
		},
		":listIndex": {N: new("0")},
		":nestedMap": {
			M: map[string]*types.Item{
				"lvl1": {
					M: map[string]*types.Item{
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
		{"attribute_exists(:nilVal)", TRUE},
		{"attribute_not_exists(:n)", TRUE},
		{"attribute_not_exists(:nilVal)", FALSE},
		{"begins_with(:s, :prefix)", TRUE},
		{"contains(:s, :subtext)", TRUE},
		{"contains(:list, :element)", TRUE},
		{"contains(:strSet, :element)", TRUE},
		{"contains(:binSet, :bin)", TRUE},
		{"contains(:numSet, :num)", TRUE},
		{"attribute_type(:s, :type)", TRUE},
		{"attribute_type(:nilVal, :nullType)", TRUE},
		{"attribute_type(:n, :nullType)", FALSE},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*types.Item{
		":s":        {S: new("HELLO WORLD!")},
		":sSize":    {N: new("12")},
		":type":     {S: new("S")},
		":nullType": {S: new("NULL")},
		":nilVal":   {NULL: &boolTrue},
		":bin":      {B: []byte{10, 10, 10}},
		":binSize":  {N: new("3")},
		":prefix":   {S: new("HELLO")},
		":subtext":  {S: new("ELL")},
		":element":  {S: new("a")},
		":num":      {N: new("1")},
		":list": {
			L: []*types.Item{
				{S: new("a")},
				{S: new("b")},
				{S: new("c")},
			},
		},
		":strSet": {
			SS: []*string{new("a"), new("a"), new("b")},
		},
		":binSet": {
			BS: [][]byte{{10, 10, 10}},
		},
		":numSet": {
			NS: []*string{new("1"), new("2"), new("4")},
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

	err := env.AddAttributes(map[string]*types.Item{
		":x":    {BOOL: &boolTrue},
		":val":  {S: new("text")},
		":one":  {N: new("1")},
		":bin":  {B: []byte("c")},
		":list": {L: []*types.Item{{N: new("0")}}},
		":hash": {
			M: map[string]*types.Item{
				"a": {BOOL: &boolTrue},
			},
		},
		":mapField": {S: new("key")},
		":matrix": {
			L: []*types.Item{
				{L: []*types.Item{{N: new("0")}}},
			},
		},
		":nestedMap": {
			M: map[string]*types.Item{
				"lvl1": {
					M: map[string]*types.Item{
						"lvl2": {N: new("0")},
					},
				},
			},
		},
		"random_field_name": {
			S: new("Some random value"),
		},
		":strSet": {
			SS: []*string{new("a"), new("a"), new("b")},
		},
		":a": {
			S: new("a"),
		},
		":binSet": {
			BS: [][]byte{[]byte("a"), []byte("b")},
		},
		":binA": {
			B: []byte("a"),
		},
		":binASet": {BS: [][]byte{[]byte("a")}},
		":aSet":    {SS: []*string{new("a")}},
		":textSet": {SS: []*string{new("text")}},
		":oneSet":  {NS: []*string{new("1")}},
		":binCSet": {BS: [][]byte{[]byte("c")}},
		":twoSet":  {NS: []*string{new("2")}},
		":numSet": {
			NS: []*string{new("2"), new("4")},
		},
		":two": {
			N: new("2"),
		},
		":tools": {L: []*types.Item{
			{S: new("Chisel")},
			{S: new("Hammer")},
			{S: new("Nails")},
			{S: new("Screwdriver")},
			{S: new("Hacksaw")},
		}},
	})
	if err != nil {
		t.Fatalf("error adding attributes %#v", err)
	}

	env.Aliases = map[string]string{
		"#pos":             ":nestedMap.lvl1.lvl2",
		"#secondLevel":     "lvl1.lvl2",
		"#invalid":         ":one.:one",
		"#nested_map_attr": "random_field_name",
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

func TestEvalUpdateIndexAliasInvalidPath(t *testing.T) {
	env := startEvalUpdateEnv(t)

	// #nested_map_attr aliases a scalar string field; indexing it with [0] fails
	// and surfaces the resolved alias in the document-path error, exercising
	// indexFieldID's alias branch.
	result := testEvalUpdate(t, "SET #nested_map_attr[0] = :one", env)

	if !isError(result) {
		t.Fatalf("expected an error, got %v", result.Inspect())
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
			"SET :two = if_not_exists(:hash.not_found, :one) + :one",
			":two",
			&Number{Value: 2},
			false,
		},
		{
			"SET :all = list_append(if_not_exists(:all, :list), :tools)",
			":all",
			&List{Value: []Object{&Number{Value: 0}, &String{Value: "Chisel"}, &String{Value: "Hammer"}, &String{Value: "Nails"}, &String{Value: "Screwdriver"}, &String{Value: "Hacksaw"}}},
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
		{
			"SET :nestedMap.lvl1.#nested_map_attr = :one",
			":nestedMap",
			&Map{Value: map[string]Object{"lvl1": &Map{Value: map[string]Object{"random_field_name": &Number{Value: 1}, "lvl2": &Number{Value: 0}}}}},
			false,
		},
		{
			"SET :nestedMap.lvl1.random_field_name = :one",
			":nestedMap",
			&Map{Value: map[string]Object{"lvl1": &Map{Value: map[string]Object{"random_field_name": &Number{Value: 1}, "lvl2": &Number{Value: 0}}}}},
			false,
		},
		{"SET :x = :val REMOVE :val", ":x", &String{Value: "text"}, true},
		{"SET :x = :val REMOVE :val", ":val", UNDEFINED, true},
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
		{"ADD :numSet :oneSet", ":numSet", &NumberSet{Value: map[float64]bool{1: boolTrue, 2: boolTrue, 4: boolTrue}}, boolFalse},
		{"ADD :binSet :binCSet", ":binSet", &BinarySet{Value: [][]byte{[]byte("a"), []byte("b"), []byte("c")}}, boolFalse},
		{"ADD :strSet :textSet", ":strSet", &StringSet{Value: map[string]bool{"a": boolTrue, "b": boolTrue, "text": boolTrue}}, boolFalse},
		{"ADD newVal :one", "newVal", &Number{Value: 1}, boolFalse},
		{"ADD newSet :textSet", "newSet", &StringSet{Value: map[string]bool{"text": boolTrue}}, boolFalse},
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
		{"REMOVE :binSet", ":binSet", UNDEFINED, boolFalse},
		{"REMOVE :binSet,:a", ":a", UNDEFINED, boolFalse},
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
		{"DELETE :binSet :binASet", ":binSet", &BinarySet{Value: [][]byte{[]byte("b")}}, boolFalse},
		{"DELETE :strSet :aSet", ":strSet", &StringSet{Value: map[string]bool{"b": boolTrue}}, boolFalse},
		{"DELETE :numSet :twoSet", ":numSet", &NumberSet{Value: map[float64]bool{4: boolTrue}}, boolFalse},
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

func TestEvalDELETEUpdateMissingAttribute(t *testing.T) {
	env := startEvalUpdateEnv(t)

	result := testEvalUpdate(t, "DELETE absent_set :strSet", env)
	if isError(result) {
		t.Fatalf("error evaluating update, env=%s, %s", env.String(), result.Inspect())
	}

	if got := env.Get("absent_set"); got != UNDEFINED {
		t.Errorf("DELETE on a missing attribute must not create it, got=%v", got.Inspect())
	}
}

func TestEvalDELETERejectsMismatchedSetType(t *testing.T) {
	const prefix = "An operand in the update expression has an incorrect data type | "

	tests := []struct {
		update string
		field  string
		detail string
	}{
		{"DELETE :strSet :twoSet", ":strSet", "operator: DELETE; target type: SS; operand type: NS"},
		{"DELETE :strSet :binASet", ":strSet", "operator: DELETE; target type: SS; operand type: BS"},
		{"DELETE :numSet :aSet", ":numSet", "operator: DELETE; target type: NS; operand type: SS"},
		{"DELETE :binSet :twoSet", ":binSet", "operator: DELETE; target type: BS; operand type: NS"},
	}

	for _, tt := range tests {
		t.Run(tt.update, func(t *testing.T) {
			env := startEvalUpdateEnv(t)
			before := env.Get(tt.field).Inspect()

			result := testEvalUpdate(t, tt.update, env)

			errObj, ok := result.(*Error)
			if !ok {
				t.Fatalf("expected an error, got=%v", result.Inspect())
			}

			if want := prefix + tt.detail; errObj.Message != want {
				t.Errorf("wrong message.\n got=%q\nwant=%q", errObj.Message, want)
			}

			if got := env.Get(tt.field).Inspect(); got != before {
				t.Errorf("target changed: got=%v, want=%v", got, before)
			}
		})
	}
}

func TestEvalADDRejectsInvalidOperand(t *testing.T) {
	const typeSetMsg = "Invalid UpdateExpression: Incorrect operand type for operator or function; " +
		"operator: ADD, operand type: %s, typeSet: ALLOWED_FOR_ADD_OPERAND"

	const dataTypeMsg = "An operand in the update expression has an incorrect data type | " +
		"operator: ADD; target type: %s; operand type: %s"

	tests := []struct {
		update string
		want   string
	}{
		{"ADD :one :val", fmt.Sprintf(typeSetMsg, "STRING")},
		{"ADD absent_attr :x", fmt.Sprintf(typeSetMsg, "BOOLEAN")},
		{"ADD absent_attr :hash", fmt.Sprintf(typeSetMsg, "MAP")},
		{"ADD :one :x", fmt.Sprintf(typeSetMsg, "BOOLEAN")},
		{"ADD :one :list", fmt.Sprintf(typeSetMsg, "LIST")},
		{"ADD :numSet :hash", fmt.Sprintf(typeSetMsg, "MAP")},
		{"ADD :binSet :val", fmt.Sprintf(typeSetMsg, "STRING")},
		{"ADD :strSet :val", fmt.Sprintf(typeSetMsg, "STRING")},
		{"ADD :binSet :bin", fmt.Sprintf(typeSetMsg, "BINARY")},
		{"ADD :numSet :one", fmt.Sprintf(dataTypeMsg, "NS", "N")},
		{"ADD :strSet :hash", fmt.Sprintf(typeSetMsg, "MAP")},
		{"ADD :strSet :one", fmt.Sprintf(dataTypeMsg, "SS", "N")},
		{"ADD :numSet :strSet", fmt.Sprintf(dataTypeMsg, "NS", "SS")},
		{"ADD :binSet :one", fmt.Sprintf(dataTypeMsg, "BS", "N")},
		{"ADD :val :one", fmt.Sprintf(dataTypeMsg, "S", "N")},
	}

	for _, tt := range tests {
		t.Run(tt.update, func(t *testing.T) {
			env := startEvalUpdateEnv(t)

			errObj, ok := testEvalUpdate(t, tt.update, env).(*Error)
			if !ok {
				t.Fatalf("expected an error")
			}

			if errObj.Message != tt.want {
				t.Errorf("wrong message.\n got=%q\nwant=%q", errObj.Message, tt.want)
			}
		})
	}
}

func TestEvalDELETERejectsNonSetOperand(t *testing.T) {
	const prefix = "Invalid UpdateExpression: Incorrect operand type for operator or function; " +
		"operator: DELETE, operand type: %s, typeSet: ALLOWED_FOR_DELETE_OPERAND"

	tests := []struct {
		name    string
		update  string
		operand string
	}{
		{"string on existing set", "DELETE target :a", "STRING"},
		{"number on existing set", "DELETE target :two", "NUMBER"},
		{"binary on existing set", "DELETE target :binA", "BINARY"},
		{"boolean on existing set", "DELETE target :x", "BOOLEAN"},
		{"list on existing set", "DELETE target :list", "LIST"},
		{"map on existing set", "DELETE target :hash", "MAP"},
		{"string on missing attribute", "DELETE absent :a", "STRING"},
		{"number on missing attribute", "DELETE absent :two", "NUMBER"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := startEvalUpdateEnv(t)

			if err := env.AddAttributes(map[string]*types.Item{
				"target": {SS: []*string{new("a"), new("b")}},
			}); err != nil {
				t.Fatal(err)
			}

			result := testEvalUpdate(t, tt.update, env)

			errObj, ok := result.(*Error)
			if !ok {
				t.Fatalf("expected an error, got=%v", result.Inspect())
			}

			if want := fmt.Sprintf(prefix, tt.operand); errObj.Message != want {
				t.Errorf("wrong message.\n got=%q\nwant=%q", errObj.Message, want)
			}

			if got, ok := env.Get("target").(*StringSet); !ok || len(got.Value) != 2 {
				t.Errorf("target set must be unchanged, got=%v", env.Get("target").Inspect())
			}
		})
	}
}

func TestEvalDELETEEmptiedSetRemovesAttribute(t *testing.T) {
	tests := []struct {
		name   string
		attr   *types.Item
		update string
	}{
		{"string set", &types.Item{SS: []*string{new("a"), new("b")}}, "DELETE target :strSet"},
		{"number set", &types.Item{NS: []*string{new("2"), new("4")}}, "DELETE target :numSet"},
		{"binary set", &types.Item{BS: [][]byte{[]byte("a"), []byte("b")}}, "DELETE target :binSet"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := startEvalUpdateEnv(t)

			if err := env.AddAttributes(map[string]*types.Item{"target": tt.attr}); err != nil {
				t.Fatal(err)
			}

			result := testEvalUpdate(t, tt.update, env)
			if isError(result) {
				t.Fatalf("error evaluating update, env=%s, %s", env.String(), result.Inspect())
			}

			if got := env.Get("target"); got != UNDEFINED {
				t.Errorf("emptied set must be removed, got=%v", got.Inspect())
			}
		})
	}
}

func TestEvalDELETEPartialSetKeepsAttribute(t *testing.T) {
	env := startEvalUpdateEnv(t)

	if err := env.AddAttributes(map[string]*types.Item{
		"target": {SS: []*string{new("a"), new("c")}},
	}); err != nil {
		t.Fatal(err)
	}

	result := testEvalUpdate(t, "DELETE target :strSet", env)
	if isError(result) {
		t.Fatalf("error evaluating update, %s", result.Inspect())
	}

	if got := env.Get("target"); got == UNDEFINED {
		t.Error("a set with remaining elements must be kept")
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
			"the function is not allowed in a condition expression; function: list_append",
		},
		{
			"ROLE IN (:x, :str)",
			"reserved word ROLE found in expression",
		},
		{
			":x IN (ROLE, :str)",
			"reserved word ROLE found in expression",
		},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*types.Item{
		":a":   {BOOL: &boolTrue},
		":b":   {BOOL: &boolFalse},
		":x":   {N: new("24")},
		":y":   {N: new("25")},
		":z":   {N: new("26")},
		":str": {S: new("TEXT")},
		":nil": {NULL: &boolTrue},
		":list": {L: []*types.Item{
			{S: new("a")},
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

	err := env.AddAttributes(map[string]*types.Item{
		":status": {S: new("healthy")},
		":keys":   {SS: []*string{new("Key"), new("Another Key")}},
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

//nolint:gocognit // multiple table-driven scenarios for keyword validation
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

	err := env.AddAttributes(map[string]*types.Item{
		":y":   {N: new("25")},
		":str": {S: new("TEXT")},
		":obj": {
			M: map[string]*types.Item{
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

	err := env.AddAttributes(map[string]*types.Item{
		":x":   {BOOL: &boolTrue},
		":val": {S: new("text")},
		":one": {N: new("1")},
		":h": {
			M: map[string]*types.Item{
				"a": {BOOL: &boolTrue},
			},
		},
		":voidVal": {
			NULL: &boolTrue,
		},
		":list": {L: []*types.Item{
			{S: new("a")},
		}},
		":set": {SS: []*string{new("a")}},
	})
	if err != nil {
		panic(err)
	}

	testCases := []struct {
		inputs   []any
		function func(...any) Object
		expected Object
	}{
		{
			inputs: []any{&String{Value: "hello"}},
			function: func(args ...any) Object {
				arg, ok := args[0].(Object)
				if !ok {
					return newError("invalid function call; expression: %s", args[0])
				}

				return evalBangOperatorExpression(arg)
			},
			expected: newError("unknown operator: NOT S"),
		},
		{
			inputs: []any{
				&ActionExpression{
					Token: Token{Type: DELETE, Literal: "DELETE"},
					Left:  &Identifier{Value: ":r", Token: Token{Type: IDENT, Literal: ":x"}},
					Right: &Identifier{Value: ":set", Token: Token{Type: IDENT, Literal: ":set"}},
				},
				env,
			},
			function: func(args ...any) Object {
				arg, ok := args[0].(*ActionExpression)
				if !ok {
					return newError("invalid function call; expression: %s", args[0])
				}

				arg1, ok := args[1].(*Environment)
				if !ok {
					return newError("invalid function call; expression: %s", args[1])
				}

				return evalActionDelete(arg, arg1)
			},
			expected: UNDEFINED,
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
			"The document path provided in the update expression is invalid for update | field id: h; path: bar",
		},
		{
			"SET notFound.bar = :one",
			"The document path provided in the update expression is invalid for update | field id: notFound; path: bar",
		},
		{
			"SET :val.bar = :one",
			"The document path provided in the update expression is invalid for update | field id: :val; path: bar",
		},
		{
			"SET notFound[0] = :one",
			"The document path provided in the update expression is invalid for update | field id: notFound; path: 0",
		},
		{
			"SET info.a.b = :one",
			"The document path provided in the update expression is invalid for update | field id: info; path: a.b",
		},
		{
			"SET entry_list[0].a.b = :one",
			"The document path provided in the update expression is invalid for update | field id: entry_list; path: 0.a.b",
		},
		{
			"SET wrapper.label[0] = :one",
			"The document path provided in the update expression is invalid for update | field id: wrapper; path: label.0",
		},
		{
			"ADD :voidVal :one",
			"An operand in the update expression has an incorrect data type | operator: ADD; target type: NULL; operand type: N",
		},
		{
			"SET :x[1] = :one",
			"The document path provided in the update expression is invalid for update | field id: :x; path: 1",
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
			"DELETE :x :set",
			"An operand in the update expression has an incorrect data type | operator: DELETE; target type: BOOL; operand type: SS",
		},
		{
			"DELETE :list sizze(:x)",
			"invalid function name; function: sizze",
		},
	}

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*types.Item{
		":x":   {BOOL: &boolTrue},
		":val": {S: new("text")},
		":one": {N: new("1")},
		":h": {
			M: map[string]*types.Item{
				"a": {BOOL: &boolTrue},
			},
		},
		"info": {M: map[string]*types.Item{}},
		"entry_list": {L: []*types.Item{
			{M: map[string]*types.Item{}},
		}},
		"wrapper": {M: map[string]*types.Item{
			"label": {S: new("text")},
		}},
		":voidVal": {
			NULL: &boolTrue,
		},
		":list": {L: []*types.Item{
			{S: new("a")},
		}},
		":set": {SS: []*string{new("a")}},
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

func testEvalBooleanInfixUndefinedOrNil(t *testing.T, name string, left, right Object) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		got := evalBooleanInfixExpression("AND", left, right)
		if got != FALSE {
			t.Fatalf("AND: got %s, want FALSE", got.Inspect())
		}
		got = evalBooleanInfixExpression("OR", left, right)
		if got != FALSE {
			t.Fatalf("OR: got %s, want FALSE", got.Inspect())
		}
	})
}

func TestEvalBooleanInfixExpressionUndefinedOrNil(t *testing.T) {
	testEvalBooleanInfixUndefinedOrNil(t, "undefined_left", UNDEFINED, TRUE)
	testEvalBooleanInfixUndefinedOrNil(t, "undefined_right", TRUE, UNDEFINED)
	testEvalBooleanInfixUndefinedOrNil(t, "both_undefined", UNDEFINED, UNDEFINED)
	testEvalBooleanInfixUndefinedOrNil(t, "nil_left", nil, TRUE)
	testEvalBooleanInfixUndefinedOrNil(t, "nil_right", TRUE, nil)
}

func TestIsUndefinedObject(t *testing.T) {
	if !IsUndefinedObject(nil) {
		t.Fatal("nil should be undefined")
	}

	if !IsUndefinedObject(UNDEFINED) {
		t.Fatal("UNDEFINED sentinel should be undefined")
	}

	if IsUndefinedObject(&Null{IsUndefined: false}) {
		t.Fatal("NULL dynamodb value is not undefined sentinel")
	}

	if IsUndefinedObject(TRUE) {
		t.Fatal("boolean is not undefined")
	}
}

func TestEvalBooleanInfixExpressionLeftNotBoolean(t *testing.T) {
	got := evalBooleanInfixExpression("AND", &Number{Value: 1}, TRUE)
	want := newError("left operand is not boolean: N")
	if !isError(got) || got.Inspect() != want.Inspect() {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEvalBooleanInfixExpressionRightNotBoolean(t *testing.T) {
	got := evalBooleanInfixExpression("OR", TRUE, &String{Value: "x"})
	want := newError("right operand is not boolean: S")
	if !isError(got) || got.Inspect() != want.Inspect() {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func BenchmarkEval(b *testing.B) {
	input := ":a OR :b"

	env := NewEnvironment()

	err := env.AddAttributes(map[string]*types.Item{
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

func TestDynamoDBTypeName(t *testing.T) {
	tests := map[ObjectType]string{
		ObjectTypeString:    "STRING",
		ObjectTypeNull:      "NULL",
		ObjectTypeStringSet: "STRING_SET",
		ObjectTypeNumberSet: "NUMBER_SET",
		ObjectTypeBinarySet: "BINARY_SET",
		ObjectTypeFunction:  "FN",
	}

	for in, want := range tests {
		if got := dynamoDBTypeName(in); got != want {
			t.Errorf("dynamoDBTypeName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewInvalidFunctionCallErrorKeepsExpressionAsDetail(t *testing.T) {
	node := &CallExpression{
		Function:  &Identifier{Value: "f"},
		Arguments: []Expression{&Identifier{Value: "a"}},
	}

	got := newInvalidFunctionCallError(node)

	if want := "invalid function call | expression: " + node.String(); got.Message != want {
		t.Errorf("wrong message.\n got=%q\nwant=%q", got.Message, want)
	}
}

const (
	nestedPathInvalidMsg = "The document path provided in the update expression is invalid for update"
	nestedBadTypeMsg     = "An operand in the update expression has an incorrect data type"
	nestedAddOperandMsg  = "Invalid UpdateExpression: Incorrect operand type for operator or function; " +
		"operator: ADD, operand type: %s, typeSet: ALLOWED_FOR_ADD_OPERAND"
	nestedDeleteOperandMsg = "Invalid UpdateExpression: Incorrect operand type for operator or function; " +
		"operator: DELETE, operand type: %s, typeSet: ALLOWED_FOR_DELETE_OPERAND"

	nestedWantM = "{n:1 ns:{1,2} s:a ss:{x,y}}"
	nestedWantL = "[{x,y} 1 {1,2} a]"
)

func startNestedUpdateEnv(t *testing.T) *Environment {
	t.Helper()

	env := startEvalUpdateEnv(t)

	err := env.AddAttributes(map[string]*types.Item{
		"m": {M: map[string]*types.Item{
			"ss": {SS: []*string{new("x"), new("y")}},
			"ns": {NS: []*string{new("1"), new("2")}},
			"n":  {N: new("1")},
			"s":  {S: new("a")},
		}},
		"l": {L: []*types.Item{
			{SS: []*string{new("x"), new("y")}},
			{N: new("1")},
			{NS: []*string{new("1"), new("2")}},
			{S: new("a")},
		}},
		":xSet":     {SS: []*string{new("x")}},
		":xySet":    {SS: []*string{new("x"), new("y")}},
		":zSet":     {SS: []*string{new("z")}},
		":threeSet": {NS: []*string{new("3")}},
	})
	if err != nil {
		t.Fatal(err)
	}

	return env
}

// TestEvalNestedDeleteAndAdd checks DELETE and ADD on document paths below the top level
// against DynamoDB Local (#184). An empty wantM or wantL means the attribute is unchanged.
func TestEvalNestedDeleteAndAdd(t *testing.T) {
	tests := []struct {
		update string
		wantM  string
		wantL  string
	}{
		// DELETE on a nested set.
		{"DELETE m.ss :xSet", "{n:1 ns:{1,2} s:a ss:{y}}", ""},
		{"DELETE m.ss :xySet", "{n:1 ns:{1,2} s:a}", ""},
		{"DELETE m.ss :zSet", "", ""},
		{"DELETE m.ns :oneSet", "{n:1 ns:{2} s:a ss:{x,y}}", ""},
		{"DELETE m.nope :xSet", "", ""},
		// DELETE on list elements.
		{"DELETE l[0] :xSet", "", "[{y} 1 {1,2} a]"},
		{"DELETE l[0] :xySet", "", "[1 {1,2} a]"},
		{"DELETE l[2] :oneSet", "", "[{x,y} 1 {2} a]"},
		{"DELETE l[9] :xSet", "", ""},
		{"DELETE l[0] :xySet, l[2] :oneSet", "", "[1 {2} a]"},
		// ADD to nested sets and numbers.
		{"ADD m.ss :zSet", "{n:1 ns:{1,2} s:a ss:{x,y,z}}", ""},
		{"ADD m.ns :threeSet", "{n:1 ns:{1,2,3} s:a ss:{x,y}}", ""},
		{"ADD m.n :one", "{n:2 ns:{1,2} s:a ss:{x,y}}", ""},
		{"ADD l[0] :zSet", "", "[{x,y,z} 1 {1,2} a]"},
		{"ADD l[1] :one", "", "[{x,y} 2 {1,2} a]"},
		{"ADD l[2] :threeSet", "", "[{x,y} 1 {1,2,3} a]"},
		// ADD creating the key.
		{"ADD m.new :one", "{n:1 new:1 ns:{1,2} s:a ss:{x,y}}", ""},
		{"ADD m.new :zSet", "{n:1 new:{z} ns:{1,2} s:a ss:{x,y}}", ""},
		{"ADD l[9] :one", "", "[{x,y} 1 {1,2} a 1]"},
		{"ADD l[9] :zSet", "", "[{x,y} 1 {1,2} a {z}]"},
		// Several nested actions in one update.
		{"DELETE m.ss :xSet ADD m.n :one", "{n:2 ns:{1,2} s:a ss:{y}}", ""},
	}

	for _, tt := range tests {
		t.Run(tt.update, func(t *testing.T) {
			env := startNestedUpdateEnv(t)

			if result := testEvalUpdate(t, tt.update, env); isError(result) {
				t.Fatalf("unexpected error: %s", result.Inspect())
			}

			wantM, wantL := tt.wantM, tt.wantL
			if wantM == "" {
				wantM = nestedWantM
			}

			if wantL == "" {
				wantL = nestedWantL
			}

			if got := nestedInspect(env.Get("m")); got != wantM {
				t.Errorf("wrong m.\n got=%s\nwant=%s", got, wantM)
			}

			if got := nestedInspect(env.Get("l")); got != wantL {
				t.Errorf("wrong l.\n got=%s\nwant=%s", got, wantL)
			}
		})
	}
}

// nestedInspect renders an object with sorted map keys and set members and no type noise.
func nestedInspect(obj Object) string {
	switch o := obj.(type) {
	case *Map:
		keys := make([]string, 0, len(o.Value))
		for k := range o.Value {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+":"+nestedInspect(o.Value[k]))
		}

		return "{" + strings.Join(parts, " ") + "}"
	case *List:
		parts := make([]string, 0, len(o.Value))
		for _, e := range o.Value {
			parts = append(parts, nestedInspect(e))
		}

		return "[" + strings.Join(parts, " ") + "]"
	case *StringSet:
		members := make([]string, 0, len(o.Value))
		for k := range o.Value {
			members = append(members, k)
		}

		sort.Strings(members)

		return "{" + strings.Join(members, ",") + "}"
	case *NumberSet:
		members := make([]string, 0, len(o.Value))
		for k := range o.Value {
			members = append(members, strconv.FormatFloat(k, 'f', -1, 64))
		}

		sort.Strings(members)

		return "{" + strings.Join(members, ",") + "}"
	case *Number:
		return strconv.FormatFloat(o.Value, 'f', -1, 64)
	case *String:
		return o.Value
	}

	return obj.Inspect()
}

// TestEvalNestedDeleteAndAddErrors checks the failures DynamoDB Local reports for DELETE and
// ADD on nested paths (#184), and that a failed update leaves the item untouched.
func TestEvalNestedDeleteAndAddErrors(t *testing.T) {
	tests := []struct {
		update string
		want   string
	}{
		// A parent that is missing or not a map/list.
		{"DELETE nope.x :xSet", nestedPathInvalidMsg},
		{"DELETE m.nope.x :xSet", nestedPathInvalidMsg},
		{"DELETE m.s.x :xSet", nestedPathInvalidMsg},
		{"DELETE m.ss.x :xSet", nestedPathInvalidMsg},
		{"DELETE nope[0] :xSet", nestedPathInvalidMsg},
		{"DELETE m[0] :xSet", nestedPathInvalidMsg},
		{"ADD nope.x :one", nestedPathInvalidMsg},
		{"ADD nope.x :zSet", nestedPathInvalidMsg},
		{"ADD m.nope.x :one", nestedPathInvalidMsg},
		{"ADD m.s.x :one", nestedPathInvalidMsg},
		{"ADD m.ss.x :one", nestedPathInvalidMsg},
		{"ADD nope[0] :one", nestedPathInvalidMsg},
		{"ADD m[0] :one", nestedPathInvalidMsg},
		// A target of the wrong type.
		{"DELETE m.ns :xSet", nestedBadTypeMsg},
		{"DELETE m.n :xSet", nestedBadTypeMsg},
		{"DELETE m.s :xSet", nestedBadTypeMsg},
		{"DELETE l[1] :xSet", nestedBadTypeMsg},
		{"DELETE l[0] :oneSet", nestedBadTypeMsg},
		{"ADD m.ss :threeSet", nestedBadTypeMsg},
		{"ADD m.ns :zSet", nestedBadTypeMsg},
		{"ADD m.n :zSet", nestedBadTypeMsg},
		{"ADD m.ss :one", nestedBadTypeMsg},
		{"ADD m.s :one", nestedBadTypeMsg},
		{"ADD m.s :zSet", nestedBadTypeMsg},
		{"ADD l[1] :zSet", nestedBadTypeMsg},
		{"ADD l[0] :one", nestedBadTypeMsg},
		{"ADD l[3] :one", nestedBadTypeMsg},
		// An operand that is not a set (or a number for ADD) wins over a missing path.
		{"ADD m.ss :val", fmt.Sprintf(nestedAddOperandMsg, "STRING")},
		{"ADD m.new :x", fmt.Sprintf(nestedAddOperandMsg, "BOOLEAN")},
		{"ADD nope.x :val", fmt.Sprintf(nestedAddOperandMsg, "STRING")},
		{"DELETE nope.x :val", fmt.Sprintf(nestedDeleteOperandMsg, "STRING")},
		{"DELETE m.nope :two", fmt.Sprintf(nestedDeleteOperandMsg, "NUMBER")},
	}

	for _, tt := range tests {
		t.Run(tt.update, func(t *testing.T) {
			env := startNestedUpdateEnv(t)

			errObj, ok := testEvalUpdate(t, tt.update, env).(*Error)
			if !ok {
				t.Fatalf("expected an error")
			}

			if got, _, _ := strings.Cut(errObj.Message, " | "); got != tt.want {
				t.Errorf("wrong message.\n got=%q\nwant=%q", got, tt.want)
			}

			if got := nestedInspect(env.Get("m")); got != nestedWantM {
				t.Errorf("m changed: got=%s", got)
			}

			if got := nestedInspect(env.Get("l")); got != nestedWantL {
				t.Errorf("l changed: got=%s", got)
			}
		})
	}
}

// TestEvalNestedDeleteAddWithAliases resolves #name placeholders on nested paths.
func TestEvalNestedDeleteAddWithAliases(t *testing.T) {
	env := startNestedUpdateEnv(t)
	env.Aliases["#m"] = "m"
	env.Aliases["#ss"] = "ss"
	env.Aliases["#n"] = "n"
	env.Aliases["#nope"] = "nope"

	for _, update := range []string{"DELETE #m.#ss :xSet", "ADD #m.#n :one", "DELETE #m.#nope :xSet"} {
		if result := testEvalUpdate(t, update, env); isError(result) {
			t.Fatalf("%s: unexpected error: %s", update, result.Inspect())
		}
	}

	if got, want := nestedInspect(env.Get("m")), "{n:2 ns:{1,2} s:a ss:{y}}"; got != want {
		t.Errorf("wrong m.\n got=%s\nwant=%s", got, want)
	}

	errObj, ok := testEvalUpdate(t, "DELETE #nope.#x :xSet", env).(*Error)
	if !ok {
		t.Fatalf("expected an error")
	}

	if !strings.HasPrefix(errObj.Message, nestedPathInvalidMsg+" | ") {
		t.Errorf("wrong message: %q", errObj.Message)
	}
}
