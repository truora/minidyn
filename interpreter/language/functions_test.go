package language

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/truora/minidyn/types"
)

func TestFunctionInspect(t *testing.T) {
	fn := Function{
		Name:  "attribute_exists",
		Value: attributeExists,
	}

	if fn.Inspect() != "attribute_exists" {
		t.Fatalf("not equal actual=%s expected=%s", fn.Inspect(), "attribute_exists")
	}

	if !cmp.Equal(fn.ToDynamoDB(), types.Item{}) {
		t.Fatalf("not empty actual=%v", fn.ToDynamoDB())
	}
}

func TestAttributeExists(t *testing.T) {
	cases := map[string]struct {
		path     Object
		expected Object
	}{
		"string_attribute":  {&String{Value: "hello"}, TRUE},
		"explicit_null":     {&Null{}, TRUE},
		"missing_attribute": {UNDEFINED, FALSE},
		"nil_path":          {nil, FALSE},
	}

	for name, tc := range cases {
		got := attributeExists(tc.path)
		if got != tc.expected {
			t.Fatalf("%s: expected=%s got=%s", name, tc.expected.Inspect(), got.Inspect())
		}
	}
}

func TestAttributeNotExists(t *testing.T) {
	cases := map[string]struct {
		path     Object
		expected Object
	}{
		"string_attribute":  {&String{Value: "hello"}, FALSE},
		"explicit_null":     {&Null{}, FALSE},
		"missing_attribute": {UNDEFINED, TRUE},
		"nil_path":          {nil, TRUE},
	}

	for name, tc := range cases {
		got := attributeNotExists(tc.path)
		if got != tc.expected {
			t.Fatalf("%s: expected=%s got=%s", name, tc.expected.Inspect(), got.Inspect())
		}
	}
}

func TestAttributeType(t *testing.T) {
	cases := map[string]struct {
		path     Object
		typeArg  Object
		expected Object
	}{
		"string_matches_S":            {&String{Value: "hello"}, &String{Value: "S"}, TRUE},
		"string_does_not_match":       {&String{Value: "hello"}, &String{Value: "N"}, FALSE},
		"null_value_matches_NULL":     {&Null{}, &String{Value: "NULL"}, TRUE},
		"missing_does_not_match_NULL": {UNDEFINED, &String{Value: "NULL"}, FALSE},
		"missing_does_not_match_S":    {UNDEFINED, &String{Value: "S"}, FALSE},
	}

	for name, tc := range cases {
		got := attributeType(tc.path, tc.typeArg)
		if got != tc.expected {
			t.Fatalf("%s: expected=%s got=%s", name, tc.expected.Inspect(), got.Inspect())
		}
	}

	invalidType := &String{Value: "TYPE"}
	got := attributeType(&String{Value: "hello"}, invalidType)

	if got.Type() != ObjectTypeError || got.Inspect() != "ERROR: invalid type TYPE" {
		t.Fatalf("expect invalid type error, got=%s %s", got.Type(), got.Inspect())
	}
}

func TestBeginsWithSuccess(t *testing.T) {
	str := &String{Value: "Beto Gomez"}
	expectedBegin := &String{Value: "Bet"}

	begins := beginsWith(str, expectedBegin)
	if begins.Type() == ObjectTypeBoolean && begins.Inspect() != "true" {
		t.Fatal("value should be true")
	}

	bin := &Binary{Value: []byte{'h', 'e', 'l', 'l', 'o'}}
	expectedBinary := &Binary{Value: []byte{'h', 'e'}}

	begins = beginsWith(bin, expectedBinary)
	if begins.Type() == ObjectTypeBoolean && begins.Inspect() != "true" {
		t.Fatal("value should be true")
	}
}

func TestBeginsWithFailure(t *testing.T) {
	str := &String{Value: "Beto Gomez"}
	expectedBegin := &String{Value: "Mar"}

	begins := beginsWith(str, expectedBegin)
	if begins.Type() == ObjectTypeBoolean && begins.Inspect() != "false" {
		t.Fatal("value should be false")
	}

	bin := &Binary{Value: []byte{'h', 'e', 'l', 'l', 'o'}}
	expectedBinary := &Binary{Value: []byte{'j', 'o'}}

	begins = beginsWith(bin, expectedBinary)
	if begins.Type() == ObjectTypeBoolean && begins.Inspect() != "false" {
		t.Fatal("value should be true")
	}
}

func TestBeginsWithError(t *testing.T) {
	str := &String{Value: "Beto Gomez"}
	expectedBinary := &Binary{Value: []byte{'j', 'o'}}

	begins := beginsWith(str, expectedBinary)
	if begins.Type() != ObjectTypeError || begins.Inspect() != "ERROR: invalid substr type B" {
		t.Fatalf("expect invalid type error, got=%s %s", begins.Type(), begins.Inspect())
	}

	num := &Number{Value: 5}
	begins = beginsWith(num, expectedBinary)

	if begins.Type() != ObjectTypeError || begins.Inspect() != "ERROR: invalid type N" {
		t.Fatalf("expect invalid type error, got=%s %s", begins.Type(), begins.Inspect())
	}
}

func TestContainsSuccess(t *testing.T) {
	str := &String{Value: "Beto Gomez"}
	expectedContains := &String{Value: "ome"}

	contained := contains(str, expectedContains)
	if contained.Type() == ObjectTypeBoolean && contained.Inspect() != "true" {
		t.Fatal("value should be true")
	}

	bin := &Binary{Value: []byte{'h', 'e', 'l', 'l', 'o'}}
	expectedBinary := &Binary{Value: []byte{'e', 'l'}}

	contained = contains(bin, expectedBinary)
	if contained.Type() == ObjectTypeBoolean && contained.Inspect() != "true" {
		t.Fatal("value should be true")
	}
}

func TestContainsWithFailure(t *testing.T) {
	str := &String{Value: "Beto Gomez"}
	expectedContains := &String{Value: "Mar"}

	contained := contains(str, expectedContains)
	if contained.Type() == ObjectTypeBoolean && contained.Inspect() != "false" {
		t.Fatal("value should be false")
	}

	bin := &Binary{Value: []byte{'h', 'e', 'l', 'l', 'o'}}
	expectedBinary := &Binary{Value: []byte{'j', 'o'}}

	contained = contains(bin, expectedBinary)
	if contained.Type() == ObjectTypeBoolean && contained.Inspect() != "false" {
		t.Fatal("value should be true")
	}
}

func TestContainsWithError(t *testing.T) {
	str := &String{Value: "Beto Gomez"}
	expectedBinary := &Binary{Value: []byte{'j', 'o'}}

	contained := contains(str, expectedBinary)
	if contained.Type() != ObjectTypeError || contained.Inspect() != "ERROR: contains is not supported for path=S operand=B" {
		t.Fatalf("expect invalid type error, got=%s %q", contained.Type(), contained.Inspect())
	}

	num := &Number{Value: 5}
	contained = contains(num, expectedBinary)

	if contained.Type() != ObjectTypeError || contained.Inspect() != "ERROR: contains is not supported for path=N" {
		t.Fatalf("expect invalid type error, got=%s %q", contained.Type(), contained.Inspect())
	}
}

func TestObjectSize(t *testing.T) {
	str := String{Value: "hello"}
	expected := "5"

	size := objectSize(&str)
	if size.Inspect() != expected {
		t.Fatalf("size mismatch expected=%s, actual=%s", expected, size.Inspect())
	}

	bin := Binary{Value: []byte{'h', 'e', 'l', 'l', 'o'}}

	size = objectSize(&bin)
	if size.Inspect() != expected {
		t.Fatalf("size mismatch expected=%s, actual=%s", expected, size.Inspect())
	}

	list := &List{Value: []Object{&String{Value: "a"}, &String{Value: "b"}}}
	size = objectSize(list)
	if size.Inspect() != "2" {
		t.Fatalf("list size expected=2, actual=%s", size.Inspect())
	}

	m := &Map{Value: map[string]Object{"k": &String{Value: "v"}}}
	size = objectSize(m)
	if size.Inspect() != "1" {
		t.Fatalf("map size expected=1, actual=%s", size.Inspect())
	}

	ss := &StringSet{Value: map[string]bool{"a": true, "b": true}}
	size = objectSize(ss)
	if size.Inspect() != "2" {
		t.Fatalf("string set size expected=2, actual=%s", size.Inspect())
	}

	ns := &NumberSet{Value: map[float64]bool{1: true, 2: true}}
	size = objectSize(ns)
	if size.Inspect() != "2" {
		t.Fatalf("number set size expected=2, actual=%s", size.Inspect())
	}

	bs := &BinarySet{Value: [][]byte{{1}, {2}}}
	size = objectSize(bs)
	if size.Inspect() != "2" {
		t.Fatalf("binary set size expected=2, actual=%s", size.Inspect())
	}

	size = objectSize(TRUE)
	if !isError(size) {
		t.Fatalf("error expected: %s", size.Inspect())
	}
}

func TestIfNotExists(t *testing.T) {
	str := String{Value: "hello"}
	val := ifNotExists(UNDEFINED, &str)

	if str.Inspect() != val.Inspect() {
		t.Fatalf("expected=%s, actual=%s", str, val.Inspect())
	}

	val = ifNotExists(&str, UNDEFINED)

	if str.Inspect() != val.Inspect() {
		t.Fatalf("expected=%s, actual=%s", str, val.Inspect())
	}
}

func TestListAppend(t *testing.T) {
	testCases := map[string]struct {
		arg1   Object
		arg2   Object
		result Object
	}{
		`success`: {
			arg1:   &List{Value: []Object{&String{Value: "a"}}},
			arg2:   &List{Value: []Object{&String{Value: "b"}}},
			result: &List{Value: []Object{&String{Value: "a"}, &String{Value: "b"}}},
		},
		`arg1_no_list`: {
			arg1:   &String{Value: "a"},
			arg2:   &List{Value: []Object{&String{Value: "b"}}},
			result: &Error{Message: "list_append is not supported for list1=S"},
		},
		`arg2_no_list`: {
			arg1:   &List{Value: []Object{&String{Value: "a"}}},
			arg2:   &String{Value: "b"},
			result: &Error{Message: "list_append is not supported for list2=S"},
		},
	}

	for _, tt := range testCases {
		r := listAppend(tt.arg1, tt.arg2)
		if tt.result.Inspect() != r.Inspect() {
			t.Fatalf("expected=%s, actual=%s", tt.result.Inspect(), r.Inspect())
		}
	}
}

func BenchmarkFunctionInspect(b *testing.B) {
	fn := Function{
		Name:  "attribute_exists",
		Value: attributeExists,
	}

	for n := 0; n < b.N; n++ {
		if fn.Inspect() != "attribute_exists" {
			b.Fatalf("not equal actual=%s expected=%s", fn.Inspect(), "attribute_exists")
		}
	}
}
