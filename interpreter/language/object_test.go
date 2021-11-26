package language

import (
	"testing"
)

func TestNumberInspect(t *testing.T) {
	n := Number{Value: 1.0}
	if n.Inspect() != "1" {
		t.Fatalf("not equal actual=%s expected=%s", n.Inspect(), "1")
	}
}

func TestBooleanInspect(t *testing.T) {
	boolean := Boolean{Value: true}
	if boolean.Inspect() != "true" {
		t.Fatalf("not equal actual=%s expected=%s", boolean.Inspect(), "true")
	}
}

func TestNullInspect(t *testing.T) {
	null := Null{}
	if null.Inspect() != "null" {
		t.Fatalf("not equal actual=%s expected=%s", null.Inspect(), "null")
	}
}

func TestBinaryInspect(t *testing.T) {
	bin := Binary{Value: []byte{'1'}}
	if bin.Inspect() != "[49]" {
		t.Fatalf("not equal actual=%s expected=%s", bin.Inspect(), "[49]")
	}
}

func TestBinaryContains(t *testing.T) {
	bin := Binary{Value: []byte("hello")}
	if !bin.Contains(&Binary{Value: []byte("ell")}) {
		t.Fatalf("should be true")
	}

	if bin.Contains(&Binary{Value: []byte("c")}) {
		t.Fatalf("should be false")
	}

	if bin.Contains(&String{Value: "c"}) {
		t.Fatalf("should be false")
	}
}

func TestStringInspect(t *testing.T) {
	str := String{Value: "hello"}
	if str.Inspect() != "hello" {
		t.Fatalf("not equal actual=%s expected=%s", str.Inspect(), "hello")
	}
}

func TestStringContains(t *testing.T) {
	str := String{Value: "hello"}
	if !str.Contains(&String{Value: "ell"}) {
		t.Fatalf("should be true")
	}

	if str.Contains(&String{Value: "c"}) {
		t.Fatalf("should be false")
	}

	if str.Contains(&Number{Value: 10}) {
		t.Fatalf("should be false")
	}
}

func TestMapInspect(t *testing.T) {
	str := Map{Value: map[string]Object{
		":a": TRUE,
		":b": FALSE,
		":m": &Map{
			Value: map[string]Object{
				":x": TRUE,
				":y": FALSE,
			},
		},
	}}
	expected := "{\n\t\":a\" : true<BOOL>,\n\t\":b\" : false<BOOL>,\n\t\":m\" : {\n\t\t\":x\" : true<BOOL>,\n\t\t\":y\" : false<BOOL>,\n\t}<M>,\n}"

	if str.Inspect() != expected {
		t.Fatalf("not equal actual=%q expected=%q", str.Inspect(), expected)
	}
}

func TestListInspect(t *testing.T) {
	str := List{
		Value: []Object{
			&String{Value: "Cookies"}, &String{Value: "Coffee"}, &Number{Value: 3.14159},
		},
	}

	expected := "[ Cookies<S> Coffee<S> 3.14159<N> ]"
	if str.Inspect() != expected {
		t.Fatalf("not equal actual=%s expected=%s", str.Inspect(), expected)
	}
}

func TestListContains(t *testing.T) {
	list := List{
		Value: []Object{
			&String{Value: "Cookies"}, &String{Value: "Coffee"}, &Number{Value: 3.14159},
		},
	}

	if !list.Contains(&String{Value: "Cookies"}) {
		t.Fatalf("should be true")
	}

	if !list.Contains(&Number{Value: 3.14159}) {
		t.Fatalf("should be true")
	}

	if list.Contains(&String{Value: "Orange"}) {
		t.Fatalf("should be false")
	}

	if list.Contains(&List{Value: []Object{&String{Value: "Cookies"}}}) {
		t.Fatalf("should be true")
	}
}

func TestStringSetInspect(t *testing.T) {
	strSet := StringSet{
		Value: map[string]bool{
			"Cookies": true,
			"Coffee":  true,
		},
	}

	expected := "[ Coffee Cookies ]<SS>"
	if strSet.Inspect() != expected {
		t.Fatalf("not equal actual=%s expected=%s", strSet.Inspect(), expected)
	}
}

func TestStringSetContains(t *testing.T) {
	strSet := StringSet{
		Value: map[string]bool{
			"Cookies": true,
			"Coffee":  true,
		},
	}

	if !strSet.Contains(&String{Value: "Cookies"}) {
		t.Fatalf("should be true")
	}

	if strSet.Contains(&String{Value: "Orange"}) {
		t.Fatalf("should be false")
	}

	if !strSet.Contains(&StringSet{Value: map[string]bool{"Cookies": true}}) {
		t.Fatalf("should be true")
	}

	if strSet.Contains(&StringSet{Value: map[string]bool{"Orange": true}}) {
		t.Fatalf("should be false")
	}

	if strSet.Contains(&Number{Value: 10}) {
		t.Fatalf("should be false")
	}
}

func TestBinarySetInspect(t *testing.T) {
	strSet := BinarySet{
		Value: [][]byte{
			[]byte("Cookies"),
			[]byte("Coffee"),
		},
	}

	expected := "[ [67 111 111 107 105 101 115] [67 111 102 102 101 101] ]<BS>"
	if strSet.Inspect() != expected {
		t.Fatalf("not equal actual=%s expected=%s", strSet.Inspect(), expected)
	}
}

func TestBinarySetContains(t *testing.T) {
	strSet := BinarySet{
		Value: [][]byte{
			[]byte("Cookies"),
			[]byte("Coffee"),
		},
	}

	if !strSet.Contains(&Binary{Value: []byte("Cookies")}) {
		t.Fatalf("should be true")
	}

	if strSet.Contains(&Binary{Value: []byte("Orange")}) {
		t.Fatalf("should be false")
	}

	if !strSet.Contains(&BinarySet{Value: [][]byte{[]byte("Cookies")}}) {
		t.Fatalf("should be true")
	}

	if strSet.Contains(&BinarySet{Value: [][]byte{[]byte("Orange")}}) {
		t.Fatalf("should be false")
	}

	if strSet.Contains(&Number{Value: 10}) {
		t.Fatalf("should be false")
	}
}

func TestNumberSetInspect(t *testing.T) {
	strSet := NumberSet{
		Value: map[float64]bool{
			1: true,
			2: true,
		},
	}

	expected := "[ 1 2 ]<NS>"
	if strSet.Inspect() != expected {
		t.Fatalf("not equal actual=%s expected=%s", strSet.Inspect(), expected)
	}
}

func TestNumberSetContains(t *testing.T) {
	strSet := NumberSet{
		Value: map[float64]bool{
			1: true,
			2: true,
		},
	}

	if !strSet.Contains(&Number{Value: 1}) {
		t.Fatalf("should be true")
	}

	if strSet.Contains(&Number{Value: 3}) {
		t.Fatalf("should be false")
	}

	if !strSet.Contains(&NumberSet{Value: map[float64]bool{1: true}}) {
		t.Fatalf("should be true")
	}

	if strSet.Contains(&NumberSet{Value: map[float64]bool{3: true}}) {
		t.Fatalf("should be false")
	}

	if strSet.Contains(&String{Value: "1"}) {
		t.Fatalf("should be false")
	}
}

func BenchmarkStringInspect(b *testing.B) {
	str := String{Value: "hello"}

	for n := 0; n < b.N; n++ {
		if str.Inspect() != "hello" {
			b.Fatalf("not equal actual=%s expected=%s", str.Inspect(), "hello")
		}
	}
}
