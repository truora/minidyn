package language

import (
	"bytes"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/truora/minidyn/types"
)

func TestNumberInspect(t *testing.T) {
	n := Number{Value: 1.0}
	if n.Inspect() != "1" {
		t.Fatalf("not equal actual=%s expected=%s", n.Inspect(), "1")
	}
}

func TestNumberAdd(t *testing.T) {
	n := Number{Value: 1.0}

	obj := n.Add(&Number{Value: 1.0})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if n.Value != 2 {
		t.Fatalf("result object should be 2, got=%q", n.Inspect())
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

func TestListAdd(t *testing.T) {
	l := List{Value: []Object{&String{Value: "Cookies"}}}

	obj := l.Add(&String{Value: "Orange"})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(l.Value) != 2 {
		t.Fatalf("result object should be 2 elements, got=%q", l.Inspect())
	}

	obj = l.Add(&List{Value: []Object{&String{Value: "Cookies"}}})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(l.Value) != 3 {
		t.Fatalf("result object should be 3 elements, got=%q", l.Inspect())
	}
}

func TestListRemove(t *testing.T) {
	l := List{Value: []Object{&String{Value: "Cookies"}, &String{Value: "Orange"}}}

	obj := l.Remove(1)
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(l.Value) != 2 {
		t.Fatalf("result object should be 2 elements, got=%q", l.Inspect())
	}

	obj = l.Remove(0)
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(l.Value) != 2 {
		t.Fatalf("result object should be 2 elements, got=%q", l.Inspect())
	}

	l.Compact()

	if len(l.Value) != 0 {
		t.Fatalf("result object should be empty, got=%q", l.Inspect())
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

func TestStringSetAdd(t *testing.T) {
	ss := StringSet{Value: map[string]bool{"Cookies": true}}

	obj := ss.Add(&String{Value: "Orange"})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(ss.Value) != 2 {
		t.Fatalf("result object should be 2 elements, got=%q", ss.Inspect())
	}

	obj = ss.Add(&StringSet{Value: map[string]bool{"Milk": true}})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(ss.Value) != 3 {
		t.Fatalf("result object should be 3 elements, got=%q", ss.Inspect())
	}
}

func TestStringSetDelete(t *testing.T) {
	ss := StringSet{Value: map[string]bool{"Cookies": true, "Orange": true, "Milk": true}}

	obj := ss.Delete(&String{Value: "Orange"})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(ss.Value) != 2 {
		t.Fatalf("result object should have 2 elements, got=%q", ss.Inspect())
	}

	obj = ss.Delete(&StringSet{Value: map[string]bool{"Milk": true, "Cookies": true}})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(ss.Value) != 0 {
		t.Fatalf("result object should be empty SS, got=%q", ss.Inspect())
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

func TestBinaryAdd(t *testing.T) {
	bs := BinarySet{
		Value: [][]byte{
			[]byte("Cookies"),
		},
	}

	obj := bs.Add(&Binary{Value: []byte("Coffee")})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(bs.Value) != 2 {
		t.Fatalf("result object should have 2 elements, got=%q", bs.Inspect())
	}

	more := &BinarySet{
		Value: [][]byte{
			[]byte("Cookies"),
			[]byte("Milk"),
		},
	}

	obj = bs.Add(more)
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(bs.Value) != 3 {
		t.Fatalf("result object should have 3 elements, got=%q", bs.Inspect())
	}
}

func TestBinarySetRemove(t *testing.T) {
	bs := BinarySet{
		Value: [][]byte{
			[]byte("Cookies"),
			[]byte("Coffee"),
			[]byte("Milk"),
		},
	}

	obj := bs.Delete(&Binary{Value: []byte("Coffee")})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(bs.Value) != 2 {
		t.Fatalf("result object should have 2 elements, got=%q", bs.Inspect())
	}

	rest := &BinarySet{
		Value: [][]byte{
			[]byte("Cookies"),
			[]byte("Milk"),
		},
	}

	obj = bs.Delete(rest)
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(bs.Value) != 0 {
		t.Fatalf("result object should be empty elements, got=%q", bs.Inspect())
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

func TestNumberSetAdd(t *testing.T) {
	ns := NumberSet{Value: map[float64]bool{1: true}}

	obj := ns.Add(&Number{Value: 2})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(ns.Value) != 2 {
		t.Fatalf("result object should be 2 elements, got=%q", ns.Inspect())
	}

	obj = ns.Add(&NumberSet{Value: map[float64]bool{3: true}})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(ns.Value) != 3 {
		t.Fatalf("result object should be 3 elements, got=%q", ns.Inspect())
	}
}

func TestNumberSetDelete(t *testing.T) {
	ns := NumberSet{Value: map[float64]bool{1: true, 2: true, 3: true}}

	obj := ns.Delete(&Number{Value: 2})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(ns.Value) != 2 {
		t.Fatalf("result object should have 2 elements, got=%q", ns.Inspect())
	}

	obj = ns.Delete(&NumberSet{Value: map[float64]bool{1: true, 3: true}})
	if obj != UNDEFINED {
		t.Fatalf("return object should be NULL, got=%q", obj.Inspect())
	}

	if len(ns.Value) != 0 {
		t.Fatalf("result object should be empty, got=%q", ns.Inspect())
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

func TestToDynamo(t *testing.T) {
	num := Number{Value: 3}
	dNum := num.ToDynamoDB()

	if num.Inspect() != types.StringValue(dNum.N) {
		t.Errorf("expected number %s got=%q", num.Inspect(), types.StringValue(dNum.N))
	}

	boolean := Boolean{Value: true}
	dBoolean := boolean.ToDynamoDB()

	if boolean.Value != *dBoolean.BOOL {
		t.Errorf("expected bool %s got=%t", num.Inspect(), *dBoolean.BOOL)
	}

	binary := Binary{Value: []byte("Hello")}
	dBinary := binary.ToDynamoDB()

	if !bytes.Equal(binary.Value, dBinary.B) {
		t.Errorf("expected binary %s got=%s", binary.Value, dBinary.B)
	}

	null := Null{}
	dNull := null.ToDynamoDB()

	if !*dNull.NULL {
		t.Errorf("expected bool %t got=%t", true, *dNull.NULL)
	}

	err := Error{Message: "Some Error"}
	dError := err.ToDynamoDB()

	emptyItem := types.Item{}

	if !cmp.Equal(dError, emptyItem) {
		t.Errorf("expected error to be empty, got=%v", dError)
	}

	mapN := Map{
		Value: map[string]Object{"Key": &String{Value: "Value"}},
	}
	dMap := mapN.ToDynamoDB()

	if types.StringValue(mapN.Value["Key"].ToDynamoDB().S) != types.StringValue(dMap.M["Key"].S) {
		t.Errorf("expected value on Key to be %s got=%s", types.StringValue(mapN.Value["Key"].ToDynamoDB().S), types.StringValue(dMap.M["Key"].S))
	}

	list := List{Value: []Object{&String{Value: "Cookies"}}}
	dList := list.ToDynamoDB()

	if types.StringValue(list.Value[0].ToDynamoDB().S) != types.StringValue(dList.L[0].S) {
		t.Errorf("expected item to be %s got=%s", types.StringValue(list.Value[0].ToDynamoDB().S), types.StringValue(dList.L[0].S))
	}

	ss := StringSet{Value: map[string]bool{"Cookies": true}}
	dSS := ss.ToDynamoDB()

	if "Cookies" != types.StringValue(dSS.SS[0]) {
		t.Errorf("expected item to be %s got=%s", "Cookies", types.StringValue(dSS.SS[0]))
	}

	bs := &BinarySet{Value: [][]byte{[]byte("a"), []byte("c"), []byte("c")}}
	dBs := bs.ToDynamoDB()

	if !bytes.Equal(bs.Value[0], dBs.BS[0]) {
		t.Errorf("binary set item to be %s got=%s", bs.Value[0], dBs.BS[0])
	}

	nm := NumberSet{Value: map[float64]bool{1: true}}
	dNm := nm.ToDynamoDB()

	if "1" != types.StringValue(dNm.NS[0]) {
		t.Errorf("expected item to be %s got=%s", "1", types.StringValue(dNm.NS[0]))
	}
}

func TestGetList(t *testing.T) {
	list := List{Value: []Object{nil}}
	res := list.Get(0)

	if res != UNDEFINED {
		t.Errorf("expected item to be NULL got=%s", res)
	}
}
