package docsize

import "testing"

func TestCalculateDocumentSize_Scalars(t *testing.T) {
	// attribute name "S" (1) + S value "hello" (5) = 6
	got, err := CalculateDocumentSize(map[string]any{"S": map[string]any{"S": "hello"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 6 {
		t.Fatalf("got=%d want 6", got)
	}
}

func TestCalculateDocumentSize_NumberAndBinary(t *testing.T) {
	// "n" (1) + N "123" (3 digits -> ceil(3/2)=2 blocks +1 = 3) = 4
	got, err := CalculateDocumentSize(map[string]any{"n": map[string]any{"N": "123"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 4 {
		t.Fatalf("number: got=%d want 4", got)
	}

	// "b" (1) + B base64 "AAEC" -> 3 decoded bytes = 4
	got, err = CalculateDocumentSize(map[string]any{"b": map[string]any{"B": "AAEC"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 4 {
		t.Fatalf("binary: got=%d want 4", got)
	}
}

func TestCalculateDocumentSize_BoolNull(t *testing.T) {
	// "b" (1) + BOOL (1) + "n" (1) + NULL (1) = 4
	doc := map[string]any{
		"b": map[string]any{"BOOL": true},
		"n": map[string]any{"NULL": true},
	}

	got, err := CalculateDocumentSize(doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 4 {
		t.Fatalf("got=%d want 4", got)
	}
}

func TestCalculateDocumentSize_NestedMapList(t *testing.T) {
	// "m"(1) + M{compositeOverhead 3 + key "a"(1) + S "x"(1) + nestedOverhead 1} = 1 + 6 = 7
	// "l"(1) + L{compositeOverhead 3 + N "5"(2) + nestedOverhead 1}              = 1 + 6 = 7
	doc := map[string]any{
		"m": map[string]any{"M": map[string]any{"a": map[string]any{"S": "x"}}},
		"l": map[string]any{"L": []any{map[string]any{"N": "5"}}},
	}

	got, err := CalculateDocumentSize(doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 14 {
		t.Fatalf("got=%d want 14", got)
	}
}

func TestCalculateDocumentSize_Sets(t *testing.T) {
	// "ss"(2) + SS{"a"(1)+"bb"(2)=3}            = 5
	// "ns"(2) + NS{"1"(2)+"10"(2)=4}            = 6
	// "bs"(2) + BS{base64 "AAEC" -> 3 bytes}    = 5
	doc := map[string]any{
		"ss": map[string]any{"SS": []any{"a", "bb"}},
		"ns": map[string]any{"NS": []any{"1", "10"}},
		"bs": map[string]any{"BS": []any{"AAEC"}},
	}

	got, err := CalculateDocumentSize(doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 16 {
		t.Fatalf("got=%d want 16", got)
	}
}

func TestCalculateDocumentSize_DocumentSet(t *testing.T) {
	// "s"(1) + String set {"a"(1)+"bb"(2)=3}        = 4
	// "n"(1) + Number set {1 -> "1"(2), 22 -> "22"(2)} = 5
	doc := map[string]any{
		"s": DocumentSet{Type: "String", Values: []any{"a", "bb"}},
		"n": &DocumentSet{Type: "Number", Values: []any{1, 22}},
	}

	got, err := CalculateDocumentSize(doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 9 {
		t.Fatalf("got=%d want 9", got)
	}
}

func TestCalculateDocumentSize_Errors(t *testing.T) {
	cases := []struct {
		name string
		doc  map[string]any
	}{
		{"nil doc", nil},
		{"non-attribute value", map[string]any{"x": 123}},
		{"two type keys", map[string]any{"x": map[string]any{"S": "a", "N": "1"}}},
		{"unknown type key", map[string]any{"x": map[string]any{"WUT": "a"}}},
		{"bad nested map value", map[string]any{"x": map[string]any{"M": map[string]any{"k": 5}}}},
		{"nil DocumentSet pointer", map[string]any{"x": (*DocumentSet)(nil)}},
	}

	for _, c := range cases {
		if _, err := CalculateDocumentSize(c.doc); err == nil {
			t.Errorf("%s: expected error, got nil", c.name)
		}
	}
}
