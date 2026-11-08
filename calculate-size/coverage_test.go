package docsize

import (
	"encoding/json"
	"testing"
)

func TestCalculateDocumentSize_DocumentSetNumbers(t *testing.T) {
	// Number set with mixed Go numeric kinds exercises toFloat64 + trimFloatJSON:
	//   1.5        -> "1.5" -> 3
	//   int64(22)  -> "22"  -> 2
	//   float32(2) -> "2"   -> 2
	//   json.Number("7")    -> 2
	// sum = 9, plus name "n" (1) = 10
	doc := map[string]any{
		"n": DocumentSet{Type: "Number", Values: []any{1.5, int64(22), float32(2), json.Number("7")}},
	}

	got, err := CalculateDocumentSize(doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 10 {
		t.Fatalf("got=%d want 10", got)
	}
}

func TestCalculateDocumentSize_DocumentSetBinary(t *testing.T) {
	// Binary set with a raw []byte (3 bytes) and a base64 string ("AAEC" -> 3 bytes)
	// sum = 6, plus name "b" (1) = 7
	doc := map[string]any{
		"b": DocumentSet{Type: "Binary", Values: []any{[]byte{1, 2, 3}, "AAEC"}},
	}

	got, err := CalculateDocumentSize(doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 7 {
		t.Fatalf("got=%d want 7", got)
	}
}

func TestCalculateDocumentSize_DocumentSetUnknownType(t *testing.T) {
	// Unknown set type contributes 0; only the name "x" (1) is counted.
	doc := map[string]any{
		"x": DocumentSet{Type: "Mystery", Values: []any{"a"}},
	}

	got, err := CalculateDocumentSize(doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 1 {
		t.Fatalf("got=%d want 1", got)
	}
}

func TestCalculateDocumentSize_NumberJSONNumber(t *testing.T) {
	// N value provided as json.Number exercises coerceNumberString's json.Number path.
	// "12" -> 2, plus name "n" (1) = 3
	doc := map[string]any{"n": map[string]any{"N": json.Number("12")}}

	got, err := CalculateDocumentSize(doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got != 3 {
		t.Fatalf("got=%d want 3", got)
	}
}

func TestCalculateDocumentSize_NumberSetJSONNumber(t *testing.T) {
	// NS with json.Number elements exercises coerceNumberString in the NS branch.
	doc := map[string]any{"ns": map[string]any{"NS": []any{json.Number("1"), json.Number("100")}}}

	if _, err := CalculateDocumentSize(doc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCalculateDocumentSize_NumberErrors(t *testing.T) {
	// N with a non-string/non-json.Number value is rejected by coerceNumberString.
	if _, err := CalculateDocumentSize(map[string]any{"n": map[string]any{"N": 123}}); err == nil {
		t.Errorf("expected error for numeric N value")
	}

	// NS with a bad element is rejected.
	if _, err := CalculateDocumentSize(map[string]any{"ns": map[string]any{"NS": []any{true}}}); err == nil {
		t.Errorf("expected error for bad NS element")
	}
}

// strconv's shortest 'f' formatting never leaves trailing zeros, so no extra trimming is needed.
func TestTrimFloatJSON(t *testing.T) {
	cases := map[float64]string{
		0:     "0",
		42:    "42",
		100:   "100",
		1.5:   "1.5",
		-2.5:  "-2.5",
		0.1:   "0.1",
		1e21:  "1000000000000000000000",
		1e-07: "0.0000001",
	}

	for in, want := range cases {
		if got := trimFloatJSON(in); got != want {
			t.Errorf("trimFloatJSON(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestCalculateBinaryDecoded(t *testing.T) {
	if got := calculateBinaryDecoded([]byte{1, 2, 3}); got != 3 {
		t.Errorf("calculateBinaryDecoded([]byte) = %d, want 3", got)
	}

	if got := calculateBinaryDecoded("AAEC"); got != 3 {
		t.Errorf("calculateBinaryDecoded(base64) = %d, want 3", got)
	}

	if got := calculateBinaryDecoded("!!not base64!!"); got != 0 {
		t.Errorf("calculateBinaryDecoded(invalid base64) = %d, want 0", got)
	}

	if got := calculateBinaryDecoded(123); got != 0 {
		t.Errorf("calculateBinaryDecoded(int) = %d, want 0", got)
	}
}

func TestDynamoNumberSizeBytes_ZeroStripping(t *testing.T) {
	// Leading/trailing zero handling must not panic and stays within the 1..21 range.
	for _, in := range []string{"0.00012", "1200", "0.5", "100.00", "-0.0007"} {
		got := dynamoNumberSizeBytes(in)
		if got < 1 || got > 21 {
			t.Errorf("dynamoNumberSizeBytes(%q) = %d, want within [1,21]", in, got)
		}
	}
}
