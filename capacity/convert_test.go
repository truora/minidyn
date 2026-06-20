package capacity

import (
	"testing"

	"github.com/truora/minidyn/types"
)

func TestSize_Scalar(t *testing.T) {
	// "id" (2) + S "hello" (5) = 7
	item := map[string]*types.Item{"id": {S: new("hello")}}

	if got := Size(item); got != 7 {
		t.Fatalf("Size = %d, want 7", got)
	}
}

func TestSize_NilAndEmpty(t *testing.T) {
	if got := Size(nil); got != 0 {
		t.Fatalf("Size(nil) = %d, want 0", got)
	}

	if got := Size(map[string]*types.Item{}); got != 0 {
		t.Fatalf("Size(empty) = %d, want 0", got)
	}
}

func TestSize_AllShapes(t *testing.T) {
	b := true
	n := "5"

	item := map[string]*types.Item{
		"s":    {S: new("x")},
		"n":    {N: &n},
		"b":    {B: []byte{1, 2, 3}},
		"bool": {BOOL: &b},
		"null": {NULL: &b},
		"ss":   {SS: []*string{new("a"), new("bb")}},
		"ns":   {NS: []*string{new("1"), new("10")}},
		"l":    {L: []*types.Item{{N: &n}}},
		"m":    {M: map[string]*types.Item{"a": {S: new("x")}}},
	}

	if got := Size(item); got <= 0 {
		t.Fatalf("Size = %d, want > 0", got)
	}
}

func TestSize_BinarySet(t *testing.T) {
	// "bs" (2) + BS{[]byte(3 bytes), base64 "AAEC" -> 3 bytes} = 2 + 6 = 8
	item := map[string]*types.Item{"bs": {BS: [][]byte{{1, 2, 3}, {0, 1, 2}}}}

	if got := Size(item); got != 8 {
		t.Fatalf("Size = %d, want 8", got)
	}
}

func TestSumSize(t *testing.T) {
	items := []map[string]*types.Item{
		{"id": {S: new("hello")}}, // 7
		{"id": {S: new("hi")}},    // 2 + 2 = 4
	}

	if got := SumSize(items); got != 11 {
		t.Fatalf("SumSize = %d, want 11", got)
	}

	if got := SumSize(nil); got != 0 {
		t.Fatalf("SumSize(nil) = %d, want 0", got)
	}
}

func TestSize_NilAttributeAndEmptyItem(t *testing.T) {
	// A nil attribute value and an all-zero item must not panic; they size as NULL (1 byte).
	item := map[string]*types.Item{"a": nil, "b": {}}

	if got := Size(item); got <= 0 {
		t.Fatalf("Size = %d, want > 0", got)
	}
}
