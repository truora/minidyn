package interpreter

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/truora/minidyn/types"
)

const precisionDigits38 = "12345678901234567890123456789012345678"

func numItem(s string) *types.Item { return &types.Item{N: new(s)} }

func zeros(n int) string { return strings.Repeat("0", n) }

// A value that is only copied or projected keeps its 38 digits, at any depth. Other spellings
// of a number come back in DynamoDB's form: no leading or trailing zeros, no exponent.
func TestLanguageProject_numbersKeepPrecisionAndNormalize(t *testing.T) {
	t.Parallel()

	tests := []struct{ in, want string }{
		{precisionDigits38, precisionDigits38},
		{"-" + precisionDigits38, "-" + precisionDigits38},
		{"1.2345678901234567890123456789012345678", "1.2345678901234567890123456789012345678"},
		{"0.0000000000000000000000000000000000001", "0.0000000000000000000000000000000000001"},
		{"1.0", "1"},
		{"01", "1"},
		{"1e2", "100"},
		{"1E+2", "100"},
		{".5", "0.5"},
		{"-0", "0"},
		{"0.10", "0.1"},
		{"1e-2", "0.01"},
		{"1e125", "1" + zeros(125)},
	}

	for _, tt := range tests {
		item := map[string]*types.Item{
			"n":  numItem(tt.in),
			"l":  {L: []*types.Item{numItem(tt.in)}},
			"m":  {M: map[string]*types.Item{"x": numItem(tt.in)}},
			"ns": {NS: []*string{new(tt.in)}},
		}

		out, err := (&Language{}).Project(ProjectInput{Expression: "n, l, m, ns", Item: item})
		if err != nil {
			t.Fatalf("%s: %v", tt.in, err)
		}

		want := map[string]*types.Item{
			"n":  numItem(tt.want),
			"l":  {L: []*types.Item{numItem(tt.want)}},
			"m":  {M: map[string]*types.Item{"x": numItem(tt.want)}},
			"ns": {NS: []*string{new(tt.want)}},
		}
		if diff := cmp.Diff(want, out); diff != "" {
			t.Errorf("%s: %s", tt.in, diff)
		}
	}
}

// Update writes back every attribute it read, so the ones it does not change must keep their
// digits too, and so must a copy, list_append and if_not_exists result.
func TestLanguageUpdate_numbersKeepPrecision(t *testing.T) {
	t.Parallel()

	item := map[string]*types.Item{
		"id": numItem(precisionDigits38),
		"n":  numItem(precisionDigits38),
		"l":  {L: []*types.Item{numItem(precisionDigits38)}},
		"m":  {M: map[string]*types.Item{"x": numItem(precisionDigits38)}},
		"ns": {NS: []*string{new(precisionDigits38)}},
	}

	err := (&Language{}).Update(UpdateInput{
		Expression: "SET s = :s, c = n, ln = list_append(l, l), ie = if_not_exists(nope, n), mx = m.x",
		Item:       item,
		Attributes: map[string]*types.Item{":s": {S: new("v")}},
	})
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]*types.Item{
		"id": numItem(precisionDigits38),
		"n":  numItem(precisionDigits38),
		"l":  {L: []*types.Item{numItem(precisionDigits38)}},
		"m":  {M: map[string]*types.Item{"x": numItem(precisionDigits38)}},
		"ns": {NS: []*string{new(precisionDigits38)}},
		"s":  {S: new("v")},
		"c":  numItem(precisionDigits38),
		"ln": {L: []*types.Item{numItem(precisionDigits38), numItem(precisionDigits38)}},
		"ie": numItem(precisionDigits38),
		"mx": numItem(precisionDigits38),
	}
	if diff := cmp.Diff(want, item); diff != "" {
		t.Fatal(diff)
	}
}

// Sets of numbers keep 38 digits, tell 38-digit neighbours apart and drop duplicate spellings.
func TestLanguageUpdate_numberSetsKeepPrecision(t *testing.T) {
	t.Parallel()

	other := "12345678901234567890123456789012345679"
	item := map[string]*types.Item{"ns": {NS: []*string{new(precisionDigits38), new("1.0")}}}

	err := (&Language{}).Update(UpdateInput{
		Expression: "ADD ns :add DELETE ns :del",
		Item:       item,
		Attributes: map[string]*types.Item{
			":add": {NS: []*string{new(other), new("1")}},
			":del": {NS: []*string{new(precisionDigits38)}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	for _, v := range item["ns"].NS {
		got[*v] = true
	}

	if diff := cmp.Diff(map[string]bool{other: true, "1": true}, got); diff != "" {
		t.Fatal(diff)
	}
}

// Arithmetic is exact up to 38 significant digits, with the result written as DynamoDB writes it:
// no trailing zeros. A result of more digits is DynamoDB's precision error.
func TestLanguageUpdate_exactArithmetic(t *testing.T) {
	t.Parallel()

	const precisionErr = "DynamoDB only supports precision up to 38 digits"

	tests := []struct{ stored, expr, value, want, wantErr string }{
		{stored: precisionDigits38, expr: "SET n = n + :v", value: "1", want: "12345678901234567890123456789012345679"},
		{stored: precisionDigits38, expr: "SET n = n - :v", value: "1", want: "12345678901234567890123456789012345677"},
		{stored: precisionDigits38, expr: "ADD n :v", value: "1", want: "12345678901234567890123456789012345679"},
		{stored: "99999999999999999999999999999999999999", expr: "SET n = n + :v", value: "1", want: "1" + zeros(38)},
		{stored: "0.1", expr: "SET n = n + :v", value: "0.2", want: "0.3"},
		{stored: "0.3", expr: "SET n = n - :v", value: "0.1", want: "0.2"},
		{stored: "1", expr: "SET n = n + :v", value: "1e-37", want: "1.0000000000000000000000000000000000001"},
		{stored: "1.1", expr: "SET n = n + :v", value: "1.20", want: "2.3"},
		{stored: "1", expr: "SET n = n - :v", value: "3", want: "-2"},
		{stored: "-5", expr: "SET n = n - :v", value: "-7", want: "2"},
		{stored: "1.5", expr: "SET n = n + :v", value: "-1.5", want: "0"},
		{stored: "0", expr: "SET n = n + :v", value: "-0", want: "0"},
		{stored: "1e125", expr: "SET n = n + :v", value: "1e125", want: "2" + zeros(125)},
		{stored: "1e-130", expr: "SET n = n - :v", value: "1e-130", want: "0"},
		{stored: "1e100", expr: "SET n = n + :v", value: "1e-100", wantErr: precisionErr},
		{stored: precisionDigits38, expr: "SET n = n + :v", value: "0.5", wantErr: precisionErr},
		{stored: "1", expr: "SET n = n + :v", value: "1e-38", wantErr: precisionErr},
		{stored: "1", expr: "ADD n :v", value: "1e-38", wantErr: precisionErr},
		{stored: "1", expr: "SET n = n + :v", value: "1e2147483000", wantErr: precisionErr},
	}

	for _, tt := range tests {
		item := map[string]*types.Item{"n": numItem(tt.stored)}

		err := (&Language{}).Update(UpdateInput{
			Expression: tt.expr,
			Item:       item,
			Attributes: map[string]*types.Item{":v": numItem(tt.value)},
		})

		if tt.wantErr != "" {
			if err == nil || err.Error() != tt.wantErr {
				t.Errorf("%s %s %s: got err %v, want %q", tt.stored, tt.expr, tt.value, err, tt.wantErr)
			}

			continue
		}

		if err != nil {
			t.Errorf("%s %s %s: %v", tt.stored, tt.expr, tt.value, err)

			continue
		}

		if got := *item["n"].N; got != tt.want {
			t.Errorf("%s %s %s: got %s, want %s", tt.stored, tt.expr, tt.value, got, tt.want)
		}
	}
}

// Conditions compare N values by value, to the last digit, whatever the spelling.
func TestLanguageMatch_numbersCompareExactly(t *testing.T) {
	t.Parallel()

	other := "12345678901234567890123456789012345679"

	tests := []struct {
		expr, stored, value string
		want                bool
	}{
		{"n = :v", precisionDigits38, other, false},
		{"n <> :v", precisionDigits38, other, true},
		{"n < :v", precisionDigits38, other, true},
		{"n <= :v", other, precisionDigits38, false},
		{"n > :v", other, precisionDigits38, true},
		{"n >= :v", precisionDigits38, precisionDigits38, true},
		{"n = :v", "1.0", "1", true},
		{"n = :v", "1e2", "100", true},
		{"n = :v", "-0", "0", true},
		{"n < :v", "-1e2", "-99", true},
		{"n > :v", "0.0000000000000000000000000000000000002", "0.0000000000000000000000000000000000001", true},
		{"n BETWEEN :v AND :w", precisionDigits38, precisionDigits38, true},
		{"n IN (:v, :w)", other, precisionDigits38, false},
	}

	for _, tt := range tests {
		got, err := (&Language{}).Match(MatchInput{
			Expression: tt.expr,
			Item:       map[string]*types.Item{"n": numItem(tt.stored)},
			Attributes: map[string]*types.Item{":v": numItem(tt.value), ":w": numItem(tt.value)},
		})
		if err != nil {
			t.Errorf("%s (%s, %s): %v", tt.expr, tt.stored, tt.value, err)

			continue
		}

		if got != tt.want {
			t.Errorf("%s (%s, %s): got %v, want %v", tt.expr, tt.stored, tt.value, got, tt.want)
		}
	}
}
