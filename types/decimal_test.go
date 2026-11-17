package types

import (
	"errors"
	"strconv"
	"strings"
	"testing"
)

func TestParseDecimal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in     string
		want   Decimal
		wantOK bool
	}{
		{"1", Decimal{Digits: "1", Exp: 1}, true},
		{"-12.50", Decimal{Neg: true, Digits: "125", Exp: 2}, true},
		{"+0.001", Decimal{Digits: "1", Exp: -2}, true},
		{"1E+2", Decimal{Digits: "1", Exp: 3}, true},
		{"100", Decimal{Digits: "1", Exp: 3}, true},
		{"0", Decimal{}, true},
		{"-0.0", Decimal{}, true},
		{".5", Decimal{Digits: "5"}, true},
		{"5.", Decimal{Digits: "5", Exp: 1}, true},
		{"", Decimal{}, false},
		{"-", Decimal{}, false},
		{".", Decimal{}, false},
		{"abc", Decimal{}, false},
		{" 5", Decimal{}, false},
		{"1e", Decimal{}, false},
		{"1e99999999999", Decimal{}, false},
		{"0x10", Decimal{}, false},
		{"+", Decimal{}, false},
		{"1e+", Decimal{}, false},
		{"0e5", Decimal{}, true},
		{"1e-0", Decimal{Digits: "1", Exp: 1}, true},
		{"1e2147483648", Decimal{}, false},
		{"10e2147483647", Decimal{}, false},
		{"0.1e2147483648", Decimal{}, false},
	}

	for _, tt := range tests {
		got, ok := ParseDecimal(tt.in)
		if ok != tt.wantOK || got != tt.want {
			t.Errorf("ParseDecimal(%q) = %+v, %v; want %+v, %v", tt.in, got, ok, tt.want, tt.wantOK)
		}
	}
}

// TestParseDecimalLargestExponent pins that the largest exponent text ParseDecimal takes is read
// as a number far above the range. The exponent is added to an int in ParseDecimal ("d.Exp +="),
// so on a 32-bit platform 1e2147483647 would wrap negative and read as an underflow; the
// check only runs where int has 64 bits.
func TestParseDecimalLargestExponent(t *testing.T) {
	t.Parallel()

	if strconv.IntSize < 64 {
		t.Skip("the exponent sum needs a 64-bit int")
	}

	d, ok := ParseDecimal("1e2147483647")
	if !ok || d.Exp != 1<<31 || !errors.Is(d.CheckRange(), ErrNumberOverflow) {
		t.Errorf("got %+v, %v", d, ok)
	}
}

func TestDecimalCmp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		a, b string
		want int
	}{
		{"1", "1.0", 0},
		{"0", "-0", 0},
		{"1", "2", -1},
		{"-1", "-2", 1},
		{"-1", "1", -1},
		{"0", "-1", 1},
		{"0", "1", -1},
		{"10", "9", 1},
		{"0.1", "0.09", 1},
		{"-0.1", "-0.09", -1},
		{"12345678901234567890123456789012345678", "12345678901234567890123456789012345679", -1},
		{"1e125", "9e124", 1},
		{"1e-130", "0", 1},
		{"1E400", "1E401", -1},
		{"1.5E-400", "1.5E-401", 1},
		{"-1E400", "-1E401", 1},
		{"1E400", "1" + strings.Repeat("0", 400), 0},
	}

	for _, tt := range tests {
		a, _ := ParseDecimal(tt.a)
		b, _ := ParseDecimal(tt.b)

		if got := a.Cmp(b); got != tt.want {
			t.Errorf("Cmp(%s, %s) = %d, want %d", tt.a, tt.b, got, tt.want)
		}

		if got := b.Cmp(a); got != -tt.want {
			t.Errorf("Cmp(%s, %s) = %d, want %d", tt.b, tt.a, got, -tt.want)
		}
	}
}

func TestDecimalAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		a, b, want string
		wantOK     bool
	}{
		{"1", "2", "3", true},
		{"0", "-5", "-5", true},
		{"5", "-5", "0", true},
		{"0.1", "0.2", "0.3", true},
		{"-1.5", "0.25", "-1.25", true},
		{"99999999999999999999999999999999999999", "1", "100000000000000000000000000000000000000", true},
		{"12345678901234567890123456789012345678", "1", "12345678901234567890123456789012345679", true},
		{"1", "1e-37", "1.0000000000000000000000000000000000001", true},
		{"1", "1e-38", "", false},
		{"12345678901234567890123456789012345678", "0.5", "", false},
		{"1e125", "1e125", "2" + strings.Repeat("0", 125), true},
		{"1", "1e2000000000", "", false},
		{"1e-2000000000", "1e2000000000", "", false},
		{"1" + strings.Repeat("0", 999), "1", "", false},
		{"1", "1" + strings.Repeat("0", 999), "", false},
		{strings.Repeat("9", 1000), "1", "", false},
	}

	for _, tt := range tests {
		a, _ := ParseDecimal(tt.a)
		b, _ := ParseDecimal(tt.b)

		got, ok := a.Add(b)
		if ok != tt.wantOK || (ok && got.String() != tt.want) {
			t.Errorf("%s + %s = %s, %v; want %s, %v", tt.a, tt.b, got, ok, tt.want, tt.wantOK)
		}
	}
}

func TestDecimalNegate(t *testing.T) {
	t.Parallel()

	zero, _ := ParseDecimal("-0")
	if got := zero.Negate(); got.Neg {
		t.Error("negating zero must stay zero")
	}

	one, _ := ParseDecimal("1")
	if got := one.Negate().String(); got != "-1" {
		t.Errorf("got %s", got)
	}
}

func TestNormalizeNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in, want string
		wantOK   bool
	}{
		{"1", "1", true},
		{"-1", "-1", true},
		{"0", "0", true},
		{"100", "100", true},
		{"0.5", "0.5", true},
		{"-0.00001", "-0.00001", true},
		{"12345678901234567890123456789012345678", "12345678901234567890123456789012345678", true},
		{"1.0", "1", true},
		{"01", "1", true},
		{"00.100", "0.1", true},
		{"1e2", "100", true},
		{"1E+2", "100", true},
		{".5", "0.5", true},
		{"5.", "5", true},
		{"-0", "0", true},
		{"0.0", "0", true},
		{"1e-2", "0.01", true},
		{"1.5e3", "1500", true},
		{"1e125", "1" + strings.Repeat("0", 125), true},
		{"1e-130", "0." + strings.Repeat("0", 129) + "1", true},
		{"1e2000000000", "1E2000000000", true},
		{"-1.5e-2000000000", "-1.5E-2000000000", true},
		{"", "", false},
		{"-", "", false},
		{"abc", "", false},
		{"1e", "", false},
	}

	for _, tt := range tests {
		got, ok := NormalizeNumber(tt.in)
		if ok != tt.wantOK || got != tt.want {
			t.Errorf("NormalizeNumber(%q) = %q, %v; want %q, %v", tt.in, got, ok, tt.want, tt.wantOK)
		}
	}
}

func TestDecimalCheckRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want error
	}{
		{"0", nil},
		{"-0", nil},
		{"1", nil},
		{"1e-130", nil},
		{"-1e-130", nil},
		{"9.9999999999999999999999999999999999999e125", nil},
		{"-9.9999999999999999999999999999999999999e125", nil},
		{"1e126", ErrNumberOverflow},
		{"-1e126", ErrNumberOverflow},
		{"1e2147483647", ErrNumberOverflow},
		{"1e-131", ErrNumberUnderflow},
		{"-9e-131", ErrNumberUnderflow},
		{"1e-2147483647", ErrNumberUnderflow},
	}

	for _, tt := range tests {
		d, ok := ParseDecimal(tt.in)
		if !ok {
			t.Fatalf("ParseDecimal(%q) failed", tt.in)
		}

		if got := d.CheckRange(); !errors.Is(got, tt.want) {
			t.Errorf("CheckRange(%s) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestDecimalAddRange(t *testing.T) {
	t.Parallel()

	// Add only reports the precision; the caller checks the range of the result.
	a, _ := ParseDecimal("9e125")

	sum, ok := a.Add(a)
	if !ok || !errors.Is(sum.CheckRange(), ErrNumberOverflow) {
		t.Errorf("9e125 + 9e125 = %s, %v; want an overflowing sum", sum, ok)
	}
}
