package types

import (
	"math/big"
	"strings"
	"testing"
)

// maxRatExp bounds the exponents FuzzDecimal compares against math/big, which refuses large
// exponents itself and would turn the fuzzer into a memory test.
const maxRatExp = 1000

// ratOf is the exact value of d.
func ratOf(d Decimal) *big.Rat {
	r := new(big.Rat)
	if d.IsZero() {
		return r
	}

	n, _ := new(big.Int).SetString(d.Digits, 10)
	if d.Neg {
		n.Neg(n)
	}

	r.SetInt(n)

	return r.Mul(r, pow10Rat(d.Exp-len(d.Digits)))
}

func pow10Rat(e int) *big.Rat {
	p := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(max(e, -e))), nil)
	if e < 0 {
		return new(big.Rat).SetFrac(big.NewInt(1), p)
	}

	return new(big.Rat).SetInt(p)
}

func smallExp(d Decimal) bool {
	return d.Exp >= -maxRatExp && d.Exp <= maxRatExp
}

// plainNumberText reports whether s only uses the characters of a decimal number, which are the
// ones math/big and ParseDecimal agree on. big.Rat also reads fractions, base prefixes and
// underscores.
func plainNumberText(s string) bool {
	return strings.Trim(s, "0123456789+-.eE") == ""
}

// sigDigits counts the significant digits of the exact value r, a decimal number.
func sigDigits(r *big.Rat, lastDigitExp int) int {
	scaled := new(big.Rat).Quo(r, pow10Rat(lastDigitExp))
	if !scaled.IsInt() {
		return 1 << 30
	}

	if scaled.Sign() == 0 {
		return 0
	}

	return len(strings.TrimRight(new(big.Int).Abs(scaled.Num()).String(), "0"))
}

func FuzzDecimal(f *testing.F) {
	for _, s := range []string{
		"0", "-0", "1", "+1", "-1.50", "0.0001", "1e5", "1E+5", "1e-5", ".5", "5.", "+", "-", ".", "", "e", "1e", "1e+",
		"0e5", "1e-0", "00012.3400", "1e2147483647", "1e-2147483647", "1e2147483648", "1e126", "1e-131",
		"9.9999999999999999999999999999999999999e125", "12345678901234567890123456789012345678",
		"1_0", "0x10", "1/2", " 1", "١٢", "1e400", "1e401", "1e-401", strings.Repeat("9", 1000),
	} {
		f.Add(s, "1")
		f.Add("1", s)
		f.Add(s, s)
	}

	f.Fuzz(func(t *testing.T, a, b string) {
		da, okA := ParseDecimal(a)
		checkParse(t, a, da, okA)

		db, okB := ParseDecimal(b)
		checkParse(t, b, db, okB)

		// Add takes numbers DynamoDB accepts: a longer operand makes it give up on a wide span
		// without adding, even when the sum would be short (9...9 + 1).
		if !okA || !okB || !smallExp(da) || !smallExp(db) ||
			len(da.Digits) > MaxNumberDigits || len(db.Digits) > MaxNumberDigits {
			return
		}

		ra, rb := ratOf(da), ratOf(db)

		if got, want := da.Cmp(db), ra.Cmp(rb); got != want {
			t.Fatalf("Cmp(%q, %q) = %d, big.Rat says %d", a, b, got, want)
		}

		sum, ok := da.Add(db)
		exact := new(big.Rat).Add(ra, rb)
		lo := min(da.Exp-len(da.Digits), db.Exp-len(db.Digits))

		if wantOK := sigDigits(exact, lo) <= MaxNumberDigits; ok != wantOK {
			t.Fatalf("%q + %q: ok = %v, but the exact sum %s has %d digits", a, b, ok, exact.RatString(), sigDigits(exact, lo))
		}

		if ok && ratOf(sum).Cmp(exact) != 0 {
			t.Fatalf("%q + %q = %s, big.Rat says %s", a, b, sum, exact.RatString())
		}
	})
}

func checkParse(t *testing.T, s string, d Decimal, ok bool) {
	t.Helper()

	if plainNumberText(s) {
		r, ratOK := new(big.Rat).SetString(s)

		switch {
		// big.Rat reads zero with any exponent; ParseDecimal stops at the 32 bits DynamoDB does.
		case !ok && ratOK && r.Sign() != 0:
			t.Fatalf("ParseDecimal(%q) failed but big.Rat reads %s", s, r.RatString())
		case ok && !ratOK && smallExp(d):
			t.Fatalf("ParseDecimal(%q) = %+v but big.Rat rejects it", s, d)
		case ok && ratOK && smallExp(d) && ratOf(d).Cmp(r) != 0:
			t.Fatalf("ParseDecimal(%q) = %s, big.Rat says %s", s, ratOf(d).RatString(), r.RatString())
		}
	}

	norm, normOK := NormalizeNumber(s)
	if normOK != ok {
		t.Fatalf("NormalizeNumber(%q) ok = %v, ParseDecimal ok = %v", s, normOK, ok)
	}

	if !ok {
		return
	}

	if again, againOK := NormalizeNumber(norm); !againOK || again != norm {
		t.Fatalf("NormalizeNumber is not idempotent: %q -> %q -> %q", s, norm, again)
	}

	if back, backOK := ParseDecimal(d.String()); !backOK || back != d {
		t.Fatalf("ParseDecimal(String(%+v)) = %+v, %v", d, back, backOK)
	}
}
