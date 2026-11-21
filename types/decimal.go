package types

import (
	"errors"
	"math"
	"math/big"
	"strconv"
	"strings"
)

// MaxNumberDigits is the most significant digits an N value can have.
const MaxNumberDigits = 38

// maxPlainExp is the largest exponent String writes out in full. A value beyond it, which no
// stored N has, is written in scientific notation instead of as thousands of zeros.
const maxPlainExp = 400

const (
	// minNumberExp and maxNumberExp are the exponents (0.digits * 10^exp) of the smallest
	// (1E-130) and largest (9.99...E+125) magnitudes DynamoDB stores.
	minNumberExp = -129
	maxNumberExp = 126
)

var (
	// ErrNumberOverflow is DynamoDB's error for a number whose magnitude is above 9.99...E+125.
	ErrNumberOverflow = errors.New("Number overflow. Attempting to store a number with magnitude larger than supported range") //nolint:stylecheck,staticcheck,ST1005 // consistent with AWS SDK errors

	// ErrNumberUnderflow is DynamoDB's error for a number whose magnitude is below 1E-130 and not zero.
	ErrNumberUnderflow = errors.New("Number underflow. Attempting to store a number with magnitude smaller than supported range") //nolint:stylecheck,staticcheck,ST1005 // consistent with AWS SDK errors
)

// Decimal is an N value taken apart: a sign, its significant digits (no leading or trailing
// zeros) and the exponent Exp such that the value is 0.Digits * 10^Exp. No digits means zero.
type Decimal struct {
	Neg    bool
	Digits string
	Exp    int
}

// ParseDecimal reads an N value in any of DynamoDB's spellings (sign, point, exponent, leading
// and trailing zeros). It returns false when s is not a number or its exponent text doesn't fit
// in 32 bits, which is where DynamoDB stops treating it as a number. The same holds for the
// exponent of the number in scientific notation.
func ParseDecimal(s string) (d Decimal, ok bool) {
	if s != "" && (s[0] == '-' || s[0] == '+') {
		d.Neg = s[0] == '-'
		s = s[1:]
	}

	mantissa := s

	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.ParseInt(s[i+1:], 10, 32)
		if err != nil || e == math.MinInt32 {
			return Decimal{}, false
		}

		mantissa, d.Exp = s[:i], int(e)
	}

	intPart, frac, _ := strings.Cut(mantissa, ".")

	all := intPart + frac
	if all == "" || !onlyDigits(all) {
		return Decimal{}, false
	}

	// Each leading zero moves the point one place right of the first significant digit.
	trimmed := strings.TrimLeft(all, "0")
	d.Exp += len(intPart) - (len(all) - len(trimmed))
	d.Digits = strings.TrimRight(trimmed, "0")

	if d.Digits == "" {
		return Decimal{}, true
	}

	// String writes an exponent beyond 400 as scientific notation, whose exponent is Exp-1. It
	// must fit the 32 bits ParseDecimal reads, or the text String writes would not parse back
	// (10e2147483647). int is 32 bits on some platforms, where the sum above already wrapped for
	// exponents this large.
	if sci := int64(d.Exp) - 1; sci > math.MaxInt32 {
		return Decimal{}, false
	}

	return d, true
}

func onlyDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}

	return true
}

// CheckRange accepts the magnitudes DynamoDB stores, 1E-130 to 9.99...E+125, and zero. Otherwise
// it returns ErrNumberOverflow or ErrNumberUnderflow.
func (d Decimal) CheckRange() error {
	switch {
	case d.IsZero():
		return nil
	case d.Exp > maxNumberExp:
		return ErrNumberOverflow
	case d.Exp < minNumberExp:
		return ErrNumberUnderflow
	}

	return nil
}

// IsZero reports whether d is zero, whatever its sign.
func (d Decimal) IsZero() bool { return d.Digits == "" }

// Cmp compares d and o by value: -1, 0 or 1.
func (d Decimal) Cmp(o Decimal) int {
	ds, os := d.sign(), o.sign()
	if ds != os || ds == 0 {
		return cmpInt(ds, os)
	}

	// Same sign: the larger magnitude is the one with the larger exponent, then the larger
	// digits, which compare as text because neither has trailing zeros.
	mag := cmpInt(d.Exp, o.Exp)
	if mag == 0 {
		mag = strings.Compare(d.Digits, o.Digits)
	}

	return ds * mag
}

// sign is -1, 0 or 1.
func (d Decimal) sign() int {
	switch {
	case d.IsZero():
		return 0
	case d.Neg:
		return -1
	default:
		return 1
	}
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// Add returns d + o exactly. It returns false when the sum has more than MaxNumberDigits
// significant digits, which DynamoDB rejects. The operands are numbers DynamoDB accepts, so each
// has at most MaxNumberDigits digits.
func (d Decimal) Add(o Decimal) (Decimal, bool) {
	switch {
	case o.IsZero():
		return d, true
	case d.IsZero():
		return o, true
	}

	// Digits are placed from the highest digit of the larger operand down to the lowest digit of
	// the smaller one. A wider span cannot yield a sum of at most 38 digits, and cutting it off
	// here keeps an exponent like 1e2000000000 from asking for billions of zeros.
	hi, lo := max(d.Exp, o.Exp), min(d.Exp-len(d.Digits), o.Exp-len(o.Digits))
	if hi-lo > 2*MaxNumberDigits+1 {
		return Decimal{}, false
	}

	sum := new(big.Int).Add(d.scaled(lo), o.scaled(lo))
	if sum.Sign() == 0 {
		return Decimal{}, true
	}

	res := Decimal{Neg: sum.Sign() < 0}
	text := new(big.Int).Abs(sum).String()
	res.Exp = len(text) + lo
	res.Digits = strings.TrimRight(text, "0")

	return res, len(res.Digits) <= MaxNumberDigits
}

// Negate returns -d.
func (d Decimal) Negate() Decimal {
	d.Neg = !d.Neg && !d.IsZero()

	return d
}

// scaled is d as the integer d * 10^-lo, where lo is at most the exponent of d's last digit.
func (d Decimal) scaled(lo int) *big.Int {
	n, _ := new(big.Int).SetString(d.Digits+strings.Repeat("0", d.Exp-len(d.Digits)-lo), 10)
	if d.Neg {
		n.Neg(n)
	}

	return n
}

// String writes d the way DynamoDB writes a stored number: plain digits, no exponent, no
// leading or trailing zeros.
func (d Decimal) String() string {
	if d.IsZero() {
		return "0"
	}

	sign := ""
	if d.Neg {
		sign = "-"
	}

	switch {
	case d.Exp > maxPlainExp || d.Exp < -maxPlainExp:
		mantissa := d.Digits[:1]
		if len(d.Digits) > 1 {
			mantissa += "." + d.Digits[1:]
		}

		return sign + mantissa + "E" + strconv.Itoa(d.Exp-1)
	case d.Exp >= len(d.Digits):
		return sign + d.Digits + strings.Repeat("0", d.Exp-len(d.Digits))
	case d.Exp <= 0:
		return sign + "0." + strings.Repeat("0", -d.Exp) + d.Digits
	default:
		return sign + d.Digits[:d.Exp] + "." + d.Digits[d.Exp:]
	}
}

// NormalizeNumber returns the text DynamoDB stores for an N value: the same number with no
// leading or trailing zeros and no exponent. It returns false when s is not a number. Text that
// is already in that form, the usual case, is returned as it is without allocating.
func NormalizeNumber(s string) (string, bool) {
	if isPlainNumber(s) {
		return s, true
	}

	d, ok := ParseDecimal(s)
	if !ok {
		return "", false
	}

	return d.String(), true
}

// isPlainNumber reports whether s is already what Decimal.String writes: optional "-", no
// leading zero before a digit, a point only with a nonzero last digit, and not "-0". Texts
// longer than a plain 38-digit value with its point and zeros may need the exponent cutoff of
// String, so they take the slow path.
func isPlainNumber(s string) bool {
	const maxFast = 64

	if s == "" || len(s) > maxFast || s == "-0" {
		return false
	}

	body := s
	if body[0] == '-' {
		body = body[1:]
	}

	intPart, frac, hasPoint := strings.Cut(body, ".")
	if intPart == "" || !onlyDigits(intPart) || (len(intPart) > 1 && intPart[0] == '0') {
		return false
	}

	if !hasPoint {
		return true
	}

	return frac != "" && onlyDigits(frac) && frac[len(frac)-1] != '0'
}
