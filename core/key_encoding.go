package core

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/truora/minidyn/types"
)

// Stored keys are strings built by an order-preserving, self-delimiting encoding of the
// typed key attributes, so comparing two encoded keys with plain string comparison orders
// them like DynamoDB does, and no two distinct primary keys share an encoding:
//
//   - S and B: the bytes with 0x00 escaped as 0x00 0xFF, then the terminator 0x00 0x01.
//     The terminator sorts below every escaped or plain byte, so a value sorts before the
//     values it is a prefix of, and byte order is preserved.
//   - N: a sign byte (0x40 negative, 0x80 zero, 0xC0 positive), a big-endian 16-bit decimal
//     exponent, the significant digits (each as digit+1, so never 0x00) and the terminator
//     0x00. A negative number is the encoding of its magnitude with every byte after the
//     sign inverted, which reverses the order. Equal values have one encoding whatever their
//     spelling ("7", "7.0", "07", "7e0"), and any number of up to 38 digits is exact.
//   - A composite key is the hash encoding followed by the range encoding. Each part is
//     self-delimiting, so the pair can't be confused with another one, and the hash encoding
//     is a prefix shared exactly by the keys of one partition.
const (
	numberNegative = 0x40
	numberZero     = 0x80
	numberPositive = 0xC0

	// numberExpBias maps the signed decimal exponent onto an unsigned 16-bit value.
	numberExpBias = 1 << 15
	// minNumberExp and maxNumberExp are the exponents (0.digits * 10^exp) of the smallest
	// (1E-130) and largest (9.99...E+125) magnitudes DynamoDB accepts.
	minNumberExp = -129
	maxNumberExp = 126
)

// appendKeyPart appends the encoding of one key attribute value of attribute type typ.
// The attribute name field is only used to report an invalid number.
func appendKeyPart(dst []byte, field, typ string, val any) ([]byte, error) {
	switch v := val.(type) {
	case []byte:
		return appendEscaped(dst, v), nil
	case string:
		if typ == "N" {
			return appendNumber(dst, field, v)
		}

		return appendEscaped(dst, v), nil
	default:
		return appendEscaped(dst, fmt.Sprintf("%v", v)), nil
	}
}

func appendEscaped[T ~string | ~[]byte](dst []byte, b T) []byte {
	for i := range len(b) {
		if b[i] == 0x00 {
			dst = append(dst, 0x00, 0xFF)

			continue
		}

		dst = append(dst, b[i])
	}

	return append(dst, 0x00, 0x01)
}

// parseDecimal splits a number into its sign, its significant digits (no leading or trailing
// zeros) and the exponent e such that the value is 0.digits * 10^e. No digits means zero. It
// returns ErrInvalidNumber when s is not a number or its exponent text doesn't fit in 32 bits,
// which is where DynamoDB stops treating it as a number.
func parseDecimal(s string) (neg bool, digits string, exp int, err error) {
	if s != "" && (s[0] == '-' || s[0] == '+') {
		neg = s[0] == '-'
		s = s[1:]
	}

	mantissa := s

	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, perr := strconv.ParseInt(s[i+1:], 10, 32)
		if perr != nil || e == math.MinInt32 {
			return false, "", 0, ErrInvalidNumber
		}

		mantissa, exp = s[:i], int(e)
	}

	intPart, frac, _ := strings.Cut(mantissa, ".")

	all := intPart + frac
	if all == "" || !onlyDigits(all) {
		return false, "", 0, ErrInvalidNumber
	}

	// Each leading zero moves the point one place right of the first significant digit.
	trimmed := strings.TrimLeft(all, "0")
	exp += len(intPart) - (len(all) - len(trimmed))

	return neg, strings.TrimRight(trimmed, "0"), exp, nil
}

func onlyDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}

	return true
}

// checkNumberRange accepts the magnitudes DynamoDB stores, 1E-130 to 9.99...E+125, which are the
// exponents minNumberExp to maxNumberExp of the 0.digits * 10^exp form. They all fit the 16-bit
// exponent of the encoding.
func checkNumberRange(exp int) error {
	switch {
	case exp > maxNumberExp:
		return ErrNumberOverflow
	case exp < minNumberExp:
		return ErrNumberUnderflow
	}

	return nil
}

func appendNumber(dst []byte, field, s string) ([]byte, error) {
	neg, digits, exp, err := parseDecimal(s)
	if err == nil && digits != "" {
		err = checkNumberRange(exp)
	}

	if err != nil {
		return nil, fmt.Errorf("%w%s", err, types.Detail("field", field, "number", s))
	}

	if digits == "" {
		return append(dst, numberZero), nil
	}

	sign := byte(numberPositive)
	if neg {
		sign = numberNegative
	}

	dst = append(dst, sign)
	start := len(dst)
	e := uint16(exp + numberExpBias) //nolint:gosec // bounded by checkNumberRange

	dst = append(dst, byte(e>>8), byte(e))
	for i := range len(digits) {
		dst = append(dst, digits[i]-'0'+1)
	}

	dst = append(dst, 0x00)

	if neg {
		for i := start; i < len(dst); i++ {
			dst[i] = ^dst[i]
		}
	}

	return dst, nil
}
