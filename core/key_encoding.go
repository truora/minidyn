package core

import (
	"fmt"

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

func appendNumber(dst []byte, field, s string) ([]byte, error) {
	d, err := types.ParseNumber(s)
	if err != nil {
		return nil, fmt.Errorf("%w%s", err, types.Detail("field", field, "number", s))
	}

	neg, digits, exp := d.Neg, d.Digits, d.Exp
	if digits == "" {
		return append(dst, numberZero), nil
	}

	sign := byte(numberPositive)
	if neg {
		sign = numberNegative
	}

	dst = append(dst, sign)
	start := len(dst)
	e := uint16(exp + numberExpBias) //nolint:gosec // bounded by Decimal.CheckRange

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
