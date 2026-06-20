package docsize

import (
	"math"
	"math/big"
	"strings"
)

// dynamoNumberSizeBytes matches Zac Charles' dynamodb-calculator (Medium / GitHub)
// calculateNumberSizeInBytes: Decimal normalization + measure() + 21-byte cap.
func dynamoNumberSizeBytes(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 21
	}

	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return 21
	}

	if r.Sign() == 0 {
		return 1
	}

	neg := r.Sign() < 0
	if neg {
		r = new(big.Rat).Abs(r)
	}

	fixed := strings.TrimRight(strings.TrimRight(r.FloatString(380), "0"), ".")
	if fixed == "" {
		fixed = "0"
	}

	m := measureNumberString(fixed)

	size := m + 1
	if neg {
		size++
	}

	if size > 21 {
		size = 21
	}

	return size
}

func measureNumberString(n string) int {
	if i := strings.IndexByte(n, '.'); i >= 0 {
		p0 := n[:i]
		p1 := n[i+1:]

		if p0 == "0" {
			p0 = ""
			p1 = zerosStripLeadingZeroPairs(p1)
		}

		if len(p0)%2 != 0 {
			p0 = "Z" + p0
		}

		if len(p1)%2 != 0 {
			p1 += "Z"
		}

		return measureNumberString(p0 + p1)
	}

	n = zerosStripBothEndsPairs(n)
	if n == "" {
		return 0
	}

	return int(math.Ceil(float64(len(n)) / 2))
}

func zerosStripLeadingZeroPairs(n string) string {
	for {
		t := stripLeadingPairOfZeros(n)
		if t == n {
			return n
		}

		n = t
	}
}

func stripLeadingPairOfZeros(n string) string {
	if strings.HasPrefix(n, "00") {
		return n[2:]
	}

	return n
}

func zerosStripBothEndsPairs(n string) string {
	for {
		t := stripLeadingPairOfZeros(n)
		if t == n {
			break
		}

		n = t
	}

	for {
		t := stripTrailingPairOfZeros(n)
		if t == n {
			break
		}

		n = t
	}

	return n
}

func stripTrailingPairOfZeros(n string) string {
	if len(n) >= 2 && n[len(n)-2:] == "00" {
		return n[:len(n)-2]
	}

	return n
}
