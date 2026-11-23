package types

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	textInvalidNumber = "A value provided cannot be converted into a number"
	textOverflow      = "Number overflow. Attempting to store a number with magnitude larger than supported range"
	textUnderflow     = "Number underflow. Attempting to store a number with magnitude smaller than supported range"
	textPrecision     = "DynamoDB only supports precision up to 38 digits"
	textDuplicates    = "One or more parameter values were invalid: Input collection"
)

func numItem(s string) *Item { return &Item{N: &s} }

func TestValidateItemAttributeValue_numbers(t *testing.T) {
	t.Parallel()

	const digits39 = "123456789012345678901234567890123456789"

	tests := []struct {
		name string
		item *Item
		want string // DynamoDB's text; the error may continue with " | detail"
	}{
		{"plain", numItem("12.5"), ""},
		{"zero spellings", numItem("-0"), ""},
		{"leading zeros before 38 digits", numItem("0001234567890123456789012345678901234567"), ""},
		{"trailing zeros are not significant", numItem("1.00000000000000000000000000000000000000000"), ""},
		{"38 digits with a trailing zero", numItem("123456789012345678901234567890123456780"), ""},
		{"limits", numItem("9.9999999999999999999999999999999999999E+125"), ""},
		{"smallest", numItem("-1e-130"), ""},
		{"zero with an exponent in 32 bits", numItem("0e2147483647"), ""},
		{"zero with an exponent past 32 bits", numItem("0e99999999999"), textInvalidNumber},
		{"not a number", numItem("abc"), textInvalidNumber},
		{"empty", numItem(""), textInvalidNumber},
		{"sign only", numItem("-"), textInvalidNumber},
		{"hex", numItem("0x10"), textInvalidNumber},
		{"space", numItem(" 5"), textInvalidNumber},
		{"NaN", numItem("NaN"), textInvalidNumber},
		{"exponent past 32 bits", numItem("1e2147483648"), textInvalidNumber},
		{"negative exponent past 32 bits", numItem("1e-2147483649"), textInvalidNumber},
		{"overflow", numItem("1e126"), textOverflow},
		{"overflow with exponent at 32 bits", numItem("1e2147483647"), textOverflow},
		{"underflow", numItem("-1e-131"), textUnderflow},
		{"underflow with exponent at 32 bits", numItem("1e-2147483647"), textUnderflow},
		{"39 digits", numItem(digits39), textPrecision},
		{"39 digits after the point", numItem("0." + digits39), textPrecision},
		{"39 digits and an overflow: precision first", numItem(digits39 + "e130"), textPrecision},
		{"39 digits and an underflow: precision first", numItem(digits39 + "e-200"), textPrecision},
		{"in a list", &Item{L: []*Item{numItem("1"), numItem("abc")}}, textInvalidNumber},
		{"in a map", &Item{M: map[string]*Item{"a": numItem("1e126")}}, textOverflow},
		{"deep", &Item{L: []*Item{{M: map[string]*Item{"a": {L: []*Item{numItem("1e-131")}}}}}}, textUnderflow},
		{"number set member", &Item{NS: []*string{new("1"), new("abc")}}, textInvalidNumber},
		{"number set member precision", &Item{NS: []*string{new(digits39)}}, textPrecision},
		{"nil number set member", &Item{NS: []*string{nil, new("1")}}, textInvalidNumber},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateItemAttributeValue(tt.item)
			if tt.want == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.True(t, strings.HasPrefix(err.Error(), tt.want), err.Error())
		})
	}
}

// DynamoDB Local reports what it meets first, walking lists, maps and sets in order: an invalid
// number before a later duplicate, and a duplicate before a later invalid number.
func TestValidateItemAttributeValue_numberOrder(t *testing.T) {
	t.Parallel()

	dupSS := &Item{SS: []*string{new("a"), new("a")}}

	tests := []struct {
		name string
		item *Item
		want string
	}{
		{"number before SS duplicate", &Item{L: []*Item{numItem("abc"), dupSS}}, textInvalidNumber},
		{"SS duplicate before number", &Item{L: []*Item{dupSS, numItem("abc")}}, textDuplicates},
		{"NS number before duplicate", &Item{NS: []*string{new("abc"), new("1"), new("1")}}, textInvalidNumber},
		{"NS duplicate before number", &Item{NS: []*string{new("1"), new("1"), new("abc")}}, textDuplicates},
		{"NS repeated invalid number", &Item{NS: []*string{new("abc"), new("abc")}}, textInvalidNumber},
		{"first invalid number wins", &Item{L: []*Item{numItem("1e126"), numItem("abc")}}, textOverflow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateItemAttributeValue(tt.item)
			require.Error(t, err)
			require.True(t, strings.HasPrefix(err.Error(), tt.want), err.Error())
		})
	}
}

func TestValidateItemMap_numberDetail(t *testing.T) {
	t.Parallel()

	err := ValidateItemMap(map[string]*Item{
		"doc": {M: map[string]*Item{"vals": {L: []*Item{numItem("1"), numItem("abc")}}}},
	})
	require.EqualError(t, err, textInvalidNumber+" | path: doc.vals[1]; number: abc")
	require.ErrorIs(t, err, ErrInvalidNumber)

	err = ValidateItemMap(map[string]*Item{"n": numItem("1e126")})
	require.EqualError(t, err, textOverflow+" | path: n; number: 1e126")
	require.ErrorIs(t, err, ErrNumberOverflow)

	err = ValidateItemMap(map[string]*Item{"n": numItem("1e-131")})
	require.ErrorIs(t, err, ErrNumberUnderflow)

	err = ValidateItemMap(map[string]*Item{"n": {NS: []*string{new("1"), new("123456789012345678901234567890123456789")}}})
	require.ErrorIs(t, err, ErrNumberPrecision)
}

// An expression attribute value with a number DynamoDB rejects is reported the way DynamoDB
// Local words it: with its own prefix and the placeholder the value is bound to.
func TestValidateExpressionAttributeValues(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateExpressionAttributeValues(nil))
	require.NoError(t, ValidateExpressionAttributeValues(map[string]*Item{":v": numItem("5")}))

	const prefix = "ExpressionAttributeValues contains invalid value: "

	tests := []struct {
		name string
		vals map[string]*Item
		want string
	}{
		{"invalid", map[string]*Item{":v": numItem("abc")}, prefix + textInvalidNumber + " for key :v"},
		{"overflow", map[string]*Item{":v": numItem("1e126")}, prefix + textOverflow + " for key :v"},
		{"nested", map[string]*Item{":w": {L: []*Item{numItem("1e-131")}}}, prefix + textUnderflow + " for key :w"},
		{"set", map[string]*Item{":s": {NS: []*string{new("1"), new("x")}}}, prefix + textInvalidNumber + " for key :s"},
		{"precision", map[string]*Item{":p": numItem("123456789012345678901234567890123456789")}, prefix + textPrecision + " for key :p"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateExpressionAttributeValues(tt.vals)
			require.Error(t, err)
			require.True(t, strings.HasPrefix(err.Error(), tt.want), err.Error())
		})
	}

	// Any other problem of a value is wrapped the same way: a duplicate set member, a null.
	tests = []struct {
		name string
		vals map[string]*Item
		want string
	}{
		{"duplicate set", map[string]*Item{":v": {SS: []*string{new("a"), new("a")}}}, prefix + textDuplicates},
		{"null", map[string]*Item{":v": nil}, prefix + nullAttributeValueText + " for key :v"},
		{
			"null nested in a map",
			map[string]*Item{":v": {M: map[string]*Item{"a": nil}}},
			prefix + nullAttributeValueText + ": a is null for key :v",
		},
		{"null list element", map[string]*Item{":v": {L: []*Item{nil}}}, prefix + nullAttributeValueText + " for key :v"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateExpressionAttributeValues(tt.vals)
			require.Error(t, err)
			require.True(t, strings.HasPrefix(err.Error(), tt.want), err.Error())
			require.Contains(t, err.Error(), " for key :v")
		})
	}
}

// Local words the duplicate set of an expression value by the type of the set: a String Set
// without the closing period, a Number Set without the members, a Binary Set by its type.
func TestValidateExpressionAttributeValues_duplicateText(t *testing.T) {
	t.Parallel()

	const prefix = "ExpressionAttributeValues contains invalid value: "

	tests := []struct {
		name string
		av   *Item
		want string
	}{
		{"SS", &Item{SS: []*string{new("a"), new("a")}}, prefix + "One or more parameter values were invalid: Input collection [a, a] contains duplicates for key :v"},
		{"BS", &Item{BS: [][]byte{{1}, {1}}}, prefix + "One or more parameter values were invalid: Input collection of type BS contains duplicates. for key :v"},
		{"NS", &Item{NS: []*string{new("1"), new("1")}}, prefix + "Input collection contains duplicates for key :v"},
		{"nested SS", &Item{L: []*Item{{SS: []*string{new("a"), new("a")}}}}, prefix + "One or more parameter values were invalid: Input collection [a, a] contains duplicates for key :v"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateExpressionAttributeValues(map[string]*Item{":v": tt.av})
			require.Error(t, err)
			require.True(t, strings.HasPrefix(err.Error(), tt.want), err.Error())
		})
	}
}

// The walk reports every problem as one typed error, whatever its kind.
func TestValidateItemAttributeValue_oneTypedError(t *testing.T) {
	t.Parallel()

	for name, av := range map[string]*Item{
		"number":    numItem("abc"),
		"duplicate": {SS: []*string{new("a"), new("a")}},
		"null":      {L: []*Item{nil}},
	} {
		err := ValidateItemAttributeValue(av)
		require.Error(t, err, name)

		_, ok := errors.AsType[*ValueError](err)
		require.True(t, ok, name)
	}
}

// A path is only named when there is one.
func TestValidateItemAttributeValue_emptyPath(t *testing.T) {
	t.Parallel()

	require.EqualError(t, ValidateItemAttributeValue(numItem("abc")), textInvalidNumber+" | number: abc")
	require.EqualError(t, ValidateItemAttributeValue(&Item{L: []*Item{numItem("abc")}}),
		textInvalidNumber+" | path: [0]; number: abc")
}

// A valid value is walked without allocating: the path of an error is built only when there is one.
func TestValidateItemMap_noAllocationsWhenValid(t *testing.T) {
	m := map[string]*Item{
		"a": {L: []*Item{numItem("1"), {M: map[string]*Item{"k": numItem("2.5"), "l": {L: []*Item{numItem("3")}}}}}},
		"b": {M: map[string]*Item{"x": numItem("-4")}},
	}

	allocs := testing.AllocsPerRun(100, func() {
		if err := ValidateItemMap(m); err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocs)
}

func TestParseNumber(t *testing.T) {
	t.Parallel()

	d, err := ParseNumber("-0012.50")
	require.NoError(t, err)
	require.Equal(t, Decimal{Neg: true, Digits: "125", Exp: 2}, d)

	for in, want := range map[string]error{
		"abc":          ErrInvalidNumber,
		"1e2147483648": ErrInvalidNumber,
		"123456789012345678901234567890123456789": ErrNumberPrecision,
		"1e126":  ErrNumberOverflow,
		"1e-131": ErrNumberUnderflow,
	} {
		_, err := ParseNumber(in)
		require.ErrorIs(t, err, want, in)
		require.ErrorIs(t, ValidateNumber(in), want, in)
	}

	// The shortcut for plain numbers agrees with the parser at its edges.
	for _, in := range []string{"0", "-0", "5", "-12.5", "1234567890123456789012345678901234567", "0.0000000000000000000000000000000000001"} {
		require.NoError(t, ValidateNumber(in), in)

		_, err := ParseNumber(in)
		require.NoError(t, err, in)
	}
}

// The wording of an expression value keeps the typed error and its cause reachable.
func TestValidateExpressionAttributeValues_unwraps(t *testing.T) {
	t.Parallel()

	err := ValidateExpressionAttributeValues(map[string]*Item{":v": numItem("1e126")})
	require.ErrorIs(t, err, ErrNumberOverflow)

	ve, ok := errors.AsType[*ValueError](err)
	require.True(t, ok)
	require.EqualError(t, ve, textOverflow+" | path: :v; number: 1e126")
}
