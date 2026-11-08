package core

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func keyItemN(v string) *types.Item { return &types.Item{N: aws.String(v)} }

func keyItemS(v string) *types.Item { return &types.Item{S: aws.String(v)} }

func keyItemB(v ...byte) *types.Item { return &types.Item{B: v} }

// encodedKeyFor encodes a hash-only key of the given type.
func encodedKeyFor(t *testing.T, typ string, it *types.Item) string {
	t.Helper()

	ks := keySchema{HashKey: "k"}

	key, err := ks.GetKey(map[string]string{"k": typ}, map[string]*types.Item{"k": it})
	require.NoError(t, err)

	return key
}

// requireStrictlyAscending checks that the encoded keys of items sort in the given order.
func requireStrictlyAscending(t *testing.T, typ string, items []*types.Item) {
	t.Helper()

	var prev string

	for i, it := range items {
		cur := encodedKeyFor(t, typ, it)
		if i > 0 {
			require.Less(t, prev, cur, "item %d (%v) must sort after item %d", i, it, i-1)
		}

		prev = cur
	}
}

func TestKeyEncodingNumberOrder(t *testing.T) {
	t.Parallel()

	ordered := []string{
		"-1E100", "-99999999999999999999999999999999999999", "-99999999999", "-100", "-10.25", "-10",
		"-9", "-5", "-1.5", "-1", "-0.5", "-0.001", "-1E-100",
		"0",
		"1E-100", "0.001", "0.5", "1", "1.0000000000000000000000000000000000001", "1.5", "2", "9", "9.9",
		"10", "20", "100", "1E3", "99999999999", "99999999999999999999999999999999999999", "1E100",
	}

	items := make([]*types.Item, len(ordered))
	for i, v := range ordered {
		items[i] = keyItemN(v)
	}

	requireStrictlyAscending(t, "N", items)
}

func TestKeyEncodingNumberSpellings(t *testing.T) {
	t.Parallel()

	groups := [][]string{
		{"7", "7.0", "07", "7e0", "7E0", "+7", "0.7e1", "70e-1", "7.000"},
		{"0", "0.0", "-0", "-0.00", "0e5"},
		{"-1.5", "-1.50", "-15e-1", "-0.15E1"},
		{"1000", "1E3", "1e+3", "0.001e6"},
	}

	for _, group := range groups {
		want := encodedKeyFor(t, "N", keyItemN(group[0]))

		for _, v := range group[1:] {
			require.Equal(t, want, encodedKeyFor(t, "N", keyItemN(v)), "%s must equal %s", v, group[0])
		}
	}
}

func TestKeyEncodingBinaryOrder(t *testing.T) {
	t.Parallel()

	requireStrictlyAscending(t, "B", []*types.Item{
		keyItemB(0x00), keyItemB(0x00, 0x00), keyItemB(0x01), keyItemB(0x01, 0x00), keyItemB(0x01, 0x02),
		keyItemB(0x02), keyItemB(0x0a), keyItemB(0x10), keyItemB(0x7f), keyItemB(0x80), keyItemB(0xff),
		keyItemB(0xff, 0x00),
	})
}

func TestKeyEncodingStringOrder(t *testing.T) {
	t.Parallel()

	requireStrictlyAscending(t, "S", []*types.Item{
		keyItemS("\x00"), keyItemS("\x00a"), keyItemS("\x01"), keyItemS(" "), keyItemS("10"), keyItemS("9"),
		keyItemS("A"), keyItemS("a"), keyItemS("a b"), keyItemS("a.b"), keyItemS("aa"), keyItemS("b"),
		keyItemS("é"), keyItemS("日本"), keyItemS("😀"),
	})
}

func TestKeyEncodingCompositeIsUnambiguous(t *testing.T) {
	t.Parallel()

	ks := keySchema{HashKey: "pk", RangeKey: "sk"}
	attrs := map[string]string{"pk": "S", "sk": "S"}

	pairs := [][2]string{
		{"a", "b.c"},
		{"a.b", "c"},
		{"a", "a"},
		{"", "a"},
		{"a", ""},
		{"a\x00", "b"},
		{"a", "\x00b"},
		{"ab", ""},
		{"a", "b"},
		{"", "ab"},
		{"a\x00\x01", "b"},
		{"a", "\x01b"},
		{"a\x00", "\x01b"},
		{"a\x00\xff", "b"},
		{"a", "\xffb"},
		{"a\x00\xff\x00\x01", ""},
		{"a\x00", "\xff\x00\x01"},
	}

	seen := map[string][2]string{}

	for _, p := range pairs {
		key, err := ks.GetKey(attrs, map[string]*types.Item{"pk": keyItemS(p[0]), "sk": keyItemS(p[1])})
		require.NoError(t, err)

		if other, dup := seen[key]; dup {
			t.Fatalf("keys %q and %q collide", other, p)
		}

		seen[key] = p
	}
}

func TestKeyEncodingCompositeIsUnambiguousForBinaryAndNumbers(t *testing.T) {
	t.Parallel()

	typed := func(typ string, v string) *types.Item {
		switch typ {
		case "N":
			return keyItemN(v)
		case "B":
			return keyItemB([]byte(v)...)
		default:
			return keyItemS(v)
		}
	}

	for _, types2 := range [][2]string{{"B", "B"}, {"N", "S"}, {"S", "N"}, {"N", "N"}, {"B", "N"}} {
		ks := keySchema{HashKey: "pk", RangeKey: "sk"}
		attrs := map[string]string{"pk": types2[0], "sk": types2[1]}

		var pairs [][2]string

		switch types2 {
		case [2]string{"B", "B"}:
			pairs = [][2]string{
				{"a\x00\x01", "b"},
				{"a", "\x01b"},
				{"a\x00\xff", "b"},
				{"a", "\xffb"},
				{"a\x00", "\x01b"},
				{"", "a"},
				{"a", ""},
				{"a\x00", "b"},
				{"a", "\x00b"},
			}
		case [2]string{"N", "S"}:
			pairs = [][2]string{
				{"1", "2"},
				{"12", ""},
				{"1", "\x002"},
				{"1", "\x01"},
				{"-1", "2"},
				{"0", "2"},
				{"0", ""},
				{"10", "0"},
				{"1", "0"},
			}
		case [2]string{"S", "N"}:
			pairs = [][2]string{{"1", "2"}, {"12", "0"}, {"1\x00", "2"}, {"", "12"}, {"1", "-2"}, {"1\x00\x01", "2"}}
		case [2]string{"N", "N"}:
			pairs = [][2]string{{"1", "2"}, {"12", "0"}, {"1", "20"}, {"-1", "2"}, {"1", "-2"}, {"0", "0"}, {"0", "1"}}
		default:
			pairs = [][2]string{{"\x00\x01", "1"}, {"\x00", "1"}, {"", "1"}, {"\x00\xff", "1"}, {"a", "10"}, {"a", "1"}}
		}

		seen := map[string][2]string{}

		for _, p := range pairs {
			key, err := ks.GetKey(attrs, map[string]*types.Item{"pk": typed(types2[0], p[0]), "sk": typed(types2[1], p[1])})
			require.NoError(t, err)

			if other, dup := seen[key]; dup {
				t.Fatalf("%v: keys %q and %q collide", types2, other, p)
			}

			seen[key] = p
		}
	}
}

func TestKeyEncodingCompositeOrdersByHashThenRange(t *testing.T) {
	t.Parallel()

	ks := keySchema{HashKey: "pk", RangeKey: "sk"}
	attrs := map[string]string{"pk": "S", "sk": "N"}

	var prev string

	for i, p := range [][2]string{{"a", "1"}, {"a", "9"}, {"a", "10"}, {"a.b", "1"}, {"ab", "-1"}, {"b", "0"}} {
		key, err := ks.GetKey(attrs, map[string]*types.Item{"pk": keyItemS(p[0]), "sk": keyItemN(p[1])})
		require.NoError(t, err)

		if i > 0 {
			require.Less(t, prev, key, "%v", p)
		}

		prev = key
	}
}

func TestKeyEncodingInvalidNumber(t *testing.T) {
	t.Parallel()

	ks := keySchema{HashKey: "k"}

	// The exponent text must fit in 32 bits; DynamoDB Local says the text is not a number
	// beyond that, even for zero.
	for _, v := range []string{
		"", "abc", "-", "1e", "1.2.3", "--1", "0x10", " 5", "NaN", "Infinity", "1e999999999999999999999",
		"1e2147483648", "1e-2147483648", "0e99999999999",
	} {
		_, err := ks.GetKey(map[string]string{"k": "N"}, map[string]*types.Item{"k": keyItemN(v)})
		require.ErrorIs(t, err, ErrInvalidNumber, v)
		require.NotErrorIs(t, err, ErrInvalidAtrributeValue, v)
		require.Contains(t, err.Error(), "field: k; number: "+v, v)
	}
}

func TestKeyEncodingNumberOutOfRange(t *testing.T) {
	t.Parallel()

	ks := keySchema{HashKey: "k"}

	// DynamoDB accepts magnitudes from 1E-130 to 9.99...E+125. 1e40000 and 1e-40000 are
	// inside the exponent text bound, far outside the range.
	for v, want := range map[string]error{
		"1e126": ErrNumberOverflow, "-1e126": ErrNumberOverflow, "1e200": ErrNumberOverflow,
		"1e40000": ErrNumberOverflow, "-1e2147483647": ErrNumberOverflow, "1" + strings.Repeat("0", 200): ErrNumberOverflow,
		"1e-131": ErrNumberUnderflow, "-1e-200": ErrNumberUnderflow, "1e-40000": ErrNumberUnderflow,
		"-1e-40000": ErrNumberUnderflow, "1e-2147483647": ErrNumberUnderflow,
	} {
		_, err := ks.GetKey(map[string]string{"k": "N"}, map[string]*types.Item{"k": keyItemN(v)})
		require.ErrorIs(t, err, want, v)
		require.Contains(t, err.Error(), "field: k; number: ", v)
	}

	for _, v := range []string{
		"9.9999999999999999999999999999999999999E+125", "-9.9999999999999999999999999999999999999E+125",
		"1e-130", "-1e-130", "0e500", "0e-500",
	} {
		_, err := ks.GetKey(map[string]string{"k": "N"}, map[string]*types.Item{"k": keyItemN(v)})
		require.NoError(t, err, v)
	}
}

func TestPutKeepsDistinctItemsWithSeparatorInKeys(t *testing.T) {
	t.Parallel()

	table := seekTestTable(t, true,
		map[string]*types.Item{"pk": keyItemS("a"), "sk": keyItemS("b.c")},
		map[string]*types.Item{"pk": keyItemS("a.b"), "sk": keyItemS("c")},
		map[string]*types.Item{"pk": keyItemS("a"), "sk": keyItemS("a")},
	)

	require.Len(t, table.Data, 3)
	require.Len(t, table.SortedKeys, 3)
}

// encS is the encoding of a string key part.
func encS(s string) string { return s + "\x00\x01" }

func TestQueryOrdersNumericRangeKeyByValue(t *testing.T) {
	t.Parallel()

	table := NewTable("nums")
	table.AttributesDef = map[string]string{"pk": "S", "sk": "N"}
	table.KeySchema = keySchema{HashKey: "pk", RangeKey: "sk"}

	for _, v := range []string{"10", "2", "-5", "100", "9", "1.5", "7", "7.0"} {
		_, err := table.Put(&types.PutItemInput{
			TableName: aws.String("nums"),
			Item:      map[string]*types.Item{"pk": keyItemS("p"), "sk": keyItemN(v)},
		})
		require.NoError(t, err)
	}

	res, err := table.SearchData(QueryInput{
		KeyConditionExpression:    "pk = :p",
		ExpressionAttributeValues: map[string]*types.Item{":p": keyItemS("p")},
		ScanIndexForward:          true,
	})
	require.NoError(t, err)

	var got []string
	for _, it := range res.Items {
		got = append(got, *it["sk"].N)
	}

	// "7" and "7.0" are the same key, so the second put replaces the first.
	require.Equal(t, []string{"-5", "1.5", "2", "7.0", "9", "10", "100"}, got)
}

func TestSearchRejectsExclusiveStartKeyWithInvalidNumber(t *testing.T) {
	t.Parallel()

	table := NewTable("nums")
	table.AttributesDef = map[string]string{"pk": "S", "sk": "N"}
	table.KeySchema = keySchema{HashKey: "pk", RangeKey: "sk"}

	_, err := table.Put(&types.PutItemInput{
		TableName: aws.String("nums"),
		Item:      map[string]*types.Item{"pk": keyItemS("p"), "sk": keyItemN("1")},
	})
	require.NoError(t, err)

	for v, want := range map[string]string{
		"abc":    ErrInvalidNumber.Error(),
		"1e200":  ErrNumberOverflow.Error(),
		"1e-200": ErrNumberUnderflow.Error(),
	} {
		start := map[string]*types.Item{"pk": keyItemS("p"), "sk": keyItemN(v)}

		// Without a start key the page would restart from the first item, so a paginating
		// client could loop forever on a start key that is never valid.
		_, err := table.SearchData(QueryInput{
			KeyConditionExpression:    "pk = :p",
			ExpressionAttributeValues: map[string]*types.Item{":p": keyItemS("p")},
			ExclusiveStartKey:         start,
			ScanIndexForward:          true,
		})
		requireValidationException(t, err, want)

		_, err = table.SearchData(QueryInput{ExclusiveStartKey: start, ScanIndexForward: true})
		requireValidationException(t, err, want)
	}
}

func requireValidationException(t *testing.T, err error, message string) {
	t.Helper()

	var apiErr types.Error

	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ValidationException", apiErr.Code())
	require.Contains(t, apiErr.Error(), message)
}
