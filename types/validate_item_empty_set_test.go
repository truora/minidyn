package types

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// DynamoDB rejects an empty String, Number or Binary Set (#294). Local's texts are kept as
// they are, the double space of the first two included. An empty set is a non-nil empty slice:
// a nil one is an attribute that is not a set.
const (
	textEmptyStringSet = "One or more parameter values were invalid: An string set  may not be empty"
	textEmptyNumberSet = "One or more parameter values were invalid: An number set  may not be empty"
	textEmptyBinarySet = "One or more parameter values were invalid: Binary sets should not be empty"
)

func emptySets() map[string]struct {
	item *Item
	text string
} {
	return map[string]struct {
		item *Item
		text string
	}{
		"SS": {&Item{SS: []*string{}}, textEmptyStringSet},
		"NS": {&Item{NS: []*string{}}, textEmptyNumberSet},
		"BS": {&Item{BS: [][]byte{}}, textEmptyBinarySet},
	}
}

func TestValidateItemAttributeValue_emptySets(t *testing.T) {
	t.Parallel()

	for kind, es := range emptySets() {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			require.EqualError(t, ValidateItemAttributeValue(es.item), es.text)

			nested := map[string]struct {
				item *Item
				path string
			}{
				"list": {&Item{L: []*Item{{S: new("a")}, es.item}}, "[1]"},
				"map":  {&Item{M: map[string]*Item{"k": es.item}}, "k"},
				"deep": {&Item{L: []*Item{{M: map[string]*Item{"k": {L: []*Item{es.item}}}}}}, "[0].k[0]"},
			}

			for name, n := range nested {
				require.EqualError(t, ValidateItemAttributeValue(n.item), es.text+" | path: "+n.path, name)
			}

			require.EqualError(t, ValidateItemMap(map[string]*Item{"x": es.item}), es.text+" | path: x")
		})
	}
}

// A set with a member, an empty member of a set and an attribute that is not a set are valid.
func TestValidateItemAttributeValue_emptySetLookalikes(t *testing.T) {
	t.Parallel()

	for name, item := range map[string]*Item{
		"SS member":       {SS: []*string{new("")}},
		"BS member":       {BS: [][]byte{{}}},
		"NS member":       {NS: []*string{new("1")}},
		"nil sets":        {SS: nil, NS: nil, BS: nil},
		"empty list":      {L: []*Item{}},
		"empty map":       {M: map[string]*Item{}},
		"list of members": {L: []*Item{{SS: []*string{new("a")}}}},
	} {
		require.NoError(t, ValidateItemAttributeValue(item), name)
	}
}

// The walk meets the problems of a value in order, an empty set among them (Local, #294).
func TestValidateItemAttributeValue_emptySetOrder(t *testing.T) {
	t.Parallel()

	empty := func() *Item { return &Item{SS: []*string{}} }
	dup := func() *Item { return &Item{SS: []*string{new("a"), new("a")}} }

	tests := []struct {
		name string
		item *Item
		want string
	}{
		{"empty then bad number", &Item{L: []*Item{empty(), numItem("abc")}}, textEmptyStringSet},
		{"bad number then empty", &Item{L: []*Item{numItem("abc"), empty()}}, textInvalidNumber},
		{"empty then duplicate", &Item{L: []*Item{empty(), dup()}}, textEmptyStringSet},
		{"duplicate then empty", &Item{L: []*Item{dup(), empty()}}, "contains duplicates"},
		{"empty NS then bad number set", &Item{L: []*Item{{NS: []*string{}}, {NS: []*string{new("x")}}}}, textEmptyNumberSet},
		{"bad number set then empty NS", &Item{L: []*Item{{NS: []*string{new("x")}}, {NS: []*string{}}}}, textInvalidNumber},
		{"empty then nil element", &Item{L: []*Item{empty(), nil}}, textEmptyStringSet},
		{"nil element then empty", &Item{L: []*Item{nil, empty()}}, nullAttributeValueText},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateItemAttributeValue(tt.item)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestValidateExpressionAttributeValues_emptySets(t *testing.T) {
	t.Parallel()

	const prefix = "ExpressionAttributeValues contains invalid value: "

	for kind, es := range emptySets() {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			err := ValidateExpressionAttributeValues(map[string]*Item{":v": es.item})
			require.Error(t, err)
			require.True(t, strings.HasPrefix(err.Error(), prefix+es.text+" for key :v"), err.Error())

			err = ValidateExpressionAttributeValues(map[string]*Item{":w": {L: []*Item{es.item}}})
			require.Error(t, err)
			require.True(t, strings.HasPrefix(err.Error(), prefix+es.text+" for key :w"), err.Error())
		})
	}
}
