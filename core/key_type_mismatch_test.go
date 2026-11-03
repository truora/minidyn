package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestGetKeyTypeMismatch(t *testing.T) {
	t.Parallel()

	attrs := map[string]string{"id": "S", "sk": "N"}

	t.Run("primary_hash_wrong_type", func(t *testing.T) {
		t.Parallel()

		ks := keySchema{HashKey: "id"}
		_, err := ks.GetKey(attrs, map[string]*types.Item{"id": {N: new("1")}})
		require.ErrorIs(t, err, ErrKeyTypeMismatch)
		require.EqualError(t, err,
			"One or more parameter values were invalid: Type mismatch for key; id expected: S actual: N")
	})

	t.Run("primary_range_wrong_type", func(t *testing.T) {
		t.Parallel()

		ks := keySchema{HashKey: "id", RangeKey: "sk"}
		_, err := ks.GetKey(attrs, map[string]*types.Item{"id": {S: new("a")}, "sk": {S: new("b")}})
		require.ErrorIs(t, err, ErrKeyTypeMismatch)
		require.Contains(t, err.Error(), "sk expected: N actual: S")
	})

	t.Run("empty_value_keeps_generic_error", func(t *testing.T) {
		t.Parallel()

		ks := keySchema{HashKey: "id"}
		_, err := ks.GetKey(attrs, map[string]*types.Item{"id": {}})
		require.ErrorIs(t, err, ErrInvalidAtrributeValue)
		require.NotErrorIs(t, err, ErrKeyTypeMismatch)
	})

	t.Run("nil_value_returns_invalid_attribute", func(t *testing.T) {
		t.Parallel()

		ks := keySchema{HashKey: "id"}
		require.NotPanics(t, func() {
			_, err := ks.GetKey(attrs, map[string]*types.Item{"id": nil})
			require.ErrorIs(t, err, ErrInvalidAtrributeValue)
		})
	})

	t.Run("secondary_keeps_generic_error", func(t *testing.T) {
		t.Parallel()

		ks := keySchema{HashKey: "id", Secondary: true}
		_, err := ks.GetKey(attrs, map[string]*types.Item{"id": {N: new("1")}})
		require.ErrorIs(t, err, ErrInvalidAtrributeValue)
	})
}

func TestItemTypeName(t *testing.T) {
	t.Parallel()

	cases := map[string]*types.Item{
		"S":    {S: new("a")},
		"N":    {N: new("1")},
		"B":    {B: []byte("b")},
		"BOOL": {BOOL: new(true)},
		"NULL": {NULL: new(true)},
		"SS":   {SS: []*string{new("a")}},
		"NS":   {NS: []*string{new("1")}},
		"BS":   {BS: [][]byte{[]byte("b")}},
		"L":    {L: []*types.Item{}},
		"M":    {M: map[string]*types.Item{}},
	}

	for want, item := range cases {
		require.Equal(t, want, itemTypeName(item))
	}

	require.Equal(t, "", itemTypeName(&types.Item{}))
}

func TestItemTypeNameNil(t *testing.T) {
	t.Parallel()

	require.Equal(t, "", itemTypeName(nil))
}
