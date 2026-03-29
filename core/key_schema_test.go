package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestValidatePrimaryKeyMap(t *testing.T) {
	t.Parallel()

	hashOnly := keySchema{HashKey: "id", RangeKey: "", Secondary: false}
	composite := keySchema{HashKey: "pk", RangeKey: "sk", Secondary: false}

	t.Run("hash_only_empty", func(t *testing.T) {
		t.Parallel()
		err := hashOnly.validatePrimaryKeyMap(map[string]*types.Item{})
		require.ErrorIs(t, err, errInvalidKeyConditionCount)
	})

	t.Run("hash_only_ok", func(t *testing.T) {
		t.Parallel()
		err := hashOnly.validatePrimaryKeyMap(map[string]*types.Item{
			"id": {S: new("1")},
		})
		require.NoError(t, err)
	})

	t.Run("hash_only_wrong_name_same_len", func(t *testing.T) {
		t.Parallel()
		err := hashOnly.validatePrimaryKeyMap(map[string]*types.Item{
			"wrong": {S: new("1")},
		})
		require.ErrorIs(t, err, errInvalidKeyConditionCount)
	})

	t.Run("hash_only_extra_attr", func(t *testing.T) {
		t.Parallel()
		err := hashOnly.validatePrimaryKeyMap(map[string]*types.Item{
			"id":    {S: new("1")},
			"extra": {S: new("x")},
		})
		require.ErrorIs(t, err, errInvalidKeyConditionCount)
	})

	t.Run("composite_missing_range", func(t *testing.T) {
		t.Parallel()
		err := composite.validatePrimaryKeyMap(map[string]*types.Item{
			"pk": {S: new("a")},
		})
		require.ErrorIs(t, err, errInvalidKeyConditionCount)
	})

	t.Run("composite_ok", func(t *testing.T) {
		t.Parallel()
		err := composite.validatePrimaryKeyMap(map[string]*types.Item{
			"pk": {S: new("a")},
			"sk": {S: new("b")},
		})
		require.NoError(t, err)
	})

	t.Run("secondary_skipped", func(t *testing.T) {
		t.Parallel()
		gsi := keySchema{HashKey: "g", RangeKey: "", Secondary: true}
		err := gsi.validatePrimaryKeyMap(map[string]*types.Item{})
		require.NoError(t, err)
	})
}
