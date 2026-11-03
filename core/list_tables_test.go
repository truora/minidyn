package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestListTableNames(t *testing.T) {
	tables := map[string]*Table{"b": nil, "A": nil, "a": nil, "c": nil}
	limit := func(n int32) *int32 { return &n }

	names, last, err := ListTableNames(tables, "", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"A", "a", "b", "c"}, names)
	require.Nil(t, last)

	names, last, err = ListTableNames(tables, "", limit(2))
	require.NoError(t, err)
	require.Equal(t, []string{"A", "a"}, names)
	require.Equal(t, "a", *last)

	names, last, err = ListTableNames(tables, "a", limit(2))
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c"}, names)
	require.Nil(t, last)

	names, last, err = ListTableNames(tables, "c", nil)
	require.NoError(t, err)
	require.NotNil(t, names)
	require.Empty(t, names)
	require.Nil(t, last)

	for n, msg := range map[int32]string{
		0:   "Limit must be greater than or equal to 1",
		101: "Limit in ListTables must be <= 100",
	} {
		_, _, err = ListTableNames(tables, "", limit(n))

		var tErr types.Error
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, "ValidationException", tErr.Code())
		require.Equal(t, msg, tErr.Message())
	}
}
