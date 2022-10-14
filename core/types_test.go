package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestItemValue(t *testing.T) {
	c := require.New(t)

	v, err := getItemValue(map[string]types.Item{"S": {S: types.ToString("test")}}, "S", "S")
	c.NoError(err)
	c.Equal(v, "test")

	booleanVal := true
	v, err = getItemValue(map[string]types.Item{"BOOL": {BOOL: &booleanVal}}, "BOOL", "BOOL")
	c.NoError(err)
	c.Equal(v, &booleanVal)

	v, err = getItemValue(map[string]types.Item{"SS": {SS: []*string{types.ToString("t1"), types.ToString("t2")}}}, "SS", "SS")
	c.NoError(err)
	c.Equal(v, []*string{types.ToString("t1"), types.ToString("t2")})

	v, err = getItemValue(map[string]types.Item{"N": {N: types.ToString("123.45")}}, "N", "N")
	c.NoError(err)
	c.Equal(v, "123.45")

	v, err = getItemValue(map[string]types.Item{"B": {B: []byte("dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk")}}, "B", "B")
	c.NoError(err)
	c.Equal(v, []byte("dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"))

	v, err = getItemValue(map[string]types.Item{"L": {L: []types.Item{{S: types.ToString("Cookie")}}}}, "L", "L")
	c.NoError(err)
	c.Equal(v, []types.Item{{S: types.ToString("Cookie")}})

	v, err = getItemValue(map[string]types.Item{"M": {M: map[string]types.Item{"N": {N: types.ToString("123.45")}}}}, "M", "M")
	c.NoError(err)
	c.Equal(v, map[string]types.Item{"N": {N: types.ToString("123.45")}})

	v, err = getItemValue(map[string]types.Item{"BS": {BS: [][]byte{123: []byte("x"), []byte("y"), []byte("z")}}}, "BS", "BS")
	c.NoError(err)
	c.Equal(v, [][]byte{123: []byte("x"), []byte("y"), []byte("z")})

	v, err = getItemValue(map[string]types.Item{"NS": {NS: []*string{types.ToString("t1"), types.ToString("t2")}}}, "NS", "NS")
	c.NoError(err)
	c.Equal(v, []*string{types.ToString("t1"), types.ToString("t2")})
}

func TestFailedItemValue(t *testing.T) {
	c := require.New(t)

	_, err := getItemValue(map[string]types.Item{"D": {S: types.ToString("test")}}, "S", "S")
	c.Contains(err.Error(), errMissingField.Error())

	_, err = getItemValue(map[string]types.Item{"S": {S: types.ToString("test")}}, "S", "n")
	c.Contains(err.Error(), ErrInvalidAtrributeValue.Error())
}

func TestCopyItem(t *testing.T) {
	c := require.New(t)

	cItem := copyItem(map[string]types.Item{"str": {N: types.ToString("test")}})
	c.Equal(cItem, map[string]types.Item{"str": {N: types.ToString("test")}})
}

func TestMapToDynamoDBType(t *testing.T) {
	c := require.New(t)

	r := mapToDynamoDBType("str")
	c.Equal("S", r)

	r = mapToDynamoDBType(true)
	c.Equal("BOOL", r)

	r = mapToDynamoDBType(1)
	c.Equal("N", r)

	r = mapToDynamoDBType(1.1)
	c.Equal("N", r)

	r = mapToDynamoDBType(int64(1))
	c.Equal("N", r)

	r = mapToDynamoDBType([]byte{1, 2, 3})
	c.Equal("B", r)

	r = mapToDynamoDBType([]int{1, 2, 3})
	c.Equal("L", r)

	r = mapToDynamoDBType(nil)
	c.Equal("NULL", r)

	r = mapToDynamoDBType(map[string]string{})
	c.Equal("M", r)
}

func TestGetGoValue(t *testing.T) {
	c := require.New(t)
	boolFalse := false

	all := types.Item{
		B:    []byte{1},
		BOOL: &boolFalse,
		BS:   [][]byte{{123}},
		L: []types.Item{
			{N: types.ToString("1")}, {S: types.ToString("a")},
		},
		M: map[string]types.Item{
			"f": {
				S: types.ToString("a"),
			},
		},
		N:  types.ToString("1"),
		NS: []*string{types.ToString("1"), types.ToString("2")},
		S:  types.ToString("a"),
		SS: []*string{types.ToString("a"), types.ToString("b")},
	}

	goVal, ok := getGoValue(all, "B")
	c.True(ok)
	c.Equal([]byte{1}, goVal)

	goVal, ok = getGoValue(all, "BOOL")
	c.True(ok)
	b := false
	c.Equal(&b, goVal)

	goVal, ok = getGoValue(all, "BS")
	c.True(ok)
	c.Equal([][]byte{{123}}, goVal)

	goVal, ok = getGoValue(all, "L")
	c.True(ok)

	sliceVal, ok := goVal.([]types.Item)
	c.True(ok)
	c.Len(sliceVal, 2)

	goVal, ok = getGoValue(all, "M")
	c.True(ok)

	mapVal, ok := goVal.(map[string]types.Item)
	c.True(ok)
	c.Equal("a", types.StringValue(mapVal["f"].S))

	goVal, ok = getGoValue(all, "N")
	c.True(ok)
	c.Equal("1", goVal)

	goVal, ok = getGoValue(all, "NS")
	c.True(ok)

	nsVal, ok := goVal.([]*string)
	c.True(ok)
	c.Len(nsVal, 2)

	goVal, ok = getGoValue(all, "S")
	c.True(ok)
	c.Equal("a", goVal)

	goVal, ok = getGoValue(all, "SS")
	c.True(ok)

	ssVal, ok := goVal.([]*string)
	c.True(ok)
	c.Len(ssVal, 2)
}

func BenchmarkMapToDynamoDBType(b *testing.B) {
	c := require.New(b)

	for i := 0; i < b.N; i++ {
		r := mapToDynamoDBType([]int{1, 2, 3})
		c.Equal("L", r)
	}
}
