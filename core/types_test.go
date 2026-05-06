package core

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

func TestItemValue(t *testing.T) {
	c := require.New(t)

	v, err := getItemValue(map[string]*types.Item{"S": {S: new("test")}}, "S", "S")
	c.NoError(err)
	c.Equal(v, "test")

	booleanVal := true
	v, err = getItemValue(map[string]*types.Item{"BOOL": {BOOL: &booleanVal}}, "BOOL", "BOOL")
	c.NoError(err)
	c.Equal(v, &booleanVal)

	v, err = getItemValue(map[string]*types.Item{"SS": {SS: []*string{new("t1"), new("t2")}}}, "SS", "SS")
	c.NoError(err)
	c.Equal(v, []*string{new("t1"), new("t2")})

	v, err = getItemValue(map[string]*types.Item{"N": {N: new("123.45")}}, "N", "N")
	c.NoError(err)
	c.Equal(v, "123.45")

	v, err = getItemValue(map[string]*types.Item{"B": {B: []byte("dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk")}}, "B", "B")
	c.NoError(err)
	c.Equal(v, []byte("dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"))

	v, err = getItemValue(map[string]*types.Item{"L": {L: []*types.Item{{S: new("Cookie")}}}}, "L", "L")
	c.NoError(err)
	c.Equal(v, []*types.Item{{S: new("Cookie")}})

	v, err = getItemValue(map[string]*types.Item{"M": {M: map[string]*types.Item{"N": {N: new("123.45")}}}}, "M", "M")
	c.NoError(err)
	c.Equal(v, map[string]*types.Item{"N": {N: new("123.45")}})

	v, err = getItemValue(map[string]*types.Item{"BS": {BS: [][]byte{123: []byte("x"), []byte("y"), []byte("z")}}}, "BS", "BS")
	c.NoError(err)
	c.Equal(v, [][]byte{123: []byte("x"), []byte("y"), []byte("z")})

	v, err = getItemValue(map[string]*types.Item{"NS": {NS: []*string{new("t1"), new("t2")}}}, "NS", "NS")
	c.NoError(err)
	c.Equal(v, []*string{new("t1"), new("t2")})
}

func TestFailedItemValue(t *testing.T) {
	c := require.New(t)

	_, err := getItemValue(map[string]*types.Item{"D": {S: new("test")}}, "S", "S")
	c.Contains(err.Error(), errMissingField.Error())

	_, err = getItemValue(map[string]*types.Item{"S": {S: new("test")}}, "S", "n")
	c.Contains(err.Error(), ErrInvalidAtrributeValue.Error())
}

func TestCopyItem(t *testing.T) {
	c := require.New(t)

	cItem := copyItem(map[string]*types.Item{"str": {N: new("test")}})
	c.Equal(cItem, map[string]*types.Item{"str": {N: new("test")}})
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

	all := &types.Item{
		B:    []byte{1},
		BOOL: &boolFalse,
		BS:   [][]byte{{123}},
		L: []*types.Item{
			{N: new("1")}, {S: new("a")},
		},
		M: map[string]*types.Item{
			"f": {
				S: new("a"),
			},
		},
		N:  new("1"),
		NS: []*string{new("1"), new("2")},
		S:  new("a"),
		SS: []*string{new("a"), new("b")},
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

	sliceVal, ok := goVal.([]*types.Item)
	c.True(ok)
	c.Len(sliceVal, 2)

	goVal, ok = getGoValue(all, "M")
	c.True(ok)

	mapVal, ok := goVal.(map[string]*types.Item)
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

func TestCloneBoolPtr(t *testing.T) {
	c := require.New(t)

	c.Nil(cloneBoolPtr(nil))

	tr := true
	out := cloneBoolPtr(&tr)
	c.NotNil(out)
	c.True(*out)

	f := false
	outF := cloneBoolPtr(&f)
	c.NotNil(outF)
	c.False(*outF)
}

func TestDeepCopyStringPtrSliceAndByteSliceSlice(t *testing.T) {
	c := require.New(t)

	c.Nil(deepCopyStringPtrSlice(nil))
	c.Nil(deepCopyStringPtrSlice([]*string{}))

	a1, a2 := "a", "b"
	src := []*string{&a1, nil, &a2}
	cp := deepCopyStringPtrSlice(src)
	c.Len(cp, 3)
	c.Equal(&a1, src[0])
	c.NotSame(src[0], cp[0])
	c.Equal(*src[0], *cp[0])
	c.Nil(cp[1])
	c.Equal(*src[2], *cp[2])

	c.Nil(deepCopyByteSliceSlice(nil))
	c.Nil(deepCopyByteSliceSlice([][]byte{}))

	raw := [][]byte{{1, 2}, {3, 4}}
	bcp := deepCopyByteSliceSlice(raw)
	c.Len(bcp, 2)
	c.Equal(raw[0], bcp[0])
	c.NotSame(&raw[0][0], &bcp[0][0])
}

func TestDeepCopyTypesItem_nestedAndScalars(t *testing.T) {
	c := require.New(t)

	c.Nil(deepCopyTypesItem(nil))

	sub := "nested"
	tr := true
	f := false
	it := &types.Item{
		BOOL: &tr,
		NULL: &f,
		S:    new("s"),
		N:    new("1"),
		B:    []byte{9},
		BS:   [][]byte{{1}},
		SS:   []*string{new("x")},
		NS:   []*string{new("1")},
		L: []*types.Item{
			{S: new("leaf")},
		},
		M: map[string]*types.Item{
			"sub": {S: &sub},
		},
	}

	got := deepCopyTypesItem(it)
	c.NotSame(it, got)
	c.NotSame(it.M["sub"], got.M["sub"])
	c.Equal(types.StringValue(it.S), types.StringValue(got.S))
	c.Equal(sub, types.StringValue(got.M["sub"].S))
	c.Equal(types.StringValue(it.L[0].S), types.StringValue(got.L[0].S))
}
