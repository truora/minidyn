package core

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/require"
)

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

	all := &dynamodb.AttributeValue{
		B:    []byte{1},
		BOOL: aws.Bool(false),
		BS:   [][]byte{{123}},
		L: []*dynamodb.AttributeValue{
			{N: aws.String("1")}, {S: aws.String("a")},
		},
		M: map[string]*dynamodb.AttributeValue{
			"f": &dynamodb.AttributeValue{
				S: aws.String("a"),
			},
		},
		N:  aws.String("1"),
		NS: []*string{aws.String("1"), aws.String("2")},
		S:  aws.String("a"),
		SS: []*string{aws.String("a"), aws.String("b")},
	}

	goVal, ok := getGoValue(all, "B")
	c.True(ok)
	c.Equal([]byte{1}, goVal)

	goVal, ok = getGoValue(all, "BOOL")
	c.True(ok)
	c.Equal(false, goVal)

	goVal, ok = getGoValue(all, "BS")
	c.True(ok)
	c.Equal([][]byte{{123}}, goVal)

	goVal, ok = getGoValue(all, "L")
	c.True(ok)

	sliceVal, ok := goVal.([]*dynamodb.AttributeValue)
	c.True(ok)
	c.Len(sliceVal, 2)

	goVal, ok = getGoValue(all, "M")
	c.True(ok)

	mapVal, ok := goVal.(map[string]*dynamodb.AttributeValue)
	c.True(ok)
	c.Equal("a", *mapVal["f"].S)

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
