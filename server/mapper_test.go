package server

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/types"
)

func TestMapTypesToDDBAttributeValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		it   *types.Item
		want ddbtypes.AttributeValue
	}{
		{
			name: "nil",
			it:   nil,
			want: nil,
		},
		{
			name: "binary",
			it:   &types.Item{B: []byte{0xab, 0xcd}},
			want: &ddbtypes.AttributeValueMemberB{Value: []byte{0xab, 0xcd}},
		},
		{
			name: "bool",
			it:   &types.Item{BOOL: aws.Bool(true)},
			want: &ddbtypes.AttributeValueMemberBOOL{Value: true},
		},
		{
			name: "binary_set",
			it:   &types.Item{BS: [][]byte{{1}, {2}}},
			want: &ddbtypes.AttributeValueMemberBS{Value: [][]byte{{1}, {2}}},
		},
		{
			name: "number",
			it:   &types.Item{N: aws.String("42")},
			want: &ddbtypes.AttributeValueMemberN{Value: "42"},
		},
		{
			name: "number_set",
			it:   &types.Item{NS: []*string{aws.String("1"), aws.String("2")}},
			want: &ddbtypes.AttributeValueMemberNS{Value: []string{"1", "2"}},
		},
		{
			name: "string",
			it:   &types.Item{S: aws.String("hello")},
			want: &ddbtypes.AttributeValueMemberS{Value: "hello"},
		},
		{
			name: "string_set",
			it:   &types.Item{SS: []*string{aws.String("a"), aws.String("b")}},
			want: &ddbtypes.AttributeValueMemberSS{Value: []string{"a", "b"}},
		},
		{
			name: "list_nested",
			it: &types.Item{L: []*types.Item{
				{S: aws.String("x")},
				{N: aws.String("0")},
			}},
			want: &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{
				&ddbtypes.AttributeValueMemberS{Value: "x"},
				&ddbtypes.AttributeValueMemberN{Value: "0"},
			}},
		},
		{
			name: "map_nested",
			it: &types.Item{M: map[string]*types.Item{
				"k": {BOOL: aws.Bool(false)},
			}},
			want: &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
				"k": &ddbtypes.AttributeValueMemberBOOL{Value: false},
			}},
		},
		{
			name: "default_null",
			it:   &types.Item{},
			want: &ddbtypes.AttributeValueMemberNULL{Value: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := mapTypesToDDBAttributeValue(tt.it)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapTypesMapToDDBAttributeValue(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		require.Nil(t, mapTypesMapToDDBAttributeValue(map[string]*types.Item{}))
	})

	t.Run("non_empty", func(t *testing.T) {
		t.Parallel()
		in := map[string]*types.Item{
			"id": {S: aws.String("1")},
		}
		got := mapTypesMapToDDBAttributeValue(in)
		require.Len(t, got, 1)
		require.Equal(t, &ddbtypes.AttributeValueMemberS{Value: "1"}, got["id"])
	})
}

func TestToStringPtrsAndFromStringPtrs(t *testing.T) {
	t.Parallel()

	t.Run("toStringPtrs_empty", func(t *testing.T) {
		t.Parallel()
		require.Nil(t, toStringPtrs(nil))
		require.Nil(t, toStringPtrs([]string{}))
	})

	t.Run("toStringPtrs_mixed_empty_strings", func(t *testing.T) {
		t.Parallel()
		out := toStringPtrs([]string{"", "z"})
		require.Len(t, out, 2)
		require.Nil(t, out[0])
		require.Equal(t, "z", aws.ToString(out[1]))
	})

	t.Run("fromStringPtrs_empty", func(t *testing.T) {
		t.Parallel()
		require.Nil(t, fromStringPtrs(nil))
		require.Nil(t, fromStringPtrs([]*string{}))
	})

	t.Run("fromStringPtrs_with_nil_element", func(t *testing.T) {
		t.Parallel()
		out := fromStringPtrs([]*string{nil, aws.String("a")})
		require.Equal(t, []string{"", "a"}, out)
	})
}

func TestMapAttributeValueToTypes_nil(t *testing.T) {
	t.Parallel()
	require.Nil(t, mapAttributeValueToTypes(nil))
}

func TestMapTypesToAttributeValue_nil(t *testing.T) {
	t.Parallel()
	require.Nil(t, mapTypesToAttributeValue(nil))
}

func TestMapProjectionAndProvisionedThroughput_nil(t *testing.T) {
	t.Parallel()
	require.Nil(t, mapProjection(nil))
	require.Nil(t, mapProvisionedThroughput(nil))
}

func TestMapGSIUpdateBranches(t *testing.T) {
	t.Parallel()

	out := mapGSIUpdate([]ddbtypes.GlobalSecondaryIndexUpdate{
		{Create: nil, Delete: nil, Update: nil},
	})
	require.Len(t, out, 1)
	require.Nil(t, out[0].Create)
	require.Nil(t, out[0].Delete)
	require.Nil(t, out[0].Update)
}

func TestMapCreateDeleteUpdateGSI_nil(t *testing.T) {
	t.Parallel()
	require.Nil(t, mapCreateGSI(nil))
	require.Nil(t, mapDeleteGSI(nil))
	require.Nil(t, mapUpdateGSI(nil))
}

func TestMapTableDescriptionToDDB_nil(t *testing.T) {
	t.Parallel()
	require.Nil(t, mapTableDescriptionToDDB(nil))
}

func TestMapProvisionedThroughput_nonNil(t *testing.T) {
	t.Parallel()
	out := mapProvisionedThroughput(&ddbtypes.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(10),
		WriteCapacityUnits: aws.Int64(20),
	})
	require.NotNil(t, out)
	require.Equal(t, int64(10), out.ReadCapacityUnits)
	require.Equal(t, int64(20), out.WriteCapacityUnits)
}

func TestMapDeleteAndUpdateGSI_nonNil(t *testing.T) {
	t.Parallel()

	del := mapDeleteGSI(&ddbtypes.DeleteGlobalSecondaryIndexAction{
		IndexName: aws.String("missing-gsi"),
	})
	require.NotNil(t, del)
	require.Equal(t, "missing-gsi", aws.ToString(del.IndexName))

	upd := mapUpdateGSI(&ddbtypes.UpdateGlobalSecondaryIndexAction{
		IndexName: aws.String("some-gsi"),
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(2),
		},
	})
	require.NotNil(t, upd)
	require.Equal(t, "some-gsi", aws.ToString(upd.IndexName))
	require.NotNil(t, upd.ProvisionedThroughput)
	require.Equal(t, int64(1), upd.ProvisionedThroughput.ReadCapacityUnits)
	require.Equal(t, int64(2), upd.ProvisionedThroughput.WriteCapacityUnits)
}
