package server

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/types"
)

func TestAttributeValueMarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		av   *AttributeValue
		want string
	}{
		{
			name: "empty_list",
			av:   &AttributeValue{L: []*AttributeValue{}},
			want: `{"L":[]}`,
		},
		{
			name: "empty_map",
			av:   &AttributeValue{M: map[string]*AttributeValue{}},
			want: `{"M":{}}`,
		},
		{
			name: "empty_map_inside_list",
			av:   &AttributeValue{L: []*AttributeValue{{M: map[string]*AttributeValue{}}}},
			want: `{"L":[{"M":{}}]}`,
		},
		{
			name: "empty_list_inside_map",
			av:   &AttributeValue{M: map[string]*AttributeValue{"l": {L: []*AttributeValue{}}}},
			want: `{"M":{"l":{"L":[]}}}`,
		},
		{
			name: "string",
			av:   &AttributeValue{S: aws.String("")},
			want: `{"S":""}`,
		},
		{
			name: "number",
			av:   &AttributeValue{N: aws.String("1")},
			want: `{"N":"1"}`,
		},
		{
			name: "null",
			av:   &AttributeValue{NULL: aws.Bool(true)},
			want: `{"NULL":true}`,
		},
		{
			name: "bool_false",
			av:   &AttributeValue{BOOL: aws.Bool(false)},
			want: `{"BOOL":false}`,
		},
		{
			name: "empty_string_set_omitted",
			av:   &AttributeValue{SS: []*string{}},
			want: `{}`,
		},
		{
			name: "nil_list_and_map_omitted",
			av:   &AttributeValue{},
			want: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := json.Marshal(tt.av)
			require.NoError(t, err)
			require.JSONEq(t, tt.want, string(got))

			// Values (not only pointers) serialize the same way.
			got, err = json.Marshal(*tt.av)
			require.NoError(t, err)
			require.JSONEq(t, tt.want, string(got))
		})
	}
}

func TestAttributeValueJSONRoundTripKeepsEmptyMapAndList(t *testing.T) {
	t.Parallel()

	in := `{"info":{"M":{}},"local":{"L":[]},"nested":{"L":[{"M":{}}]}}`

	var item map[string]*AttributeValue
	require.NoError(t, json.Unmarshal([]byte(in), &item))

	got, err := json.Marshal(item)
	require.NoError(t, err)
	require.JSONEq(t, in, string(got))
}

func TestMapperKeepsEmptyMapAndList(t *testing.T) {
	t.Parallel()

	av := &AttributeValue{M: map[string]*AttributeValue{
		"info":   {M: map[string]*AttributeValue{}},
		"local":  {L: []*AttributeValue{}},
		"nested": {L: []*AttributeValue{{M: map[string]*AttributeValue{}}}},
	}}

	it := mapAttributeValueToTypes(av)
	require.Equal(t, &types.Item{M: map[string]*types.Item{
		"info":   {M: map[string]*types.Item{}},
		"local":  {L: []*types.Item{}},
		"nested": {L: []*types.Item{{M: map[string]*types.Item{}}}},
	}}, it)

	require.Equal(t, av, mapTypesToAttributeValue(it))
}

// BenchmarkAttributeValueMarshalJSON guards the response encoding hot path: every
// Get/Query/Scan/Batch response over HTTP is encoded this way.
func BenchmarkAttributeValueMarshalJSON(b *testing.B) {
	item := map[string]*AttributeValue{}
	for i := range 20 {
		item[fmt.Sprintf("a%d", i)] = &AttributeValue{M: map[string]*AttributeValue{
			"s": {S: aws.String("value")},
			"l": {L: []*AttributeValue{{N: aws.String("1")}, {M: map[string]*AttributeValue{}}}},
		}}
	}

	b.ReportAllocs()

	for b.Loop() {
		if _, err := json.Marshal(item); err != nil {
			b.Fatal(err)
		}
	}
}
