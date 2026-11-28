package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConditionalCheckFailedExceptionMethods(t *testing.T) {
	t.Parallel()

	exVal := ConditionalCheckFailedException{
		MessageText: "condition was not met",
		RespMetadata: ResponseMetadata{
			StatusCode: 400,
			RequestID:  "req-abc",
		},
		Item: map[string]*Item{
			"id": {S: new("1")},
		},
	}

	require.NotEmpty(t, exVal.String())
	require.Equal(t, exVal.String(), exVal.GoString())

	ex := &exVal
	require.Equal(t, "ConditionalCheckFailedException", ex.Code())
	require.Equal(t, "condition was not met", ex.Message())
	require.Nil(t, ex.OrigErr())
	require.Equal(t, "ConditionalCheckFailedException: condition was not met", ex.Error())
	require.Equal(t, 400, ex.StatusCode())
	require.Equal(t, "req-abc", ex.RequestID())

	emptyMsg := &ConditionalCheckFailedException{}
	require.Equal(t, "", emptyMsg.Message())
}

func TestToStringAndStringValue(t *testing.T) {
	t.Parallel()

	p := new("x")
	require.NotNil(t, p)
	require.Equal(t, "x", *p)
	require.Equal(t, "x", StringValue(p))
	require.Equal(t, "", StringValue(nil))
}

func TestHasUnsupportedUpdateTableChanges(t *testing.T) {
	type request struct {
		TableName                   *string
		AttributeDefinitions        []int
		BillingMode                 string
		DeletionProtectionEnabled   *bool
		GlobalSecondaryIndexUpdates []int
		OnDemandThroughput          *int
		ProvisionedThroughput       *int
		TableClass                  string
	}

	one := 1

	require.False(t, HasUnsupportedUpdateTableChanges(&request{}))
	require.False(t, HasUnsupportedUpdateTableChanges(&request{BillingMode: "PROVISIONED", OnDemandThroughput: &one, AttributeDefinitions: []int{1}}))
	require.True(t, HasUnsupportedUpdateTableChanges(&request{TableClass: "STANDARD"}))
	require.False(t, HasUnsupportedUpdateTableChanges(nil))
}

func TestHasUnsupportedUpdateTableChangesCountsOnlyRealSettings(t *testing.T) {
	type request struct {
		TableName      *string
		BillingMode    string
		ReplicaUpdates []int
		Tags           map[string]string
		TableClass     string
		Stream         *int
		ignored        string
	}

	one := 1

	tests := []struct {
		name    string
		request any
		want    bool
	}{
		{"Empty", &request{}, false},
		{"EmptySlice", &request{ReplicaUpdates: []int{}}, false},
		{"NonEmptySlice", &request{ReplicaUpdates: []int{1}}, true},
		{"EmptyMap", &request{Tags: map[string]string{}}, false},
		{"NonEmptyMap", &request{Tags: map[string]string{"a": "b"}}, true},
		{"EmptyPointerToZero", &request{Stream: new(int)}, true},
		{"Pointer", &request{Stream: &one}, true},
		{"ZeroString", &request{TableClass: ""}, false},
		{"String", &request{TableClass: "STANDARD"}, true},
		{"ModelledOnly", &request{TableName: new("t"), BillingMode: "PROVISIONED"}, false},
		{"UnexportedIgnored", &request{ignored: "x"}, false},
		{"Nil", nil, false},
		{"NilPointer", (*request)(nil), false},
		{"Value", request{TableClass: "STANDARD"}, false},
		{"PointerToNonStruct", &one, false},
		{"PointerToSlice", &[]int{1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				require.Equal(t, tt.want, HasUnsupportedUpdateTableChanges(tt.request))
			})
		})
	}
}
