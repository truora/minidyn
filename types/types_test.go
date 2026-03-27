package types

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/private/protocol"
	"github.com/stretchr/testify/require"
)

func TestConditionalCheckFailedExceptionMethods(t *testing.T) {
	t.Parallel()

	exVal := ConditionalCheckFailedException{
		MessageText: "condition was not met",
		RespMetadata: protocol.ResponseMetadata{
			StatusCode: 400,
			RequestID:  "req-abc",
		},
		Item: map[string]*Item{
			"id": {S: aws.String("1")},
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

	p := ToString("x")
	require.NotNil(t, p)
	require.Equal(t, "x", *p)
	require.Equal(t, "x", StringValue(p))
	require.Equal(t, "", StringValue(nil))
}
