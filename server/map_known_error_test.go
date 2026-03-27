package server

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/types"
)

func TestMapKnownError(t *testing.T) {
	t.Parallel()

	require.Nil(t, mapKnownError(nil))

	plain := errors.New("plain")
	require.Equal(t, plain, mapKnownError(plain))

	gen := types.NewError("SomeCode", "generic message", nil)
	got := mapKnownError(gen)
	var api *smithy.GenericAPIError
	require.True(t, errors.As(got, &api))
	require.Equal(t, "SomeCode", api.ErrorCode())
	require.Equal(t, "generic message", api.ErrorMessage())

	rnf := types.NewError("ResourceNotFoundException", "missing resource", nil)
	got = mapKnownError(rnf)
	var rnfddb *ddbtypes.ResourceNotFoundException
	require.True(t, errors.As(got, &rnfddb))
	require.Equal(t, "missing resource", aws.ToString(rnfddb.Message))

	ccf := types.NewError("ConditionalCheckFailedException", "cond failed", nil)
	got = mapKnownError(ccf)
	var ccfddb *ddbtypes.ConditionalCheckFailedException
	require.True(t, errors.As(got, &ccfddb))
	require.Nil(t, ccfddb.Item)

	withItem := &types.ConditionalCheckFailedException{
		MessageText: "failed",
		Item: map[string]*types.Item{
			"id": {S: aws.String("42")},
		},
	}
	got = mapKnownError(withItem)
	require.True(t, errors.As(got, &ccfddb))
	require.NotNil(t, ccfddb.Item)
	idAv, ok := ccfddb.Item["id"]
	require.True(t, ok)
	require.Equal(t, "42", idAv.(*ddbtypes.AttributeValueMemberS).Value)
}
