package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestPutItemReturnValues(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)
	ctx := context.Background()

	c.NoError(ensurePokemonTable(client))

	put := func(id, name string, rv dynamodbtypes.ReturnValue) *dynamodb.PutItemOutput {
		out, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]dynamodbtypes.AttributeValue{
				"id":   &dynamodbtypes.AttributeValueMemberS{Value: id},
				"name": &dynamodbtypes.AttributeValueMemberS{Value: name},
			},
			ReturnValues: rv,
		})
		c.NoError(err)

		return out
	}

	c.Empty(put("025", "Pichu", "").Attributes)
	c.Empty(put("025", "Pikachu", dynamodbtypes.ReturnValueNone).Attributes)
	c.Equal(map[string]dynamodbtypes.AttributeValue{
		"id":   &dynamodbtypes.AttributeValueMemberS{Value: "025"},
		"name": &dynamodbtypes.AttributeValueMemberS{Value: "Pikachu"},
	}, put("025", "Raichu", dynamodbtypes.ReturnValueAllOld).Attributes)
	c.Empty(put("026", "Meowth", dynamodbtypes.ReturnValueAllOld).Attributes)
}

func TestPutItemInvalidReturnValues(t *testing.T) {
	c := require.New(t)
	client := setupClient(tableName)
	ctx := context.Background()

	c.NoError(ensurePokemonTable(client))

	for _, rv := range []dynamodbtypes.ReturnValue{
		dynamodbtypes.ReturnValueAllNew,
		dynamodbtypes.ReturnValueUpdatedOld,
		dynamodbtypes.ReturnValueUpdatedNew,
		"BOGUS",
	} {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]dynamodbtypes.AttributeValue{
				"id": &dynamodbtypes.AttributeValueMemberS{Value: "025"},
			},
			ReturnValues: rv,
		})
		c.Error(err, string(rv))
		c.Contains(err.Error(), "ValidationException")
		c.Contains(err.Error(), "Return values set to invalid value")
	}
}
