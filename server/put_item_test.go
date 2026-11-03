package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestServerPutItemReturnValues(t *testing.T) {
	c := require.New(t)
	srv := NewServer()
	ctx := context.Background()

	_, err := srv.client.CreateTable(ctx, &CreateTableInput{
		TableName: aws.String("pokemons"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	c.NoError(err)

	put := func(id, name string, rv ddbtypes.ReturnValue) *PutItemOutput {
		out, perr := srv.client.PutItem(ctx, &PutItemInput{
			TableName: aws.String("pokemons"),
			Item: map[string]*AttributeValue{
				"id":   {S: aws.String(id)},
				"name": {S: aws.String(name)},
			},
			ReturnValues: rv,
		})
		c.NoError(perr)

		return out
	}

	c.Empty(put("025", "Pichu", "").Attributes)
	c.Empty(put("025", "Pikachu", ddbtypes.ReturnValueNone).Attributes)
	c.Equal(map[string]*AttributeValue{
		"id":   {S: aws.String("025")},
		"name": {S: aws.String("Pikachu")},
	}, put("025", "Raichu", ddbtypes.ReturnValueAllOld).Attributes)
	c.Empty(put("026", "Meowth", ddbtypes.ReturnValueAllOld).Attributes)

	moves := map[string]*AttributeValue{
		"id":    {S: aws.String("004")},
		"moves": {L: []*AttributeValue{{S: aws.String("ember")}}},
	}

	_, err = srv.client.PutItem(ctx, &PutItemInput{TableName: aws.String("pokemons"), Item: moves})
	c.NoError(err)
	c.Equal(moves, put("004", "Charmeleon", ddbtypes.ReturnValueAllOld).Attributes)

	for _, rv := range []ddbtypes.ReturnValue{
		ddbtypes.ReturnValueAllNew,
		ddbtypes.ReturnValueUpdatedOld,
		ddbtypes.ReturnValueUpdatedNew,
		"BOGUS",
	} {
		_, err = srv.client.PutItem(ctx, &PutItemInput{
			TableName:    aws.String("pokemons"),
			Item:         map[string]*AttributeValue{"id": {S: aws.String("999")}},
			ReturnValues: rv,
		})
		c.Error(err, string(rv))
		c.Contains(err.Error(), "ValidationException")
		c.Contains(err.Error(), "Return values set to invalid value")
	}
}
