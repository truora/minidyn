package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// errString returns a normalized error string for parity comparison, or "<nil>"
// when no error was returned.
func errString(err error) string {
	if err == nil {
		return "<nil>"
	}

	return normalizeSDKErrorString(err.Error())
}

// TestE2E_UndeclaredExpressionAttributeName verifies that referencing an expression
// attribute name placeholder (#name) that is not declared in ExpressionAttributeNames
// returns a ValidationException, across every expression type.
func TestE2E_UndeclaredExpressionAttributeName(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T, client *dynamodb.Client) any
	}{
		{
			name: "UpdateExpression",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "u1", Type: "fire", Name: "Charmander"})

				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "u1"},
					},
					UpdateExpression: aws.String("SET #a = :v"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberS{Value: "x"},
					},
				})

				return errString(err)
			},
		},
		{
			name: "ConditionExpression",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)

				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName:           aws.String(parityPokemonTable),
					Item:                parityMarshalPokemon(t, parityPokemon{ID: "c1", Type: "water", Name: "Squirtle"}),
					ConditionExpression: aws.String("attribute_not_exists(#a)"),
				})

				return errString(err)
			},
		},
		{
			name: "FilterExpression",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)

				_, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName:        aws.String(parityPokemonTable),
					FilterExpression: aws.String("#a = :v"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberS{Value: "x"},
					},
				})

				return errString(err)
			},
		},
		{
			name: "KeyConditionExpression",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)

				_, err := client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(parityPokemonTable),
					KeyConditionExpression: aws.String("#a = :v"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberS{Value: "x"},
					},
				})

				return errString(err)
			},
		},
		{
			name: "ProjectionExpression",
			fn: func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				ctx := context.Background()
				parityCreatePokemonTable(ctx, t, client)
				parityCreatePokemon(ctx, t, client, parityPokemon{ID: "p1", Type: "grass", Name: "Bulbasaur"})

				_, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityPokemonTable),
					Key: map[string]dynamodbtypes.AttributeValue{
						"id": &dynamodbtypes.AttributeValueMemberS{Value: "p1"},
					},
					ProjectionExpression: aws.String("#a"),
				})

				return errString(err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, tt.fn)
		})
	}
}
