package core

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	"github.com/truora/minidyn/interpreter"
	"github.com/truora/minidyn/types"
)

// Update returns every expression error as a types.Error ValidationException, which does not
// unwrap: the front ends map it with mapKnownError and need no interpreter.ErrSyntaxError check.
func TestUpdateExpressionErrorsAreValidationExceptions(t *testing.T) {
	tests := map[string]struct {
		update    string
		condition *string
	}{
		"update syntax":    {update: "SET = :one"},
		"condition syntax": {update: "SET n = :one", condition: aws.String("n = = :one")},
		"operand type":     {update: "SET n = s + :one"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			table, err := createPokemonTable()
			require.NoError(t, err)

			key := map[string]*types.Item{"id": {S: aws.String("a")}, "name": {S: aws.String("b")}}
			item := map[string]*types.Item{"id": key["id"], "name": key["name"], "s": {S: aws.String("x")}}

			_, err = table.Put(&types.PutItemInput{Item: item, TableName: &table.Name})
			require.NoError(t, err)

			_, err = table.Update(&types.UpdateItemInput{
				TableName:                 &table.Name,
				Key:                       key,
				UpdateExpression:          tt.update,
				ConditionExpression:       tt.condition,
				ExpressionAttributeValues: map[string]*types.Item{":one": {N: aws.String("1")}},
			})
			require.Error(t, err)

			var typed types.Error
			require.True(t, errors.As(err, &typed), "%T: %v", err, err)
			require.Equal(t, "ValidationException", typed.Code())
			require.False(t, errors.Is(err, interpreter.ErrSyntaxError))
		})
	}
}
