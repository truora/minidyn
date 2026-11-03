package client

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestTransactionCanceledMessageListsEveryReason(t *testing.T) {
	ctx := context.Background()
	client := NewClient()

	require.NoError(t, AddTable(ctx, client, tableName, "id", ""))

	put := func(id string) dynamodbtypes.TransactWriteItem {
		return dynamodbtypes.TransactWriteItem{Put: &dynamodbtypes.Put{
			TableName: aws.String(tableName),
			Item:      map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: id}},
		}}
	}
	check := dynamodbtypes.TransactWriteItem{ConditionCheck: &dynamodbtypes.ConditionCheck{
		TableName:           aws.String(tableName),
		Key:                 map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: "404"}},
		ConditionExpression: aws.String("attribute_exists(id)"),
	}}

	tests := []struct {
		name    string
		items   []dynamodbtypes.TransactWriteItem
		message string
		codes   []string
	}{
		{
			name:    "failure last",
			items:   []dynamodbtypes.TransactWriteItem{put("001"), check},
			message: "Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed]",
			codes:   []string{"None", "ConditionalCheckFailed"},
		},
		{
			name:    "failure middle",
			items:   []dynamodbtypes.TransactWriteItem{put("001"), check, put("002")},
			message: "Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed, None]",
			codes:   []string{"None", "ConditionalCheckFailed", "None"},
		},
		{
			name:    "single item",
			items:   []dynamodbtypes.TransactWriteItem{check},
			message: "Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]",
			codes:   []string{"ConditionalCheckFailed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := require.New(t)

			_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: tt.items})

			var tce *dynamodbtypes.TransactionCanceledException
			c.ErrorAs(err, &tce)
			c.Equal(tt.message, aws.ToString(tce.Message))
			c.Len(tce.CancellationReasons, len(tt.codes))

			for i, code := range tt.codes {
				c.Equal(code, aws.ToString(tce.CancellationReasons[i].Code))

				if code == "None" {
					c.Nil(tce.CancellationReasons[i].Item)
				}

				if code == "ConditionalCheckFailed" {
					c.Equal("The conditional request failed.", aws.ToString(tce.CancellationReasons[i].Message))
				} else {
					c.Empty(aws.ToString(tce.CancellationReasons[i].Message))
				}
			}
		})
	}
}

func TestTransactionCanceledItemOnlyOnFailedIndex(t *testing.T) {
	ctx := context.Background()
	cli := NewClient()

	require.NoError(t, AddTable(ctx, cli, tableName, "id", ""))

	tn := tableName

	key := func(id string) map[string]dynamodbtypes.AttributeValue {
		return map[string]dynamodbtypes.AttributeValue{"id": &dynamodbtypes.AttributeValueMemberS{Value: id}}
	}

	_, err := cli.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tn),
		Item: map[string]dynamodbtypes.AttributeValue{
			"id":   &dynamodbtypes.AttributeValueMemberS{Value: "existing"},
			"name": &dynamodbtypes.AttributeValueMemberS{Value: "old"},
		},
	})
	require.NoError(t, err)

	put := func(id string) dynamodbtypes.TransactWriteItem {
		return dynamodbtypes.TransactWriteItem{Put: &dynamodbtypes.Put{TableName: aws.String(tn), Item: key(id)}}
	}
	failing := dynamodbtypes.ConditionCheck{
		TableName:                           aws.String(tn),
		Key:                                 key("existing"),
		ConditionExpression:                 aws.String("attribute_not_exists(id)"),
		ReturnValuesOnConditionCheckFailure: dynamodbtypes.ReturnValuesOnConditionCheckFailureAllOld,
	}

	_, err = cli.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []dynamodbtypes.TransactWriteItem{put("001"), {ConditionCheck: &failing}, put("002")},
	})

	c := require.New(t)

	var tce *dynamodbtypes.TransactionCanceledException
	c.ErrorAs(err, &tce)
	c.Len(tce.CancellationReasons, 3)
	c.Nil(tce.CancellationReasons[0].Item)
	c.Nil(tce.CancellationReasons[2].Item)
	c.Equal("ConditionalCheckFailed", aws.ToString(tce.CancellationReasons[1].Code))
	c.Equal(
		&dynamodbtypes.AttributeValueMemberS{Value: "old"},
		tce.CancellationReasons[1].Item["name"],
	)
}
