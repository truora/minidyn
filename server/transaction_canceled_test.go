package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestServerTransactionCanceledMessageListsEveryReason(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	makeBasicTable(t, cli, "pokemons", "id")

	put := func(id string) ddbtypes.TransactWriteItem {
		return ddbtypes.TransactWriteItem{Put: &ddbtypes.Put{
			TableName: aws.String("pokemons"),
			Item:      map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: id}},
		}}
	}
	check := ddbtypes.TransactWriteItem{ConditionCheck: &ddbtypes.ConditionCheck{
		TableName:           aws.String("pokemons"),
		Key:                 map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "404"}},
		ConditionExpression: aws.String("attribute_exists(id)"),
	}}

	tests := []struct {
		name    string
		items   []ddbtypes.TransactWriteItem
		message string
		codes   []string
	}{
		{
			name:    "failure last",
			items:   []ddbtypes.TransactWriteItem{put("001"), check},
			message: "Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed]",
			codes:   []string{"None", "ConditionalCheckFailed"},
		},
		{
			name:    "failure middle",
			items:   []ddbtypes.TransactWriteItem{put("001"), check, put("002")},
			message: "Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed, None]",
			codes:   []string{"None", "ConditionalCheckFailed", "None"},
		},
		{
			name:    "single item",
			items:   []ddbtypes.TransactWriteItem{check},
			message: "Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]",
			codes:   []string{"ConditionalCheckFailed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := require.New(t)

			_, err := cli.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{TransactItems: tt.items})

			var tce *ddbtypes.TransactionCanceledException
			c.ErrorAs(err, &tce)
			c.Equal(tt.message, aws.ToString(tce.Message))
			c.Len(tce.CancellationReasons, len(tt.codes))

			for i, code := range tt.codes {
				c.Equal(code, aws.ToString(tce.CancellationReasons[i].Code))

				if code == "ConditionalCheckFailed" {
					c.Equal("The conditional request failed.", aws.ToString(tce.CancellationReasons[i].Message))
				} else {
					c.Empty(aws.ToString(tce.CancellationReasons[i].Message))
				}
			}
		})
	}
}
