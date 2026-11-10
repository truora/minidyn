package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

const (
	noAttributeName = "Attribute name must be specified."
	noAttributeType = "Invalid Attribute value. Must contain exactly one of the supported datatypes."
)

func malformedAttributeDefinitions() map[string]struct {
	attrs   []ddbtypes.AttributeDefinition
	message string
} {
	type c = struct {
		attrs   []ddbtypes.AttributeDefinition
		message string
	}

	return map[string]c{
		"NoName":  {[]ddbtypes.AttributeDefinition{{AttributeType: ddbtypes.ScalarAttributeTypeS}}, noAttributeName + " | table: %s"},
		"Empty":   {[]ddbtypes.AttributeDefinition{{}}, noAttributeName + " | table: %s"},
		"NoType":  {[]ddbtypes.AttributeDefinition{{AttributeName: aws.String("id")}}, noAttributeType + " | attribute: id; table: %s"},
		"BadLast": {[]ddbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS}, {}}, noAttributeName + " | table: %s"},
	}
}

func TestServerCreateTableRejectsMalformedAttributeDefinitions(t *testing.T) {
	for name, tt := range malformedAttributeDefinitions() {
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(NewServer())
			defer ts.Close()

			cli := newTestDynamoClient(t, ts.URL)

			_, err := cli.CreateTable(context.Background(), &dynamodb.CreateTableInput{
				TableName:            aws.String("tbl"),
				BillingMode:          ddbtypes.BillingModePayPerRequest,
				KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
				AttributeDefinitions: tt.attrs,
			}, skipInputValidation)
			requireServerAPIError(t, err, "ValidationException", fmt.Sprintf(tt.message, "tbl"))
		})
	}
}

func TestServerUpdateTableRejectsMalformedAttributeDefinitions(t *testing.T) {
	for name, tt := range malformedAttributeDefinitions() {
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(NewServer())
			defer ts.Close()

			cli := newTestDynamoClient(t, ts.URL)
			makeBasicTable(t, cli, "pokemons", "id")

			_, err := cli.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
				TableName:            aws.String("pokemons"),
				AttributeDefinitions: tt.attrs,
				// well formed, so the attribute definitions are what is rejected
				GlobalSecondaryIndexUpdates: []ddbtypes.GlobalSecondaryIndexUpdate{{
					Create: &ddbtypes.CreateGlobalSecondaryIndexAction{
						IndexName:  aws.String("fresh"),
						KeySchema:  []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
						Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
					},
				}},
			}, skipInputValidation)
			requireServerAPIError(t, err, "ValidationException", fmt.Sprintf(tt.message, "pokemons"))
		})
	}
}

// A null entry can only come from raw JSON.
func TestServerRejectsNullAttributeDefinition(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	body := `{"TableName":"tbl","BillingMode":"PAY_PER_REQUEST","AttributeDefinitions":[null],` +
		`"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}`

	req, err := http.NewRequest(http.MethodPost, ts.URL, bytes.NewReader([]byte(body)))
	require.NoError(t, err)

	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.True(t, strings.Contains(string(raw), noAttributeName), string(raw))
}

func TestServerUpdateTableKeepsAttributeDefinitionsWhenRejected(t *testing.T) {
	ts := httptest.NewServer(NewServer())
	defer ts.Close()

	cli := newTestDynamoClient(t, ts.URL)
	makeBasicTable(t, cli, "pokemons", "id")

	other := []ddbtypes.AttributeDefinition{{AttributeName: aws.String("other"), AttributeType: ddbtypes.ScalarAttributeTypeS}}
	createByOther := []ddbtypes.GlobalSecondaryIndexUpdate{{Create: &ddbtypes.CreateGlobalSecondaryIndexAction{
		IndexName:  aws.String("by-other"),
		KeySchema:  []ddbtypes.KeySchemaElement{{AttributeName: aws.String("other"), KeyType: ddbtypes.KeyTypeHash}},
		Projection: &ddbtypes.Projection{ProjectionType: ddbtypes.ProjectionTypeAll},
	}}}

	_, err := cli.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
		TableName:                   aws.String("pokemons"),
		AttributeDefinitions:        other,
		GlobalSecondaryIndexUpdates: []ddbtypes.GlobalSecondaryIndexUpdate{{Delete: &ddbtypes.DeleteGlobalSecondaryIndexAction{}}},
	}, skipInputValidation)
	requireServerAPIError(t, err, "ValidationException", "Invalid global secondary index name")

	_, err = cli.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
		TableName: aws.String("pokemons"), GlobalSecondaryIndexUpdates: createByOther,
	})
	require.Error(t, err)

	_, err = cli.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
		TableName: aws.String("pokemons"), AttributeDefinitions: other, GlobalSecondaryIndexUpdates: createByOther,
	})
	require.NoError(t, err)
}
