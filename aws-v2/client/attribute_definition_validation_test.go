package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

const (
	noAttributeName = "Attribute name must be specified."
	noAttributeType = "Invalid Attribute value. Must contain exactly one of the supported datatypes."
)

func malformedAttributeDefinitions() map[string]struct {
	attrs   []dynamodbtypes.AttributeDefinition
	message string
} {
	type c = struct {
		attrs   []dynamodbtypes.AttributeDefinition
		message string
	}

	return map[string]c{
		"NoName":  {[]dynamodbtypes.AttributeDefinition{{AttributeType: dynamodbtypes.ScalarAttributeTypeS}}, noAttributeName + " | table: %s"},
		"Empty":   {[]dynamodbtypes.AttributeDefinition{{}}, noAttributeName + " | table: %s"},
		"NoType":  {[]dynamodbtypes.AttributeDefinition{{AttributeName: aws.String("pk")}}, noAttributeType + " | attribute: pk; table: %s"},
		"BadLast": {[]dynamodbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS}, {}}, noAttributeName + " | table: %s"},
	}
}

func TestCreateTableRejectsMalformedAttributeDefinitions(t *testing.T) {
	for name, tt := range malformedAttributeDefinitions() {
		t.Run(name, func(t *testing.T) {
			client := NewClient()

			_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
				TableName:            aws.String("tbl"),
				BillingMode:          dynamodbtypes.BillingModePayPerRequest,
				KeySchema:            []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash}},
				AttributeDefinitions: tt.attrs,
			})
			requireAPIError(t, err, "ValidationException", fmt.Sprintf(tt.message, "tbl"))
		})
	}
}

func TestUpdateTableRejectsMalformedAttributeDefinitions(t *testing.T) {
	for name, tt := range malformedAttributeDefinitions() {
		t.Run(name, func(t *testing.T) {
			client := NewClient()
			createDescribedTable(t, client, "tbl", &dynamodb.CreateTableInput{BillingMode: dynamodbtypes.BillingModePayPerRequest})

			_, err := client.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
				TableName:            aws.String("tbl"),
				AttributeDefinitions: tt.attrs,
				// well formed, so the attribute definitions are what is rejected
				GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{{
					Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
						IndexName:  aws.String("fresh"),
						KeySchema:  []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash}},
						Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
					},
				}},
			})
			requireAPIError(t, err, "ValidationException", fmt.Sprintf(tt.message, "tbl"))
		})
	}
}

func TestUpdateTableKeepsAttributeDefinitionsWhenRejected(t *testing.T) {
	client := NewClient()
	ctx := context.Background()
	createDescribedTable(t, client, "tbl", &dynamodb.CreateTableInput{BillingMode: dynamodbtypes.BillingModePayPerRequest})

	createByOther := []dynamodbtypes.GlobalSecondaryIndexUpdate{{Create: &dynamodbtypes.CreateGlobalSecondaryIndexAction{
		IndexName:  aws.String("by-other"),
		KeySchema:  []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("other"), KeyType: dynamodbtypes.KeyTypeHash}},
		Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
	}}}

	// the attribute definition comes with a request that is rejected as a whole
	_, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String("tbl"),
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("other"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexUpdates: []dynamodbtypes.GlobalSecondaryIndexUpdate{{Delete: &dynamodbtypes.DeleteGlobalSecondaryIndexAction{}}},
	})
	requireAPIError(t, err, "ValidationException", "Invalid global secondary index name")

	// so "other" is still undefined
	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{TableName: aws.String("tbl"), GlobalSecondaryIndexUpdates: createByOther})
	require.Error(t, err)

	// while the same definition sent with a valid request is kept
	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String("tbl"),
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("other"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexUpdates: createByOther,
	})
	require.NoError(t, err)
}
