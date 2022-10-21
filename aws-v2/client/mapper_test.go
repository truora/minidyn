package client

import (
	"testing"

	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/ldelacruztruora/minidyn/types"
	"github.com/stretchr/testify/require"
)

func TestMapDynamoToTypesStringSlice(t *testing.T) {
	c := require.New(t)

	slice := mapDynamoToTypesStringSlice([]string{"1", "2", "3"})

	c.Equal(len(slice), 3)
}

func TestMapDynamoToTypes(t *testing.T) {
	c := require.New(t)

	output := mapDynamoToTypesCreateTableInput(nil)
	c.Nil(output)

	test := "test"
	outputMap := mapDynamoToTypesStringMap(map[string]string{test: test})
	c.Equal(map[string]*string{test: &test}, outputMap)

	putItemOutput := mapDynamoToTypesPutItemInput(nil)
	c.Nil(putItemOutput)

	sliceItemOUtput := mapDynamoToTypesSliceItem(nil)
	c.Nil(sliceItemOUtput)

	deleteItemOutput := mapDynamoToTypesDeleteItemInput(nil)
	c.Nil(deleteItemOutput)

	sliceItemOUtput = mapDynamoToTypesSliceItem([]dynamodbtypes.AttributeValue{&dynamodbtypes.AttributeValueMemberS{Value: "test"}})
	c.Equal("test", types.StringValue(sliceItemOUtput[0].S))

	item := mapDynamoToTypesItem(&dynamodbtypes.AttributeValueMemberB{Value: []byte("test")})
	c.Equal([]byte("test"), item.B)

	item = mapDynamoToTypesItem(&dynamodbtypes.AttributeValueMemberNS{Value: []string{"1", "2"}})
	c.Equal([]*string{types.ToString("1"), types.ToString("2")}, item.NS)

	trueValue := true

	expectedMap := map[string]*types.Item(map[string]*types.Item{"test": &types.Item{BOOL: &trueValue}})
	item = mapDynamoToTypesAttributeDefinitionMapOrList(&dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{"test": &dynamodbtypes.AttributeValueMemberBOOL{Value: true}}})
	c.Equal(expectedMap, item.M)
}

func TestMapTypesToDynamo(t *testing.T) {
	c := require.New(t)

	tableDescriptionOutput := mapTypesToDynamoTableDescription(nil)
	c.Nil(tableDescriptionOutput)

	provisionedThroughputOutput := mapTypesToDynamoProvisionedThroughput(nil)
	c.Nil(provisionedThroughputOutput)

	provisionedThroughputOutput = mapTypesToDynamoProvisionedThroughput(&types.ProvisionedThroughput{ReadCapacityUnits: 1})
	c.Equal(aws.Int64(1), provisionedThroughputOutput.ReadCapacityUnits)

	var expectedAttributteValue dynamodbtypes.AttributeValue = &dynamodbtypes.AttributeValueMemberNULL{Value: true}
	input := &types.Item{M: map[string]*types.Item{}}

	attributeDefinitionMapOutput := mapTypesToDynamoAttributeDefinitionMapOrList(input)
	c.Equal(expectedAttributteValue, attributeDefinitionMapOutput)

	input.M = map[string]*types.Item{"test": {S: types.ToString("test")}}
	expectedAttributteValue = &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{"test": &dynamodbtypes.AttributeValueMemberS{Value: "test"}}}

	attributeDefinitionMapOutput = mapTypesToDynamoAttributeDefinitionMapOrList(input)
	c.Equal(expectedAttributteValue, attributeDefinitionMapOutput)

	item := &types.Item{B: []byte("test")}
	expectedAttributteValue = &dynamodbtypes.AttributeValueMemberB{Value: []byte("test")}

	resultItem := mapTypesToDynamoItem(item)
	c.Equal(expectedAttributteValue, resultItem)

	trueValue := true

	item = &types.Item{BOOL: &trueValue}
	expectedAttributteValue = &dynamodbtypes.AttributeValueMemberBOOL{Value: true}

	resultItem = mapTypesToDynamoItem(item)
	c.Equal(expectedAttributteValue, resultItem)

	item = &types.Item{BS: [][]byte{[]byte("test")}}
	expectedAttributteValue = &dynamodbtypes.AttributeValueMemberBS{Value: [][]byte{[]byte("test")}}

	resultItem = mapTypesToDynamoItem(item)
	c.Equal(expectedAttributteValue, resultItem)

	item = &types.Item{NS: []*string{types.ToString("1")}}
	expectedAttributteValue = &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1"}}

	resultItem = mapTypesToDynamoItem(item)
	c.Equal(expectedAttributteValue, resultItem)

	sliceResult := mapTypesToDynamoStringSlice([]*string{types.ToString("1")})
	c.Equal([]string{"1"}, sliceResult)

	projection := mapTypesToDynamoProjection(nil)
	c.Nil(projection)
}

func TestMapDynamoToTypesProjection(t *testing.T) {
	c := require.New(t)

	output := mapDynamoToTypesProjection(nil)
	c.Nil(output)

	output = mapDynamoToTypesProjection(&dynamodbtypes.Projection{})
	c.NotNil(output)
}

func TestMapDynamoToTypesExpectedAttributeValue(t *testing.T) {
	c := require.New(t)

	output := mapDynamoToTypesExpectedAttributeValue(dynamodbtypes.ExpectedAttributeValue{})
	c.NotNil(output)

	attributeValueOutput := mapDynamoToTypesExpectedAttributeValueMap(map[string]dynamodbtypes.ExpectedAttributeValue{"test": dynamodbtypes.ExpectedAttributeValue{}})
	c.Len(attributeValueOutput, 1)

	keySchemaElementsOutput := mapTypesToDynamoKeySchemaElementsPointer(nil)
	c.Nil(keySchemaElementsOutput)

	keySchemaElementsOutput = mapTypesToDynamoKeySchemaElementsPointer([]*types.KeySchemaElement{{AttributeName: "test"}})
	c.Len(keySchemaElementsOutput, 1)
}
