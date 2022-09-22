package core

import (
	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/truora/minidyn/interpreter"
)

type QueryInput struct {
	Index                     string
	ExpressionAttributeValues map[string]*dynamodb.AttributeValue
	Limit                     *int64
	ExclusiveStartKey         map[string]*dynamodb.AttributeValue
	KeyConditionExpression    *string
	ConditionExpression       *string
	FilterExpression          *string
	Aliases                   map[string]*string
	ScanIndexForward          *bool
	Scan                      bool
	started                   bool
}

// Table has the Indexes and the operation functions
type Table struct {
	Name                 string
	Indexes              map[string]*index
	AttributesDef        map[string]string
	SortedKeys           []string
	Data                 map[string]map[string]*dynamodb.AttributeValue
	KeySchema            keySchema
	BillingMode          *string
	UseNativeInterpreter bool
	NativeInterpreter    *interpreter.Native
	LangInterpreter      *interpreter.Language
}

func NewTable(name string) *Table {
	return &Table{
		Name:          name,
		Indexes:       map[string]*index{},
		AttributesDef: map[string]string{},
		SortedKeys:    []string{},
		Data:          map[string]map[string]*dynamodb.AttributeValue{},
	}
}

func (t *Table) SetAttributeDefinition(attrs []*dynamodb.AttributeDefinition) {
	for _, attr := range attrs {
		t.AttributesDef[*attr.AttributeName] = *attr.AttributeType
	}
}

func parseKeySchema(schema []*dynamodb.KeySchemaElement) (keySchema, error) {
	var ks keySchema

	for _, element := range schema {
		if *element.KeyType == "HASH" {
			ks.HashKey = *element.AttributeName
			continue
		}

		ks.RangeKey = *element.AttributeName
	}

	if ks.HashKey == "" {
		return ks, awserr.New("ValidationException", "No Hash Key specified in schema. All Dynamo DB Tables must have exactly one hash key", nil)
	}

	return ks, nil
}

func (t *Table) CreatePrimaryIndex(input *dynamodb.CreateTableInput) error {
	ks, err := parseKeySchema(input.KeySchema)
	if err != nil {
		return err
	}

	if _, ok := t.AttributesDef[ks.HashKey]; !ok {
		return awserr.New("ValidationException", "Hash Key not specified in Attribute Definitions.", nil)
	}

	if _, ok := t.AttributesDef[ks.RangeKey]; ks.RangeKey != "" && !ok {
		return awserr.New("ValidationException", "Range Key not specified in Attribute Definitions.", nil)
	}

	// dynamodb-local check this after validate the key schema
	if aws.StringValue(t.BillingMode) != "PAY_PER_REQUEST" {
		if input.ProvisionedThroughput == nil {
			// https://github.com/aws/aws-sdk-go/issues/3140
			return awserr.New("ValidationException", "No provisioned throughput specified for the table", nil)
		}
	}

	t.KeySchema = ks

	return nil
}

func buildGSI(t *Table, gsiInput *dynamodb.GlobalSecondaryIndex) (*index, error) {
	if aws.StringValue(t.BillingMode) != "PAY_PER_REQUEST" {
		if gsiInput.ProvisionedThroughput == nil {
			// https://github.com/aws/aws-sdk-go/issues/3140
			return nil, awserr.New("ValidationException", "No provisioned throughput specified for the global secondary index", nil)
		}
	}

	ks, err := parseKeySchema(gsiInput.KeySchema)
	if err != nil {
		return nil, err
	}

	if _, ok := t.AttributesDef[ks.HashKey]; !ok {
		return nil, awserr.New("ValidationException", "Global Secondary Index hash key not specified in Attribute Definitions.", nil)
	}

	if _, ok := t.AttributesDef[ks.RangeKey]; ks.RangeKey != "" && !ok {
		return nil, awserr.New("ValidationException", "Global Secondary Index range key not specified in Attribute Definitions.", nil)
	}

	i := newIndex(t, indexTypeGlobal, ks)
	i.projection = gsiInput.Projection

	return i, nil
}

func (t *Table) ApplyIndexChange(change *dynamodb.GlobalSecondaryIndexUpdate) error {
	switch {
	case change.Create != nil:
		{
			gsi := &dynamodb.GlobalSecondaryIndex{
				IndexName:             change.Create.IndexName,
				KeySchema:             change.Create.KeySchema,
				Projection:            change.Create.Projection,
				ProvisionedThroughput: change.Create.ProvisionedThroughput,
			}
			return t.addGlobalIndex(gsi)
		}
	case change.Delete != nil:
		return t.deleteIndex(*change.Delete.IndexName)
	case change.Update != nil:
		return t.updateIndex(*change.Update.IndexName, change.Update.ProvisionedThroughput)
	}

	return nil
}

func (t *Table) AddGlobalIndexes(input []*dynamodb.GlobalSecondaryIndex) error {
	if input != nil && len(input) == 0 {
		return awserr.New("ValidationException", "GSI list is empty/invalid", nil)
	}

	for _, gsiInput := range input {
		if err := t.addGlobalIndex(gsiInput); err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) addGlobalIndex(gsiInput *dynamodb.GlobalSecondaryIndex) error {
	i, err := buildGSI(t, gsiInput)
	if err != nil {
		return err
	}

	t.Indexes[*gsiInput.IndexName] = i

	return nil
}

func (t *Table) deleteIndex(indexName string) error {
	if _, ok := t.Indexes[indexName]; !ok {
		return awserr.New(dynamodb.ErrCodeResourceNotFoundException, "Requested resource not found", nil)
	}

	delete(t.Indexes, indexName)

	return nil
}

func (t *Table) updateIndex(indexName string, provisionedThroughput *dynamodb.ProvisionedThroughput) error {
	// we do not have support for provisionedThroughput values in the index
	return nil
}

func buildLSI(t *Table, lsiInput *dynamodb.LocalSecondaryIndex) (*index, error) {
	ks, err := parseKeySchema(lsiInput.KeySchema)
	if err != nil {
		return nil, err
	}

	if _, ok := t.AttributesDef[ks.HashKey]; !ok {
		return nil, awserr.New("ValidationException", "Local Secondary Index hash key not specified in Attribute Definitions.", nil)
	}

	if _, ok := t.AttributesDef[ks.RangeKey]; ks.RangeKey != "" && !ok {
		return nil, awserr.New("ValidationException", "Local Secondary Index range key not specified in Attribute Definitions.", nil)
	}

	i := newIndex(t, indexTypeLocal, ks)
	i.projection = lsiInput.Projection

	return i, nil
}

func (t *Table) AddLocalIndexes(input []*dynamodb.LocalSecondaryIndex) error {
	if input != nil && len(input) == 0 {
		return awserr.New("ValidationException", "ValidationException: LSI list is empty/invalid", nil)
	}

	for _, lsi := range input {
		i, err := buildLSI(t, lsi)
		if err != nil {
			return err
		}

		t.Indexes[*lsi.IndexName] = i
	}

	return nil
}

func (t *Table) parseStartKey(schema keySchema, startkeyAttr map[string]*dynamodb.AttributeValue) string {
	startKey := ""
	if len(startkeyAttr) != 0 {
		startKey, _ = schema.GetKey(t.AttributesDef, startkeyAttr)
	}

	return startKey
}

func getPrimaryKey(index *index, k string) (string, bool) {
	pk, ok := k, true

	if index != nil {
		pk, ok = index.getPrimaryKey(k)
	}

	return pk, ok
}

func (t *Table) fetchQueryData(input QueryInput) (*index, []string) {
	if input.Index != "" {
		i := t.Indexes[input.Index]
		i.startSearch(aws.BoolValue(input.ScanIndexForward))

		return i, i.sortedKeys
	}

	return nil, t.SortedKeys
}

func prepareSearch(input *QueryInput, index *index, k, startKey string) (string, bool) {
	pk, ok := getPrimaryKey(index, k)
	if !ok {
		return pk, ok
	}

	if input.started {
		return pk, true
	}

	if pk == startKey {
		input.started = true
	}

	return "", false
}

func (t *Table) getMatchedItemAndCount(input *QueryInput, pk, startKey string) (map[string]*dynamodb.AttributeValue, interpreter.ExpressionType, bool) {
	storedItem, ok := t.Data[pk]

	lastMatchExpressionType, matched := t.matchKey(*input, storedItem)

	if ok && !(input.started && matched) {
		return copyItem(storedItem), lastMatchExpressionType, false
	}

	// TODO: use project info to create the copy
	return copyItem(storedItem), lastMatchExpressionType, true
}

func shouldReturnNextKey(item map[string]*dynamodb.AttributeValue, startKey string, count, limit, keysSize int64) bool {
	if len(item) == 0 {
		return false
	}

	// Make sure that if we are in the first page without pages, don't return a next key
	if startKey == "" && limit != count && count < keysSize {
		return false
	}

	return limit != 0 && limit <= keysSize
}

func shouldCountItem(expressionType interpreter.ExpressionType, matched bool) bool {
	return expressionType == "" || expressionType == interpreter.ExpressionTypeFilter || (expressionType == interpreter.ExpressionTypeKey && matched)
}

func shouldBreakPage(count, limit int64) bool {
	return limit != 0 && limit == count
}

func GetKeyAt(sortedKeys []string, size int64, pos int64, forward bool) string {
	if !forward {
		return sortedKeys[size-1-int64(pos)]
	}

	return sortedKeys[pos]
}

func (t *Table) SearchData(input QueryInput) ([]map[string]*dynamodb.AttributeValue, map[string]*dynamodb.AttributeValue) {
	items := []map[string]*dynamodb.AttributeValue{}
	limit := aws.Int64Value(input.Limit)
	exclusiveStartKey := input.ExclusiveStartKey
	index, sortedKeys := t.fetchQueryData(input)

	startKey := t.parseStartKey(t.KeySchema, exclusiveStartKey)
	input.started = startKey == ""
	last := map[string]*dynamodb.AttributeValue{}
	sortedKeysSize := int64(len(sortedKeys))
	forward := aws.BoolValue(input.ScanIndexForward)

	var count int64

	for pos := range sortedKeys {
		k := GetKeyAt(sortedKeys, sortedKeysSize, int64(pos), forward)

		pk, ok := prepareSearch(&input, index, k, startKey)
		if !ok {
			continue
		}

		item, expressionType, matched := t.getMatchedItemAndCount(&input, pk, startKey)
		if matched {
			items = append(items, item)
		}

		if shouldCountItem(expressionType, matched) {
			count++
		}

		last = item

		if shouldBreakPage(count, limit) {
			break
		}
	}

	return items, t.getLastKey(last, startKey, limit, count, sortedKeysSize, index)
}

func (t *Table) getLastKey(item map[string]*dynamodb.AttributeValue, startKey string, limit, count, keysSize int64, index *index) map[string]*dynamodb.AttributeValue {
	if !shouldReturnNextKey(item, startKey, count, limit, keysSize) {
		return map[string]*dynamodb.AttributeValue{}
	}

	key := t.KeySchema.getKeyItem(item)

	if index != nil {
		iKey := index.keySchema.getKeyItem(item)
		for field, val := range iKey {
			key[field] = val
		}
	}

	return key
}

func (t *Table) interpreterMatch(input interpreter.MatchInput) bool {
	if t.UseNativeInterpreter {
		matched, err := t.NativeInterpreter.Match(input)
		if err == nil {
			return matched
		}
	}

	matched, err := t.LangInterpreter.Match(input)
	if err != nil {
		panic(err)
	}

	return matched
}

func (t *Table) matchKey(input QueryInput, item map[string]*dynamodb.AttributeValue) (interpreter.ExpressionType, bool) {
	var lastMatchExpressionType interpreter.ExpressionType
	matched := input.Scan

	if input.KeyConditionExpression != nil {
		matched = t.interpreterMatch(interpreter.MatchInput{
			TableName:      t.Name,
			Expression:     aws.StringValue(input.KeyConditionExpression),
			ExpressionType: interpreter.ExpressionTypeKey,
			Item:           item,
			Aliases:        input.Aliases,
			Attributes:     input.ExpressionAttributeValues,
		})
		lastMatchExpressionType = interpreter.ExpressionTypeKey
	}

	if input.FilterExpression != nil {
		matched = matched && t.interpreterMatch(interpreter.MatchInput{
			TableName:      t.Name,
			Expression:     aws.StringValue(input.FilterExpression),
			ExpressionType: interpreter.ExpressionTypeFilter,
			Item:           item,
			Aliases:        input.Aliases,
			Attributes:     input.ExpressionAttributeValues,
		})
		lastMatchExpressionType = interpreter.ExpressionTypeFilter
	}

	if input.ConditionExpression != nil {
		matched = t.interpreterMatch(interpreter.MatchInput{
			TableName:      t.Name,
			Expression:     aws.StringValue(input.ConditionExpression),
			ExpressionType: interpreter.ExpressionTypeConditional,
			Item:           item,
			Aliases:        input.Aliases,
			Attributes:     input.ExpressionAttributeValues,
		})
		lastMatchExpressionType = interpreter.ExpressionTypeConditional
	}

	return lastMatchExpressionType, matched
}

func (t *Table) setItem(key string, item map[string]*dynamodb.AttributeValue) {
	_, exists := t.Data[key]
	t.Data[key] = item

	if !exists {
		t.SortedKeys = append(t.SortedKeys, key)
		sort.Strings(t.SortedKeys)
	}
}

func (t *Table) getItem(key string) map[string]*dynamodb.AttributeValue {
	item, exists := t.Data[key]
	if !exists {
		return map[string]*dynamodb.AttributeValue{}
	}

	return item
}

func (t *Table) Clear() {
	t.SortedKeys = []string{}
	t.Data = map[string]map[string]*dynamodb.AttributeValue{}
}

func (t *Table) Put(input *dynamodb.PutItemInput) (map[string]*dynamodb.AttributeValue, error) {
	item := copyItem(input.Item)

	key, err := t.KeySchema.GetKey(t.AttributesDef, input.Item)
	if err != nil {
		return item, awserr.New("ValidationException", err.Error(), nil)
	}

	// support conditional writes
	if input.ConditionExpression != nil {
		_, matched := t.matchKey(QueryInput{
			Index:                     PrimaryIndexName,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Aliases:                   input.ExpressionAttributeNames,
			Limit:                     aws.Int64(1),
			ConditionExpression:       input.ConditionExpression,
		}, t.getItem(key))

		if !matched {
			return item, awserr.New(dynamodb.ErrCodeConditionalCheckFailedException, ErrConditionalRequestFailed.Error(), nil)
		}
	}

	t.setItem(key, item)

	for _, index := range t.Indexes {
		err := index.putData(key, item)
		if err != nil {
			return nil, awserr.New("ValidationException", err.Error(), nil)
		}
	}

	return item, nil
}

func (t *Table) interpreterUpdate(input interpreter.UpdateInput) error {
	if t.UseNativeInterpreter {
		return t.NativeInterpreter.Update(input)
	}

	return t.LangInterpreter.Update(input)
}

func (t *Table) Update(input *dynamodb.UpdateItemInput) (map[string]*dynamodb.AttributeValue, error) {
	// update primary index
	key, err := t.KeySchema.GetKey(t.AttributesDef, input.Key)
	if err != nil {
		return nil, awserr.New("ValidationException", err.Error(), nil)
	}

	item, ok := t.Data[key]
	if !ok {
		// it allow the use of attribute_exists to check if the item exists
		item = map[string]*dynamodb.AttributeValue{}
	}

	// support conditional writes
	if input.ConditionExpression != nil {
		query := QueryInput{
			Index:                     PrimaryIndexName,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Limit:                     aws.Int64(1),
			ConditionExpression:       input.ConditionExpression,
			Aliases:                   input.ExpressionAttributeNames,
		}

		_, matched := t.matchKey(query, item)
		if !matched {
			return nil, &dynamodb.ConditionalCheckFailedException{Message_: aws.String(ErrConditionalRequestFailed.Error())}
		}
	}

	if !ok {
		// dynamodb creates a new item when the item does not exists
		item = copyItem(input.Key)
	}

	oldItem := copyItem(item)

	err = t.interpreterUpdate(interpreter.UpdateInput{
		TableName:  t.Name,
		Expression: aws.StringValue(input.UpdateExpression),
		Item:       item,
		Attributes: input.ExpressionAttributeValues,
		Aliases:    input.ExpressionAttributeNames,
	})
	if err != nil {
		return nil, err
	}

	t.setItem(key, item)

	// update secondary Indexes
	for _, index := range t.Indexes {
		err := index.updateData(key, item, oldItem)
		if err != nil {
			return nil, awserr.New("ValidationException", err.Error(), nil)
		}
	}

	return copyItem(item), nil
}

func (t *Table) Delete(input *dynamodb.DeleteItemInput) (map[string]*dynamodb.AttributeValue, error) {
	key, err := t.KeySchema.GetKey(t.AttributesDef, input.Key)
	if err != nil {
		return nil, awserr.New("ValidationException", err.Error(), nil)
	}

	// delete is an idempotent operation,
	// running it multiple times on the same item or attribute does not result in an error response,
	// therefore we do not need to check if the item exists.
	item, ok := t.Data[key]
	if !ok {
		return item, nil
	}

	item = copyItem(item)

	delete(t.Data, key)

	pos := sort.SearchStrings(t.SortedKeys, key)
	if pos == len(t.SortedKeys) {
		return item, nil
	}

	copy(t.SortedKeys[pos:], t.SortedKeys[pos+1:])
	t.SortedKeys[len(t.SortedKeys)-1] = ""
	t.SortedKeys = t.SortedKeys[:len(t.SortedKeys)-1]

	for _, index := range t.Indexes {
		err := index.delete(key, item)
		if err != nil {
			return nil, awserr.New("ValidationException", err.Error(), nil)
		}
	}

	return item, nil
}

func (t *Table) Description(name string) *dynamodb.TableDescription {
	// TODO: implement other fields for TableDescription
	gsi, lsi := t.IndexesDescription()

	return &dynamodb.TableDescription{
		TableName:              aws.String(name),
		ItemCount:              aws.Int64(int64(len(t.SortedKeys))),
		KeySchema:              t.KeySchema.describe(),
		GlobalSecondaryIndexes: gsi,
		LocalSecondaryIndexes:  lsi,
	}
}

func (t *Table) IndexesDescription() ([]*dynamodb.GlobalSecondaryIndexDescription, []*dynamodb.LocalSecondaryIndexDescription) {
	gsi := []*dynamodb.GlobalSecondaryIndexDescription{}
	lsi := []*dynamodb.LocalSecondaryIndexDescription{}

	for indexName, index := range t.Indexes {
		schema := index.keySchema.describe()
		count := aws.Int64(index.count())

		switch index.typ {
		case indexTypeGlobal:
			{
				gsi = append(gsi, &dynamodb.GlobalSecondaryIndexDescription{
					IndexName:  &indexName,
					ItemCount:  count,
					KeySchema:  schema,
					Projection: index.projection,
				})
			}
		case indexTypeLocal:
			{
				lsi = append(lsi, &dynamodb.LocalSecondaryIndexDescription{
					IndexName:  &indexName,
					ItemCount:  count,
					KeySchema:  schema,
					Projection: index.projection,
				})
			}
		}
	}

	return gsi, lsi
}