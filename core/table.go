package core

import (
	"fmt"
	"sort"

	"github.com/truora/minidyn/interpreter"
	"github.com/truora/minidyn/types"
)

// QueryInput struct to represent a query input
type QueryInput struct {
	Index                     string
	ExpressionAttributeValues map[string]*types.Item
	Limit                     int64
	ExclusiveStartKey         map[string]*types.Item
	KeyConditionExpression    string
	ConditionExpression       *string
	FilterExpression          string
	Aliases                   map[string]string
	ScanIndexForward          bool
	Scan                      bool
	started                   bool
}

// Table struct to mock a dynamodb table
type Table struct {
	Name                 string
	Indexes              map[string]*index
	AttributesDef        map[string]string
	SortedKeys           []string
	Data                 map[string]map[string]*types.Item
	KeySchema            keySchema
	BillingMode          *string
	UseNativeInterpreter bool
	NativeInterpreter    interpreter.Native
	LangInterpreter      interpreter.Language
}

// NewTable creates a new Table
func NewTable(name string) *Table {
	return &Table{
		Name:          name,
		Indexes:       map[string]*index{},
		AttributesDef: map[string]string{},
		SortedKeys:    []string{},
		Data:          map[string]map[string]*types.Item{},
	}
}

// SetAttributeDefinition sets the attribute definition of a table
func (t *Table) SetAttributeDefinition(attrs []*types.AttributeDefinition) {
	for _, attr := range attrs {
		t.AttributesDef[*attr.AttributeName] = *attr.AttributeType
	}
}

func parseKeySchema(schema []*types.KeySchemaElement) (keySchema, error) {
	var ks keySchema

	for _, element := range schema {
		if element.KeyType == "HASH" {
			ks.HashKey = element.AttributeName
			continue
		}

		ks.RangeKey = element.AttributeName
	}

	if ks.HashKey == "" {
		return ks, types.NewError("ValidationException", "No Hash Key specified in schema. All Dynamo DB Tables must have exactly one hash key", nil)
	}

	return ks, nil
}

// CreatePrimaryIndex creates the primary index of a table
func (t *Table) CreatePrimaryIndex(input *types.CreateTableInput) error {
	ks, err := parseKeySchema(input.KeySchema)
	if err != nil {
		return err
	}

	err = t.validateAttributeDefinition(ks, "")
	if err != nil {
		return err
	}

	// types-local check this after validate the key schema
	if t.BillingMode == nil || types.StringValue(t.BillingMode) != "PAY_PER_REQUEST" {
		if input.ProvisionedThroughput == nil {
			// https://github.com/aws/aws-sdk-go/issues/3140
			return types.NewError("ValidationException", "No provisioned throughput specified for the table", nil)
		}
	}

	t.KeySchema = ks

	return nil
}

func (t *Table) validateAttributeDefinition(ks keySchema, message string) error {
	if _, ok := t.AttributesDef[ks.HashKey]; !ok {
		return types.NewError("ValidationException", fmt.Sprintf("%sHash Key not specified in Attribute Definitions.", message), nil)
	}

	if _, ok := t.AttributesDef[ks.RangeKey]; ks.RangeKey != "" && !ok {
		return types.NewError("ValidationException", fmt.Sprintf("%sRange Key not specified in Attribute Definitions.", message), nil)
	}

	return nil
}

func buildGSI(t *Table, gsiInput *types.GlobalSecondaryIndex) (*index, error) {
	if t.BillingMode == nil || types.StringValue(t.BillingMode) != "PAY_PER_REQUEST" {
		if gsiInput.ProvisionedThroughput == nil {
			// https://github.com/aws/aws-sdk-go/issues/3140
			return nil, types.NewError("ValidationException", "No provisioned throughput specified for the global secondary index", nil)
		}
	}

	ks, err := parseKeySchema(gsiInput.KeySchema)
	if err != nil {
		return nil, err
	}

	err = t.validateAttributeDefinition(ks, "Global Secondary Index ")
	if err != nil {
		return nil, err
	}

	i := newIndex(t, indexTypeGlobal, ks)
	i.projection = gsiInput.Projection

	return i, nil
}

// ApplyIndexChange applies the index change
func (t *Table) ApplyIndexChange(change *types.GlobalSecondaryIndexUpdate) error {
	switch {
	case change.Create != nil:
		{
			gsi := &types.GlobalSecondaryIndex{
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

// AddGlobalIndexes adds global indexes to a table
func (t *Table) AddGlobalIndexes(input []*types.GlobalSecondaryIndex) error {
	if input != nil && len(input) == 0 {
		return types.NewError("ValidationException", "GSI list is empty/invalid", nil)
	}

	for _, gsiInput := range input {
		if err := t.addGlobalIndex(gsiInput); err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) addGlobalIndex(gsiInput *types.GlobalSecondaryIndex) error {
	i, err := buildGSI(t, gsiInput)
	if err != nil {
		return err
	}

	t.Indexes[*gsiInput.IndexName] = i

	return nil
}

func (t *Table) deleteIndex(indexName string) error {
	if _, ok := t.Indexes[indexName]; !ok {
		return types.NewError("ResourceNotFoundException", "Requested resource not found", nil)
	}

	delete(t.Indexes, indexName)

	return nil
}

func (t *Table) updateIndex(indexName string, provisionedThroughput *types.ProvisionedThroughput) error {
	// we do not have support for provisionedThroughput values in the index
	return nil
}

func buildLSI(t *Table, lsiInput *types.LocalSecondaryIndex) (*index, error) {
	ks, err := parseKeySchema(lsiInput.KeySchema)
	if err != nil {
		return nil, err
	}

	err = t.validateAttributeDefinition(ks, "Local Secondary Index ")
	if err != nil {
		return nil, err
	}

	i := newIndex(t, indexTypeLocal, ks)
	i.projection = lsiInput.Projection

	return i, nil
}

// AddLocalIndexes adds local indexes to a table
func (t *Table) AddLocalIndexes(input []*types.LocalSecondaryIndex) error {
	if input != nil && len(input) == 0 {
		return types.NewError("ValidationException", "ValidationException: LSI list is empty/invalid", nil)
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

func (t *Table) parseStartKey(schema keySchema, startkeyAttr map[string]*types.Item) string {
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
		i.startSearch(input.ScanIndexForward)

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

func (t *Table) getMatchedItemAndCount(input *QueryInput, pk, startKey string) (map[string]*types.Item, interpreter.ExpressionType, bool) {
	storedItem, ok := t.Data[pk]

	lastMatchExpressionType, matched := t.matchKey(*input, storedItem)

	if ok && !(input.started && matched) {
		return copyItem(storedItem), lastMatchExpressionType, false
	}

	// TODO: use project info to create the copy
	return copyItem(storedItem), lastMatchExpressionType, true
}

func shouldReturnNextKey(item map[string]*types.Item, count, scanned, limit, keysSize int64) bool {
	if len(item) == 0 || limit == 0 {
		return false
	}

	return scanned <= keysSize && limit <= count
}

func shouldCountItem(expressionType interpreter.ExpressionType, matched bool) bool {
	return expressionType == "" || expressionType == interpreter.ExpressionTypeFilter || (expressionType == interpreter.ExpressionTypeKey && matched)
}

func shouldBreakPage(count, limit int64) bool {
	return limit != 0 && limit == count
}

// GetKeyAt returns the key value in a given position
func GetKeyAt(sortedKeys []string, size int64, pos int64, forward bool) string {
	if !forward {
		return sortedKeys[size-1-int64(pos)]
	}

	return sortedKeys[pos]
}

// SearchData quiery the table based on the input
func (t *Table) SearchData(input QueryInput) ([]map[string]*types.Item, map[string]*types.Item) {
	items := []map[string]*types.Item{}
	limit := input.Limit
	exclusiveStartKey := input.ExclusiveStartKey
	index, sortedKeys := t.fetchQueryData(input)

	startKey := t.parseStartKey(t.KeySchema, exclusiveStartKey)
	input.started = startKey == ""
	last := map[string]*types.Item{}
	sortedKeysSize := int64(len(sortedKeys))

	forward := input.ScanIndexForward

	var (
		count   int64
		scanned int64
	)

	for pos := range sortedKeys {
		k := GetKeyAt(sortedKeys, sortedKeysSize, int64(pos), forward)

		pk, ok := prepareSearch(&input, index, k, startKey)
		if !ok {
			scanned++
			continue
		}

		item, expressionType, matched := t.getMatchedItemAndCount(&input, pk, startKey)

		if matched {
			items = append(items, item)
		}

		scanned++

		if shouldCountItem(expressionType, matched) {
			count++
		}

		last = item

		if shouldBreakPage(count, limit) {
			break
		}
	}

	return items, t.getLastKey(last, limit, count, scanned, sortedKeysSize, index)
}

func (t *Table) getLastKey(item map[string]*types.Item, limit, count, scanned, keysSize int64, index *index) map[string]*types.Item {
	if !shouldReturnNextKey(item, count, scanned, limit, keysSize) {
		return map[string]*types.Item{}
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

func (t *Table) matchKey(input QueryInput, item map[string]*types.Item) (interpreter.ExpressionType, bool) {
	var lastMatchExpressionType interpreter.ExpressionType
	matched := input.Scan

	if input.KeyConditionExpression != "" {
		matched = t.interpreterMatch(interpreter.MatchInput{
			TableName:      t.Name,
			Expression:     input.KeyConditionExpression,
			ExpressionType: interpreter.ExpressionTypeKey,
			Item:           item,
			Aliases:        input.Aliases,
			Attributes:     input.ExpressionAttributeValues,
		})
		lastMatchExpressionType = interpreter.ExpressionTypeKey
	}

	if input.FilterExpression != "" {
		matched = matched && t.interpreterMatch(interpreter.MatchInput{
			TableName:      t.Name,
			Expression:     input.FilterExpression,
			ExpressionType: interpreter.ExpressionTypeFilter,
			Item:           item,
			Aliases:        input.Aliases,
			Attributes:     input.ExpressionAttributeValues,
		})
		lastMatchExpressionType = interpreter.ExpressionTypeFilter
	}

	if input.ConditionExpression != nil && *input.ConditionExpression != "" {
		matched = t.interpreterMatch(interpreter.MatchInput{
			TableName:      t.Name,
			Expression:     *input.ConditionExpression,
			ExpressionType: interpreter.ExpressionTypeConditional,
			Item:           item,
			Aliases:        input.Aliases,
			Attributes:     input.ExpressionAttributeValues,
		})
		lastMatchExpressionType = interpreter.ExpressionTypeConditional
	}

	return lastMatchExpressionType, matched
}

func (t *Table) setItem(key string, item map[string]*types.Item) {
	_, exists := t.Data[key]
	t.Data[key] = item

	if !exists {
		t.SortedKeys = append(t.SortedKeys, key)
		sort.Strings(t.SortedKeys)
	}
}

func (t *Table) getItem(key string) map[string]*types.Item {
	item, exists := t.Data[key]
	if !exists {
		return map[string]*types.Item{}
	}

	return item
}

// Clear removes data and sorted keys from a table
func (t *Table) Clear() {
	t.SortedKeys = []string{}
	t.Data = map[string]map[string]*types.Item{}
}

// Put puts items into table
func (t *Table) Put(input *types.PutItemInput) (map[string]*types.Item, error) {
	item := copyItem(input.Item)

	key, err := t.KeySchema.GetKey(t.AttributesDef, input.Item)
	if err != nil {
		return item, types.NewError("ValidationException", err.Error(), nil)
	}

	// support conditional writes
	if input.ConditionExpression != nil {
		_, matched := t.matchKey(QueryInput{
			Index:                     PrimaryIndexName,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Aliases:                   input.ExpressionAttributeNames,
			Limit:                     1,
			ConditionExpression:       input.ConditionExpression,
		}, t.getItem(key))

		if !matched {
			return item, types.NewError("ConditionalCheckFailedException", ErrConditionalRequestFailed.Error(), nil)
		}
	}

	t.setItem(key, item)

	for _, index := range t.Indexes {
		err := index.putData(key, item)
		if err != nil {
			return nil, types.NewError("ValidationException", err.Error(), nil)
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

// Update updates an item in the table based on the input
func (t *Table) Update(input *types.UpdateItemInput) (map[string]*types.Item, error) {
	// update primary index
	key, err := t.KeySchema.GetKey(t.AttributesDef, input.Key)
	if err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	item, ok := t.Data[key]
	if !ok {
		// it allow the use of attribute_exists to check if the item exists
		item = map[string]*types.Item{}
	}

	// support conditional writes
	if input.ConditionExpression != nil {
		query := QueryInput{
			Index:                     PrimaryIndexName,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Limit:                     1,
			ConditionExpression:       input.ConditionExpression,
			Aliases:                   input.ExpressionAttributeNames,
		}

		_, matched := t.matchKey(query, item)
		if !matched {
			return nil, &types.ConditionalCheckFailedException{MessageText: ErrConditionalRequestFailed.Error()}
		}
	}

	if !ok {
		// types creates a new item when the item does not exists
		item = copyItem(input.Key)
	}

	oldItem := copyItem(item)

	err = t.interpreterUpdate(interpreter.UpdateInput{
		TableName:  t.Name,
		Expression: input.UpdateExpression,
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
			return nil, types.NewError("ValidationException", err.Error(), nil)
		}
	}

	return copyItem(item), nil
}

// Delete deletes an item in the table based on the input
func (t *Table) Delete(input *types.DeleteItemInput) (map[string]*types.Item, error) {
	key, err := t.KeySchema.GetKey(t.AttributesDef, input.Key)
	if err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
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
			return nil, types.NewError("ValidationException", err.Error(), nil)
		}
	}

	return item, nil
}

// Description returns the description of a table
func (t *Table) Description(name string) *types.TableDescription {
	// TODO: implement other fields for TableDescription
	gsi, lsi := t.IndexesDescription()

	return &types.TableDescription{
		TableName:              name,
		ItemCount:              int64(len(t.SortedKeys)),
		KeySchema:              t.KeySchema.describe(),
		GlobalSecondaryIndexes: gsi,
		LocalSecondaryIndexes:  lsi,
	}
}

// IndexesDescription returns the description of the table indexes
func (t *Table) IndexesDescription() ([]types.GlobalSecondaryIndexDescription, []types.LocalSecondaryIndexDescription) {
	gsi := []types.GlobalSecondaryIndexDescription{}
	lsi := []types.LocalSecondaryIndexDescription{}

	for indexName, index := range t.Indexes {
		schema := index.keySchema.describe()
		count := index.count()

		switch index.typ {
		case indexTypeGlobal:
			{
				gsi = append(gsi, types.GlobalSecondaryIndexDescription{
					IndexName:  &indexName,
					ItemCount:  count,
					KeySchema:  schema,
					Projection: index.projection,
				})
			}
		case indexTypeLocal:
			{
				lsi = append(lsi, types.LocalSecondaryIndexDescription{
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
