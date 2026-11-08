package core

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/truora/minidyn/interpreter"
	"github.com/truora/minidyn/interpreter/language"
	"github.com/truora/minidyn/types"
)

const defaultIndexActivationDelay = 0

// QueryInput struct to represent a query input
type QueryInput struct {
	Index                     string
	ExpressionAttributeValues map[string]*types.Item
	Limit                     int64
	ExclusiveStartKey         map[string]*types.Item
	KeyConditionExpression    string
	ConditionExpression       *string
	FilterExpression          string
	ProjectionExpression      string
	// Select is validated against the other fields; the front ends honor COUNT.
	Select           string
	Aliases          map[string]string
	ScanIndexForward bool
	Scan             bool
	started          bool
}

// Table struct to mock a dynamodb table
type Table struct {
	Name                  string
	Indexes               map[string]*index
	AttributesDef         map[string]string
	SortedKeys            []string
	Data                  map[string]map[string]*types.Item
	KeySchema             keySchema
	BillingMode           *string
	ProvisionedThroughput *types.ProvisionedThroughput
	UseNativeInterpreter  bool
	NativeInterpreter     interpreter.Native
	LangInterpreter       interpreter.Language
	IndexActivationDelay  time.Duration
}

// NewTable creates a new Table
func NewTable(name string) *Table {
	return &Table{
		Name:                 name,
		Indexes:              map[string]*index{},
		AttributesDef:        map[string]string{},
		SortedKeys:           []string{},
		Data:                 map[string]map[string]*types.Item{},
		IndexActivationDelay: defaultIndexActivationDelay,
	}
}

// ValidatePrimaryKeyMap checks that a Key map for GetItem, DeleteItem, or UpdateItem
// contains exactly the table's primary key attribute names (hash and range when defined).
func (t *Table) ValidatePrimaryKeyMap(key map[string]*types.Item) error {
	return t.KeySchema.validatePrimaryKeyMap(key)
}

// IndexKind classifies a secondary index by name: "GSI" for a global secondary index,
// "LSI" for a local one. It returns "" for the primary index or an unknown name. Used to
// place per-index entries when reporting ConsumedCapacity at the INDEXES level.
func (t *Table) IndexKind(name string) string {
	idx, ok := t.Indexes[name]
	if !ok {
		return ""
	}

	if idx.typ == indexTypeLocal {
		return "LSI"
	}

	return "GSI"
}

// IndexWriteDelta is how a write touches one secondary index, used to compute the per-index
// write ConsumedCapacity. Before and After are the item as it sat in the index and as it lands in
// it (honoring the index's ProjectionType); each is nil when the item is not indexed there, which
// is how a sparse index is entered or left. KeyChanged reports that the item stays in the index
// under a different index key, so the old entry is deleted and a new one written.
type IndexWriteDelta struct {
	Name       string
	Kind       string
	Before     map[string]*types.Item
	After      map[string]*types.Item
	KeyChanged bool
}

// WriteIndexDeltas returns one IndexWriteDelta per secondary index a write from the item before
// to the item after has to write: an index the item enters, leaves, or stays in with a different
// entry (an index needs a complete index key). Mirroring DynamoDB, which only charges write
// capacity on indexes a write actually changes, an index whose entry comes out the same is
// omitted: the write changed only attributes that are neither index keys nor projected. Either
// item may be nil (a put has no before, a delete no after). Entries are name-sorted for
// deterministic reporting.
func (t *Table) WriteIndexDeltas(before, after map[string]*types.Item) []IndexWriteDelta {
	if (len(before) == 0 && len(after) == 0) || len(t.Indexes) == 0 {
		return nil
	}

	names := make([]string, 0, len(t.Indexes))
	for name := range t.Indexes {
		names = append(names, name)
	}

	sort.Strings(names)

	out := make([]IndexWriteDelta, 0, len(names))

	for _, name := range names {
		idx := t.Indexes[name]
		beforeKey := t.indexKeyOf(idx, before)
		afterKey := t.indexKeyOf(idx, after)

		if beforeKey == "" && afterKey == "" {
			continue
		}

		d := IndexWriteDelta{Name: name, Kind: t.IndexKind(name), KeyChanged: beforeKey != "" && afterKey != "" && beforeKey != afterKey}
		if beforeKey != "" {
			d.Before = idx.ProjectItem(before)
		}

		if afterKey != "" {
			d.After = idx.ProjectItem(after)
		}

		// An entry that comes out the same is not written: the item changed only attributes that
		// are neither index keys nor projected into the index.
		if d.Before != nil && d.After != nil && !d.KeyChanged && reflect.DeepEqual(d.Before, d.After) {
			continue
		}

		out = append(out, d)
	}

	return out
}

// indexKeyOf returns the key the item has in the index, or "" when it is not indexed there.
func (t *Table) indexKeyOf(idx *index, item map[string]*types.Item) string {
	if len(item) == 0 {
		return ""
	}

	key, err := idx.keySchema.GetKey(t.AttributesDef, item)
	if err != nil {
		return ""
	}

	return key
}

// StoredItem returns a copy of the item stored under the primary key of keyOrItem (a key map or
// a whole item), or nil when there is none. It is a copy because an update changes the stored
// item in place; callers use it to measure an item before a write is applied.
func (t *Table) StoredItem(keyOrItem map[string]*types.Item) map[string]*types.Item {
	key, err := t.KeySchema.GetKey(t.AttributesDef, keyOrItem)
	if err != nil {
		return nil
	}

	item, ok := t.Data[key]
	if !ok {
		return nil
	}

	return deepCopyItemMap(item)
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
	t.ProvisionedThroughput = input.ProvisionedThroughput

	return nil
}

func isValidKeyAttributeType(typ string) bool {
	switch typ {
	case "S", "N", "B":
		return true
	default:
		return false
	}
}

func (t *Table) validateAttributeDefinition(ks keySchema, message string) error {
	if _, ok := t.AttributesDef[ks.HashKey]; !ok {
		return types.NewError("ValidationException", fmt.Sprintf("%sHash Key not specified in Attribute Definitions.  Type unknown.", message), nil)
	}

	if _, ok := t.AttributesDef[ks.RangeKey]; ks.RangeKey != "" && !ok {
		return types.NewError("ValidationException", fmt.Sprintf("%sRange Key not specified in Attribute Definitions.  Type unknown.", message), nil)
	}

	hashType := t.AttributesDef[ks.HashKey]
	if !isValidKeyAttributeType(hashType) {
		return types.NewError("ValidationException", fmt.Sprintf(
			"%sHash key attribute %q has invalid type %q; key attributes must be scalar types S, N, or B.",
			message, ks.HashKey, hashType,
		), nil)
	}

	if ks.RangeKey != "" {
		rangeType := t.AttributesDef[ks.RangeKey]
		if !isValidKeyAttributeType(rangeType) {
			return types.NewError("ValidationException", fmt.Sprintf(
				"%sRange key attribute %q has invalid type %q; key attributes must be scalar types S, N, or B.",
				message, ks.RangeKey, rangeType,
			), nil)
		}
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
	i.provisionedThroughput = gsiInput.ProvisionedThroughput

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

	i.createdAt = time.Now()
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

func validateSearchInputMaps(input QueryInput) error {
	if err := types.ValidateItemMap(input.ExpressionAttributeValues); err != nil {
		return types.NewError("ValidationException", err.Error(), nil)
	}

	if err := types.ValidateItemMap(input.ExclusiveStartKey); err != nil {
		return types.NewError("ValidationException", err.Error(), nil)
	}

	return nil
}

// validateSearchExpressionAttributeNames rejects up front any Query/Scan expression that
// references an attribute name placeholder (#name) not declared in ExpressionAttributeNames.
// DynamoDB validates this before scanning, so it must error even when the table is empty or
// no item matches (when the per-item interpreter checks would never run).
func validateSearchExpressionAttributeNames(input QueryInput) error {
	checks := []interpreter.LabeledExpression{
		{Label: "KeyConditionExpression", Expression: input.KeyConditionExpression},
		{Label: "FilterExpression", Expression: input.FilterExpression},
		{Label: "ProjectionExpression", Expression: input.ProjectionExpression},
	}

	if err := interpreter.ValidateExpressionAttributeNamesDeclared(input.Aliases, checks...); err != nil {
		return types.NewError("ValidationException", err.Error(), nil)
	}

	// A wrong operand count is rejected the same way, before any item is read.
	for _, c := range checks[:2] {
		if err := interpreter.ValidateFunctionOperandCounts(c.Label, c.Expression); err != nil {
			return types.NewError("ValidationException", err.Error(), nil)
		}
	}

	return nil
}

// validateSearchExpressionAttributeValues rejects up front any Query/Scan expression that
// references a value placeholder (:value) missing from ExpressionAttributeValues. DynamoDB
// validates this before reading any item, so it must error even when the table is empty or
// no item matches. It reports the filter before the key condition.
func validateSearchExpressionAttributeValues(input QueryInput) error {
	err := interpreter.ValidateExpressionAttributeValuesDeclared(input.ExpressionAttributeValues,
		interpreter.LabeledExpression{Label: "FilterExpression", Expression: input.FilterExpression},
		interpreter.LabeledExpression{Label: "KeyConditionExpression", Expression: input.KeyConditionExpression},
		// Front ends run write conditions through SearchData as a pre-check.
		interpreter.LabeledExpression{Label: "ConditionExpression", Expression: derefString(input.ConditionExpression)},
	)

	return validationException(err)
}

// validationException wraps a placeholder validation error as DynamoDB's ValidationException.
func validationException(err error) error {
	if err == nil {
		return nil
	}

	return types.NewError("ValidationException", err.Error(), nil)
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

// validateWriteExpressions rejects up front any write expression that references a name or
// value placeholder missing from aliases or values, even when the table is empty or the
// condition would never be evaluated. DynamoDB reports an undeclared name before an undefined
// value, across all the expressions of the request.
func validateWriteExpressions(aliases map[string]string, values map[string]*types.Item, exprs ...interpreter.LabeledExpression) error {
	return validationException(interpreter.ValidateExpressionPlaceholders(aliases, values, exprs...))
}

// validateConditionExpression applies validateWriteExpressions to an optional condition.
func validateConditionExpression(aliases map[string]string, values map[string]*types.Item, condition *string) error {
	return validateWriteExpressions(aliases, values,
		interpreter.LabeledExpression{Label: "ConditionExpression", Expression: derefString(condition)})
}

// deleteAliases converts the pointer-valued ExpressionAttributeNames of a DeleteItemInput.
func deleteAliases(names map[string]*string) map[string]string {
	aliases := make(map[string]string, len(names))

	for k, v := range names {
		if v != nil {
			aliases[k] = *v
		}
	}

	return aliases
}

// validateKeyCondition rejects up front a Query key condition that does not match the key
// schema of the table, or of the index being queried, as DynamoDB does: even when the table
// is empty, and once per request rather than per item.
func (t *Table) validateKeyCondition(input QueryInput) error {
	if input.Scan || input.KeyConditionExpression == "" {
		return nil
	}

	schema := t.KeySchema

	if input.Index != "" {
		idx, ok := t.Indexes[input.Index]
		if !ok {
			return nil
		}

		schema = idx.keySchema
	}

	if err := interpreter.ValidateKeyCondition(input.KeyConditionExpression, input.Aliases, schema.HashKey, schema.RangeKey); err != nil {
		return types.NewError("ValidationException", err.Error(), nil)
	}

	if err := t.validateKeyConditionTypes(input); err != nil {
		return types.NewError("ValidationException", err.Error(), nil)
	}

	return nil
}

// errKeyConditionType is a key condition value whose type differs from the key attribute's.
const errKeyConditionType = "One or more parameter values were invalid: Condition parameter type does not match schema type"

// validateKeyConditionTypes checks the values a (structurally valid) key condition compares
// key attributes to against the types declared for them. Missing placeholders are left to
// the evaluation, which reports them.
func (t *Table) validateKeyConditionTypes(input QueryInput) error {
	for attr, phs := range interpreter.KeyConditionPlaceholders(input.KeyConditionExpression, input.Aliases) {
		want := t.AttributesDef[attr]

		for _, ph := range phs {
			actual := itemTypeName(input.ExpressionAttributeValues[ph])
			if actual != "" && want != "" && actual != want {
				return errors.New(types.WithDetail(errKeyConditionType, "attribute", attr, "expected", want, "actual", actual))
			}
		}
	}

	return nil
}

func validateUpdateItemInputKeysAndValues(input *types.UpdateItemInput) error {
	if err := types.ValidateItemMap(input.Key); err != nil {
		return types.NewError("ValidationException", err.Error(), nil)
	}

	if err := types.ValidateItemMap(input.ExpressionAttributeValues); err != nil {
		return types.NewError("ValidationException", err.Error(), nil)
	}

	return nil
}

// parseStartKey encodes the ExclusiveStartKey. A number DynamoDB rejects is a ValidationException,
// as it is anywhere else in a request: left unreported, the empty key would restart the walk from
// the first item, and a client paging until LastEvaluatedKey is empty would never finish. Other
// problems with the start key keep the leniency described at checkSeek.
func (t *Table) parseStartKey(schema keySchema, startkeyAttr map[string]*types.Item) (string, error) {
	if len(startkeyAttr) == 0 {
		return "", nil
	}

	startKey, err := schema.GetKey(t.AttributesDef, startkeyAttr)
	if errors.Is(err, ErrInvalidNumber) || errors.Is(err, ErrNumberOverflow) || errors.Is(err, ErrNumberUnderflow) {
		return "", types.NewError("ValidationException", err.Error(), nil)
	}

	return startKey, nil
}

func getPrimaryKey(index *index, k string) (string, bool) {
	pk, ok := k, true

	if index != nil {
		pk, ok = index.getPrimaryKey(k)
	}

	return pk, ok
}

// fetchQueryData returns the index to search (nil for the table) and its sorted keys. For a
// Query whose key condition pins the partition key, only the keys of that partition are
// returned, so the search is O(log n + partition size) instead of a walk over every key.
// The key condition is still evaluated on each returned key.
//
// seeked reports whether the keys were narrowed to prefix.
func (t *Table) fetchQueryData(input QueryInput) (idx *index, keys []string, prefix string, seeked bool) {
	if input.Index != "" {
		i := t.Indexes[input.Index]
		prefix, ok := t.partitionPrefix(input, i.keySchema)
		i.startSearch(input.ScanIndexForward, prefix, ok)

		if ok {
			return i, keysWithPrefix(i.sortedKeys, prefix), prefix, true
		}

		return i, i.sortedKeys, "", false
	}

	if prefix, ok := t.partitionPrefix(input, t.KeySchema); ok {
		return nil, keysWithPrefix(t.SortedKeys, prefix), prefix, true
	}

	return nil, t.SortedKeys, "", false
}

// checkSeek keeps a seeked search equivalent to the full walk it replaces: when the
// partition has no keys, the conditions are still evaluated once against the first item the
// walk would have visited, so invalid conditions keep failing whether or not the partition
// holds data.
//
// The key condition's shape and value types are already validated up front by
// validateKeyCondition, so this only surfaces errors that need an evaluation: in the
// FilterExpression, or in the key condition at run time (e.g. an undefined placeholder).
//
// An ExclusiveStartKey outside the partition (or missing key attributes) is not rejected,
// unlike DynamoDB: consumers' tests rely on that leniency, so validating it is a separate,
// release-noted change. Such a start key is never reached, so the page is empty.
func (t *Table) checkSeek(input QueryInput, idx *index, keys []string) error {
	if len(keys) != 0 {
		return nil
	}

	probe, ok := t.firstSearchItem(idx)
	if !ok {
		return nil
	}

	if _, _, err := t.matchKey(input, probe); err != nil {
		return types.NewError("ValidationException", err.Error(), nil)
	}

	return nil
}

// firstSearchItem returns the first item a full walk of the table (or index) would visit.
func (t *Table) firstSearchItem(idx *index) (map[string]*types.Item, bool) {
	if idx != nil {
		refs := idx.orderedRefs()
		if len(refs) == 0 {
			return nil, false
		}

		item, ok := t.Data[refs[0][0]]

		return item, ok
	}

	if len(t.SortedKeys) == 0 {
		return nil, false
	}

	item, ok := t.Data[t.SortedKeys[0]]

	return item, ok
}

// partitionPrefix returns the encoded hash key (see key_encoding.go) of the partition a
// Query's key condition selects, if it selects exactly one. It is a prefix of the stored
// key of every item in that partition and of no other: the encoding is self-delimiting, so
// it can't start another hash value, and every spelling of a number encodes the same.
// Callers use it to skip keys that can't match.
func (t *Table) partitionPrefix(input QueryInput, schema keySchema) (string, bool) {
	if input.Scan || input.KeyConditionExpression == "" {
		return "", false
	}

	ph, ok := interpreter.KeyConditionHashPlaceholder(input.KeyConditionExpression, input.Aliases, schema.HashKey)
	if !ok {
		return "", false
	}

	val, ok := input.ExpressionAttributeValues[ph]
	if !ok || val == nil {
		return "", false
	}

	hash, err := keySchema{HashKey: schema.HashKey}.getKeyValue(t.AttributesDef, map[string]*types.Item{schema.HashKey: val})
	if err != nil {
		return "", false
	}

	return hash, true
}

// keysWithPrefix returns the contiguous run of sorted keys that start with prefix.
func keysWithPrefix(keys []string, prefix string) []string {
	lo := sort.SearchStrings(keys, prefix)
	n := sort.Search(len(keys)-lo, func(j int) bool { return !strings.HasPrefix(keys[lo+j], prefix) })

	return keys[lo : lo+n]
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

func (t *Table) getMatchedItemAndCount(input *QueryInput, pk, startKey string, idx *index) (map[string]*types.Item, map[string]*types.Item, interpreter.ExpressionType, bool, error) {
	storedItem, ok := t.Data[pk]

	lastMatchExpressionType, matched, err := t.matchKey(*input, storedItem)
	if err != nil {
		return nil, nil, lastMatchExpressionType, false, err
	}

	fullCopy := copyItem(storedItem)

	if !ok || !input.started || !matched {
		return fullCopy, fullCopy, lastMatchExpressionType, false, nil
	}

	baseItem := storedItem
	if idx != nil {
		baseItem = idx.ProjectItem(storedItem)
	}

	if input.ProjectionExpression != "" {
		projected, err := t.LangInterpreter.Project(interpreter.ProjectInput{
			Expression: input.ProjectionExpression,
			Item:       baseItem,
			Aliases:    input.Aliases,
		})
		if err != nil {
			return nil, nil, lastMatchExpressionType, false, err
		}

		return projected, fullCopy, lastMatchExpressionType, true, nil
	}

	if idx != nil {
		return baseItem, fullCopy, lastMatchExpressionType, true, nil
	}

	return fullCopy, fullCopy, lastMatchExpressionType, true, nil
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

// SearchResult is one page of results from SearchData.
type SearchResult struct {
	Items            []map[string]*types.Item
	LastEvaluatedKey map[string]*types.Item
	// ScannedCount is the number of items read on this page before the filter
	// expression ran: items before the start key, or outside the key condition, are not read.
	ScannedCount int64
}

// SearchData queries the table based on the input.
func (t *Table) SearchData(input QueryInput) (SearchResult, error) { //nolint:gocognit // query loop with paging, filters, and interpreter errors
	if err := t.validateSelect(input); err != nil {
		return SearchResult{}, err
	}

	if err := validateSearchInputMaps(input); err != nil {
		return SearchResult{}, err
	}

	if err := validateSearchExpressionAttributeNames(input); err != nil {
		return SearchResult{}, err
	}

	if err := validateSearchExpressionAttributeValues(input); err != nil {
		return SearchResult{}, err
	}

	if err := t.validateKeyCondition(input); err != nil {
		return SearchResult{}, err
	}

	items := []map[string]*types.Item{}
	limit := input.Limit
	exclusiveStartKey := input.ExclusiveStartKey
	index, sortedKeys, _, seeked := t.fetchQueryData(input)

	if seeked {
		if err := t.checkSeek(input, index, sortedKeys); err != nil {
			return SearchResult{}, err
		}
	}

	startKey, err := t.parseStartKey(t.KeySchema, exclusiveStartKey)
	if err != nil {
		return SearchResult{}, err
	}

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

		item, keyItem, expressionType, matched, gerr := t.getMatchedItemAndCount(&input, pk, startKey, index)
		if gerr != nil {
			return SearchResult{}, types.NewError("ValidationException", gerr.Error(), nil)
		}

		if matched {
			items = append(items, item)
		}

		scanned++

		// Only evaluated items are read: they count toward Limit and ScannedCount, and can be
		// the LastEvaluatedKey.
		if shouldCountItem(expressionType, matched) {
			count++
			last = keyItem
		}

		if shouldBreakPage(count, limit) {
			break
		}
	}

	return SearchResult{
		Items:            items,
		LastEvaluatedKey: t.getLastKey(last, limit, count, scanned, sortedKeysSize, index),
		ScannedCount:     count,
	}, nil
}

func (t *Table) getLastKey(item map[string]*types.Item, limit, count, scanned, keysSize int64, index *index) map[string]*types.Item {
	if !shouldReturnNextKey(item, count, scanned, limit, keysSize) {
		return map[string]*types.Item{}
	}

	key := t.KeySchema.getKeyItem(item)

	if index != nil {
		iKey := index.keySchema.getKeyItem(item)
		maps.Copy(key, iKey)
	}

	return key
}

// InterpreterMatch evaluates a match expression against an item
func (t *Table) InterpreterMatch(input interpreter.MatchInput) (bool, error) {
	if t.UseNativeInterpreter {
		matched, err := t.NativeInterpreter.Match(input)
		if err == nil {
			return matched, nil
		}
	}

	matched, err := t.LangInterpreter.Match(input)
	if err != nil {
		return false, err
	}

	return matched, nil
}

func (t *Table) matchKey(input QueryInput, item map[string]*types.Item) (interpreter.ExpressionType, bool, error) { //nolint:gocognit // key, filter, and conditional expression branches
	var lastMatchExpressionType interpreter.ExpressionType
	matched := input.Scan

	if input.KeyConditionExpression != "" {
		var err error

		matched, err = t.InterpreterMatch(interpreter.MatchInput{
			TableName:      t.Name,
			Expression:     input.KeyConditionExpression,
			ExpressionType: interpreter.ExpressionTypeKey,
			Item:           item,
			Aliases:        input.Aliases,
			Attributes:     input.ExpressionAttributeValues,
		})
		if err != nil {
			return interpreter.ExpressionTypeKey, false, err
		}
		lastMatchExpressionType = interpreter.ExpressionTypeKey
	}

	if input.FilterExpression != "" {
		if matched {
			m, err := t.InterpreterMatch(interpreter.MatchInput{
				TableName:      t.Name,
				Expression:     input.FilterExpression,
				ExpressionType: interpreter.ExpressionTypeFilter,
				Item:           item,
				Aliases:        input.Aliases,
				Attributes:     input.ExpressionAttributeValues,
			})
			if err != nil {
				return interpreter.ExpressionTypeFilter, false, err
			}
			matched = m
			// Only items that passed the key condition were evaluated by the filter: an item
			// that failed it (e.g. another partition sharing the search prefix) must not count
			// toward Limit.
			lastMatchExpressionType = interpreter.ExpressionTypeFilter
		}
	}

	if input.ConditionExpression != nil && *input.ConditionExpression != "" {
		var err error

		matched, err = t.InterpreterMatch(interpreter.MatchInput{
			TableName:      t.Name,
			Expression:     *input.ConditionExpression,
			ExpressionType: interpreter.ExpressionTypeConditional,
			Item:           item,
			Aliases:        input.Aliases,
			Attributes:     input.ExpressionAttributeValues,
		})
		if err != nil {
			return interpreter.ExpressionTypeConditional, false, err
		}
		lastMatchExpressionType = interpreter.ExpressionTypeConditional
	}

	return lastMatchExpressionType, matched, nil
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

// TableSnapshot captures a point-in-time copy of mutable table state for transactional rollback
type TableSnapshot struct {
	data       map[string]map[string]*types.Item
	sortedKeys []string
	indexes    map[string]indexSnapshot
}

// Snapshot returns a deep copy of the table's mutable state
func (t *Table) Snapshot() TableSnapshot {
	data := make(map[string]map[string]*types.Item, len(t.Data))
	for k, v := range t.Data {
		data[k] = deepCopyItemMap(v)
	}

	keys := make([]string, len(t.SortedKeys))
	copy(keys, t.SortedKeys)

	indexes := make(map[string]indexSnapshot, len(t.Indexes))
	for name, idx := range t.Indexes {
		indexes[name] = idx.snapshot()
	}

	return TableSnapshot{data: data, sortedKeys: keys, indexes: indexes}
}

// Restore replaces the table's mutable state with a previously taken snapshot
func (t *Table) Restore(s TableSnapshot) {
	t.Data = s.data
	t.SortedKeys = s.sortedKeys

	for name, idx := range t.Indexes {
		if snap, ok := s.indexes[name]; ok {
			idx.restore(snap)
		}
	}
}

// Put puts an item into the table. It returns the item it replaced when
// ReturnValues is ALL_OLD, and nil otherwise (or when there was none).
func (t *Table) Put(input *types.PutItemInput) (map[string]*types.Item, error) { //nolint:gocognit // conditional write, key resolution, GSI updates
	if err := ValidateNoneOrAllOldReturnValues(input.ReturnValues); err != nil {
		return nil, err
	}

	if err := ValidateReturnValuesOnConditionCheckFailure(input.ReturnValuesOnConditionCheckFailure); err != nil {
		return nil, err
	}

	if err := types.ValidateItemMap(input.Item); err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	if err := types.ValidateItemMap(input.ExpressionAttributeValues); err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	if err := validateConditionExpression(input.ExpressionAttributeNames, input.ExpressionAttributeValues, input.ConditionExpression); err != nil {
		return nil, err
	}

	item := copyItem(input.Item)

	key, err := t.KeySchema.GetKey(t.AttributesDef, input.Item)
	if err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	// support conditional writes
	if input.ConditionExpression != nil {
		_, matched, merr := t.matchKey(QueryInput{
			Index:                     PrimaryIndexName,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Aliases:                   input.ExpressionAttributeNames,
			Limit:                     1,
			ConditionExpression:       input.ConditionExpression,
		}, t.getItem(key))
		if merr != nil {
			return nil, types.NewError("ValidationException", merr.Error(), nil)
		}

		if !matched {
			return nil, ConditionalCheckFailed(input.ReturnValuesOnConditionCheckFailure, t.getItem(key))
		}
	}

	old := t.Data[key]

	t.setItem(key, item)

	for _, index := range t.Indexes {
		err := index.putData(key, item)
		if err != nil {
			return nil, types.NewError("ValidationException", err.Error(), nil)
		}
	}

	if input.ReturnValues == nil || *input.ReturnValues != "ALL_OLD" {
		old = nil
	}

	return old, nil
}

func (t *Table) interpreterUpdate(input interpreter.UpdateInput) error {
	if t.UseNativeInterpreter {
		return t.NativeInterpreter.Update(input)
	}

	return t.LangInterpreter.Update(input)
}

// ValidateNoneOrAllOldReturnValues accepts the ReturnValues modes that PutItem
// and DeleteItem support: empty, NONE and ALL_OLD.
func ValidateNoneOrAllOldReturnValues(rv *string) error {
	return validateNoneOrAllOld(rv, func(value string) string {
		return types.WithDetail("Return values set to invalid value", "value", value, "field", "ReturnValues")
	})
}

// validateNoneOrAllOld accepts empty, NONE and ALL_OLD, and builds the
// ValidationException message of any other value with invalid.
func validateNoneOrAllOld(rv *string, invalid func(value string) string) error {
	if rv == nil {
		return nil
	}

	switch *rv {
	case "", "NONE", "ALL_OLD":
		return nil
	default:
		return types.NewError("ValidationException", invalid(*rv), nil)
	}
}

func validateUpdateItemReturnValues(rv *string) error {
	if rv == nil || *rv == "" {
		return nil
	}

	switch *rv {
	case "NONE", "ALL_OLD", "UPDATED_OLD", "UPDATED_NEW", "ALL_NEW":
		return nil
	default:
		return types.NewError("ValidationException",
			fmt.Sprintf("1 validation error detected: Value '%s' at 'returnValues' failed to satisfy constraint: Member must satisfy enum value set: [NONE, ALL_OLD, UPDATED_OLD, UPDATED_NEW, ALL_NEW]", *rv),
			nil)
	}
}

func dynamoItemEqual(a, b *types.Item) bool {
	return reflect.DeepEqual(a, b)
}

func changedUpdateAttributeKeys(oldItem, newItem map[string]*types.Item) []string {
	keys := make(map[string]struct{})

	for k := range oldItem {
		keys[k] = struct{}{}
	}

	for k := range newItem {
		keys[k] = struct{}{}
	}

	changed := make([]string, 0, len(keys))

	for k := range keys {
		ov, okO := oldItem[k]
		nv, okN := newItem[k]

		if !okO {
			ov = nil
		}

		if !okN {
			nv = nil
		}

		if !dynamoItemEqual(ov, nv) {
			changed = append(changed, k)
		}
	}

	sort.Strings(changed)

	return changed
}

func buildUpdateItemReturnAttributes(rv *string, oldItem, newItem map[string]*types.Item) map[string]*types.Item {
	mode := ""

	if rv != nil {
		mode = *rv
	}

	switch mode {
	case "", "NONE":
		return nil
	case "ALL_NEW":
		return copyItem(newItem)
	case "ALL_OLD":
		return copyItem(oldItem)
	case "UPDATED_OLD":
		ck := changedUpdateAttributeKeys(oldItem, newItem)
		if len(ck) == 0 {
			return map[string]*types.Item{}
		}

		out := make(map[string]*types.Item, len(ck))

		for _, k := range ck {
			if v, ok := oldItem[k]; ok {
				out[k] = deepCopyTypesItem(v)
			}
		}

		return out
	case "UPDATED_NEW":
		ck := changedUpdateAttributeKeys(oldItem, newItem)
		if len(ck) == 0 {
			return map[string]*types.Item{}
		}

		out := make(map[string]*types.Item, len(ck))

		for _, k := range ck {
			if v, ok := newItem[k]; ok {
				out[k] = deepCopyTypesItem(v)
			}
		}

		return out
	default:
		return nil
	}
}

// Update updates an item in the table based on the input
func (t *Table) Update(input *types.UpdateItemInput) (map[string]*types.Item, error) { //nolint:gocognit,gocyclo // conditional writes, interpreter update, GSI updates
	if err := validateUpdateItemInputKeysAndValues(input); err != nil {
		return nil, err
	}

	if err := validateUpdateItemReturnValues(input.ReturnValues); err != nil {
		return nil, err
	}

	if err := ValidateReturnValuesOnConditionCheckFailure(input.ReturnValuesOnConditionCheckFailure); err != nil {
		return nil, err
	}

	// DynamoDB reports the update expression before the condition.
	if err := validateWriteExpressions(input.ExpressionAttributeNames, input.ExpressionAttributeValues,
		interpreter.LabeledExpression{Label: "UpdateExpression", Expression: input.UpdateExpression},
		interpreter.LabeledExpression{Label: "ConditionExpression", Expression: derefString(input.ConditionExpression)},
	); err != nil {
		return nil, err
	}

	if err := t.KeySchema.validatePrimaryKeyMap(input.Key); err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	if !t.KeySchema.Secondary {
		if err := language.ValidateUpdateExpressionDoesNotTargetPrimaryKey(
			input.UpdateExpression,
			input.ExpressionAttributeNames,
			t.KeySchema.HashKey,
			t.KeySchema.RangeKey,
		); err != nil {
			return nil, types.NewError("ValidationException", err.Error(), nil)
		}
	}

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

		_, matched, merr := t.matchKey(query, item)
		if merr != nil {
			return nil, types.NewError("ValidationException", merr.Error(), nil)
		}

		if !matched {
			return nil, ConditionalCheckFailed(input.ReturnValuesOnConditionCheckFailure, item)
		}
	}

	if !ok {
		// types creates a new item when the item does not exists
		item = copyItem(input.Key)
	}

	oldItem := deepCopyItemMap(item)

	err = t.interpreterUpdate(interpreter.UpdateInput{
		TableName:  t.Name,
		Expression: input.UpdateExpression,
		Item:       item,
		Attributes: input.ExpressionAttributeValues,
		Aliases:    input.ExpressionAttributeNames,
	})
	if err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	t.setItem(key, item)

	// update secondary Indexes
	for _, index := range t.Indexes {
		err := index.putData(key, item)
		if err != nil {
			return nil, types.NewError("ValidationException", err.Error(), nil)
		}
	}

	newItem := copyItem(item)

	return buildUpdateItemReturnAttributes(input.ReturnValues, oldItem, newItem), nil
}

// Delete deletes an item in the table based on the input
func (t *Table) Delete(input *types.DeleteItemInput) (map[string]*types.Item, error) { //nolint:gocognit,gocyclo // conditional delete, key resolution, index cleanup
	if err := ValidateNoneOrAllOldReturnValues(input.ReturnValues); err != nil {
		return nil, err
	}

	if err := ValidateReturnValuesOnConditionCheckFailure(input.ReturnValuesOnConditionCheckFailure); err != nil {
		return nil, err
	}

	if err := types.ValidateItemMap(input.Key); err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	if err := types.ValidateItemMap(input.ExpressionAttributeValues); err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	if err := validateConditionExpression(deleteAliases(input.ExpressionAttributeNames), input.ExpressionAttributeValues, input.ConditionExpression); err != nil {
		return nil, err
	}

	if err := t.KeySchema.validatePrimaryKeyMap(input.Key); err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	key, err := t.KeySchema.GetKey(t.AttributesDef, input.Key)
	if err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	item, ok := t.Data[key]

	existingForCond := item
	if !ok {
		existingForCond = map[string]*types.Item{}
	}

	if input.ConditionExpression != nil && *input.ConditionExpression != "" {
		aliases := map[string]string{}

		for k, v := range input.ExpressionAttributeNames {
			if v != nil {
				aliases[k] = *v
			}
		}

		_, matched, merr := t.matchKey(QueryInput{
			Index:                     PrimaryIndexName,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
			Aliases:                   aliases,
			Limit:                     1,
			ConditionExpression:       input.ConditionExpression,
		}, existingForCond)
		if merr != nil {
			return nil, types.NewError("ValidationException", merr.Error(), nil)
		}

		if !matched {
			return nil, ConditionalCheckFailed(input.ReturnValuesOnConditionCheckFailure, item)
		}
	}

	if !ok {
		return nil, nil
	}

	item = copyItem(item)

	delete(t.Data, key)

	pos := sort.SearchStrings(t.SortedKeys, key)
	if pos == len(t.SortedKeys) {
		return deleteReturnAttributes(input.ReturnValues, item), nil
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

	return deleteReturnAttributes(input.ReturnValues, item), nil
}

func deleteReturnAttributes(rv *string, old map[string]*types.Item) map[string]*types.Item {
	if rv == nil || *rv != "ALL_OLD" {
		return nil
	}

	return old
}

// Description returns the description of a table
func (t *Table) Description(name string) *types.TableDescription {
	gsi, lsi := t.IndexesDescription()

	// minidyn tables are always ready, and deletion protection is not supported.
	return &types.TableDescription{
		TableName:                 name,
		TableStatus:               "ACTIVE",
		AttributeDefinitions:      t.attributeDefinitionsDescription(),
		BillingModeSummary:        t.billingModeSummary(),
		ProvisionedThroughput:     t.provisionedThroughputDescription(),
		DeletionProtectionEnabled: new(false),
		ItemCount:                 int64(len(t.SortedKeys)),
		KeySchema:                 t.KeySchema.describe(),
		GlobalSecondaryIndexes:    gsi,
		LocalSecondaryIndexes:     lsi,
	}
}

func (t *Table) isOnDemand() bool {
	return types.StringValue(t.BillingMode) == "PAY_PER_REQUEST"
}

// attributeDefinitionsDescription lists the attributes used by the key schemas of the table
// and its indexes, as DynamoDB does, ordered by name.
func (t *Table) attributeDefinitionsDescription() []types.AttributeDefinition {
	used := map[string]struct{}{}

	schemas := []keySchema{t.KeySchema}
	for _, index := range t.Indexes {
		schemas = append(schemas, index.keySchema)
	}

	for _, ks := range schemas {
		used[ks.HashKey] = struct{}{}

		if ks.RangeKey != "" {
			used[ks.RangeKey] = struct{}{}
		}
	}

	names := make([]string, 0, len(used))

	for attrName := range used {
		if _, ok := t.AttributesDef[attrName]; ok {
			names = append(names, attrName)
		}
	}

	sort.Strings(names)

	defs := make([]types.AttributeDefinition, 0, len(names))
	for _, attrName := range names {
		defs = append(defs, types.AttributeDefinition{
			AttributeName: new(attrName),
			AttributeType: new(t.AttributesDef[attrName]),
		})
	}

	return defs
}

func (t *Table) billingModeSummary() *types.BillingModeSummary {
	if !t.isOnDemand() {
		return nil
	}

	return &types.BillingModeSummary{BillingMode: "PAY_PER_REQUEST"}
}

// provisionedThroughputDescription reports the requested capacity, or 0/0 for on-demand tables
// whatever capacity the request carried.
func (t *Table) provisionedThroughputDescription() *types.ProvisionedThroughput {
	if t.isOnDemand() || t.ProvisionedThroughput == nil {
		return &types.ProvisionedThroughput{}
	}

	throughput := *t.ProvisionedThroughput

	return &throughput
}

// IndexesDescription returns the description of the table indexes
func (t *Table) IndexesDescription() ([]types.GlobalSecondaryIndexDescription, []types.LocalSecondaryIndexDescription) {
	gsi := []types.GlobalSecondaryIndexDescription{}
	lsi := []types.LocalSecondaryIndexDescription{}

	for indexName, index := range t.Indexes {
		switch index.typ {
		case indexTypeGlobal:
			gsi = append(gsi, t.globalIndexDescription(indexName, index))
		case indexTypeLocal:
			lsi = append(lsi, types.LocalSecondaryIndexDescription{
				IndexName:  &indexName,
				ItemCount:  index.count(),
				KeySchema:  index.keySchema.describe(),
				Projection: index.projection,
			})
		}
	}

	return gsi, lsi
}

func (t *Table) globalIndexDescription(indexName string, index *index) types.GlobalSecondaryIndexDescription {
	status := "ACTIVE"
	if t.IndexActivationDelay > 0 && time.Since(index.createdAt) < t.IndexActivationDelay {
		status = "CREATING"
	}

	return types.GlobalSecondaryIndexDescription{
		IndexName:             &indexName,
		ItemCount:             index.count(),
		KeySchema:             index.keySchema.describe(),
		Projection:            index.projection,
		IndexStatus:           &status,
		ProvisionedThroughput: index.provisionedThroughput,
	}
}

// ApplyIndexChanges applies the global secondary index changes in order and returns the
// description of the table as DynamoDB reports it in an UpdateTable response: created indexes
// carry their creation markers and deleted ones are still listed, as DELETING. On error the
// description of the table is still returned.
func (t *Table) ApplyIndexChanges(name string, changes []*types.GlobalSecondaryIndexUpdate) (*types.TableDescription, error) {
	var (
		created []string
		dropped []types.GlobalSecondaryIndexDescription
		err     error
	)

	for _, change := range changes {
		var droppedDesc *types.GlobalSecondaryIndexDescription

		if change.Delete != nil {
			if idx, ok := t.Indexes[*change.Delete.IndexName]; ok {
				d := t.globalIndexDescription(*change.Delete.IndexName, idx)
				d.IndexStatus = new("DELETING")
				droppedDesc = &d
			}
		}

		if err = t.ApplyIndexChange(change); err != nil {
			break
		}

		if change.Create != nil {
			created = append(created, *change.Create.IndexName)
		}

		if droppedDesc != nil {
			dropped = append(dropped, *droppedDesc)
		}
	}

	desc := t.Description(name)
	for i := range desc.GlobalSecondaryIndexes {
		if slices.Contains(created, *desc.GlobalSecondaryIndexes[i].IndexName) {
			t.markIndexCreating(&desc.GlobalSecondaryIndexes[i])
		}
	}

	desc.GlobalSecondaryIndexes = append(desc.GlobalSecondaryIndexes, dropped...)

	return desc, err
}

// markIndexCreating sets the fields DynamoDB reports for an index created by UpdateTable.
func (t *Table) markIndexCreating(g *types.GlobalSecondaryIndexDescription) {
	g.Backfilling = new(false)

	if t.isOnDemand() {
		g.OnDemandThroughput = &types.OnDemandThroughput{
			MaxReadRequestUnits:  new(int64(-1)),
			MaxWriteRequestUnits: new(int64(-1)),
		}
	}
}

// ValidateReturnValuesOnConditionCheckFailure accepts the modes DynamoDB defines
// for ReturnValuesOnConditionCheckFailure: empty, ALL_OLD and NONE.
//
// The front ends call it before their own expression pre-check and the table lookup, to keep
// DynamoDB's order; Put, Update and Delete call it again, so core never runs with a bad mode.
func ValidateReturnValuesOnConditionCheckFailure(rv *string) error {
	return validateNoneOrAllOld(rv, func(value string) string {
		return returnValuesOnConditionCheckFailureMessage(value, "returnValuesOnConditionCheckFailure")
	})
}

// ValidateTransactItemReturnValuesOnConditionCheckFailure validates the mode of the Put, Update,
// Delete or ConditionCheck (shape: "put", "update", "delete" or "conditionCheck") at position
// index of TransactWriteItems. DynamoDB Local accepts any value here; minidyn follows the API
// reference ("Valid Values: ALL_OLD | NONE") and rejects the whole request.
func ValidateTransactItemReturnValuesOnConditionCheckFailure(index int, shape string, rv *string) error {
	return validateNoneOrAllOld(rv, func(value string) string {
		path := fmt.Sprintf("transactItems.%d.member.%s.returnValuesOnConditionCheckFailure", index+1, shape)

		// DynamoDB's text already names the field and the 1-based item, so no detail is added.
		return returnValuesOnConditionCheckFailureMessage(value, path)
	})
}

func returnValuesOnConditionCheckFailureMessage(value, path string) string {
	return fmt.Sprintf("1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]", value, path)
}

// ConditionalCheckFailed builds the error of a failed condition. With
// ReturnValuesOnConditionCheckFailure=ALL_OLD it carries the existing item, if there is one.
func ConditionalCheckFailed(mode *string, item map[string]*types.Item) *types.ConditionalCheckFailedException {
	checkErr := &types.ConditionalCheckFailedException{MessageText: ErrConditionalRequestFailed.Error()}

	if mode != nil && *mode == "ALL_OLD" && len(item) > 0 {
		checkErr.Item = deepCopyItemMap(item)
	}

	return checkErr
}
