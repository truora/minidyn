package core

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strconv"
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

// ValidateSearchIndex rejects the IndexName of a Query or Scan when the table has no such
// index, as DynamoDB does: before anything else of the request is looked at. The primary index
// (an empty name) is always there. SearchData applies it first; the front ends call it ahead
// of their own pre-check of the expression attributes, which DynamoDB runs after it.
func (t *Table) ValidateSearchIndex(name string) error {
	if name == PrimaryIndexName {
		return nil
	}

	if _, ok := t.Indexes[name]; !ok {
		return validationError(msgNoSuchIndex+name, "table", t.Name)
	}

	return nil
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

// sortedIndexNames returns the table's index names in a stable order.
func (t *Table) sortedIndexNames() []string {
	names := make([]string, 0, len(t.Indexes))
	for name := range t.Indexes {
		names = append(names, name)
	}

	slices.Sort(names)

	return names
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

	names := t.sortedIndexNames()
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

// ReadItem is the result of reading one key. Stored is the item as stored (nil when there is
// none), not a copy: callers copy what they hand out, and front ends measure it for capacity.
// Projected is the attributes the ProjectionExpression selects, or Stored without one.
type ReadItem struct {
	Stored, Projected map[string]*types.Item
}

// GetItem reads the item stored under key. Like DynamoDB's GetItem it rejects a bad
// ProjectionExpression (its syntax, then an undeclared #name) before it looks at the key.
func (t *Table) GetItem(key map[string]*types.Item, projectionExpr string, aliases map[string]string) (ReadItem, error) {
	checked, err := checkProjection(projectionExpr, aliases)
	if err != nil {
		return ReadItem{}, err
	}

	storageKey, err := t.checkedKeyOf(key)
	if err != nil {
		return ReadItem{}, err
	}

	return t.readItem(storageKey, checked)
}

// BatchGetItem reads the keys of one table of a BatchGetItem, one ReadItem per key in order. As in
// DynamoDB it validates every key before the ProjectionExpression, which it checks once.
func (t *Table) BatchGetItem(keys []map[string]*types.Item, projectionExpr string, aliases map[string]string) ([]ReadItem, error) {
	storageKeys := make([]string, len(keys))

	for i, key := range keys {
		storageKey, err := t.checkedKeyOf(key)
		if err != nil {
			return nil, err
		}

		storageKeys[i] = storageKey
	}

	checked, err := checkProjection(projectionExpr, aliases)
	if err != nil {
		return nil, err
	}

	items := make([]ReadItem, len(keys))

	for i, storageKey := range storageKeys {
		items[i], err = t.readItem(storageKey, checked)
		if err != nil {
			return nil, err
		}
	}

	return items, nil
}

// readItem reads the item under storageKey and projects it; a nil checked selects everything.
func (t *Table) readItem(storageKey string, checked *interpreter.CheckedProjection) (ReadItem, error) {
	stored := t.Data[storageKey]
	if checked == nil {
		return ReadItem{Stored: stored, Projected: stored}, nil
	}

	projected, err := t.LangInterpreter.ProjectChecked(checked, stored)
	if err != nil {
		return ReadItem{}, types.NewError("ValidationException", err.Error(), nil)
	}

	return ReadItem{Stored: stored, Projected: projected}, nil
}

// checkProjection validates a ProjectionExpression (nil for none) and returns its parse.
func checkProjection(expr string, aliases map[string]string) (*interpreter.CheckedProjection, error) {
	if expr == "" {
		return nil, nil
	}

	checked, err := interpreter.CheckProjection(expr, aliases)
	if err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	return checked, nil
}

// SetAttributeDefinition sets the attribute definitions of a table. It checks all of them first,
// so a malformed entry leaves the table's definitions unchanged.
func (t *Table) SetAttributeDefinition(attrs []*types.AttributeDefinition) error {
	for _, attr := range attrs {
		if attr == nil || attr.AttributeName == nil {
			return validationError(msgNoKeyAttribute, "table", t.Name)
		}

		if *attr.AttributeName == "" {
			return validationError(msgEmptyAttributeName, "table", t.Name)
		}

		if types.StringValue(attr.AttributeType) == "" {
			return validationError(msgNoAttributeType, "attribute", *attr.AttributeName, "table", t.Name)
		}
	}

	for _, attr := range attrs {
		t.AttributesDef[*attr.AttributeName] = *attr.AttributeType
	}

	return nil
}

// validationError builds a ValidationException from DynamoDB's text and, optionally, minidyn-only
// detail as alternating keys and values (see types.WithDetail).
func validationError(message string, detail ...string) types.Error {
	return types.NewError("ValidationException", types.WithDetail(message, detail...), nil)
}

// UpdateTable applies the attribute definitions and global secondary index changes of an
// UpdateTable request, or none of them: when any part is rejected the table is left as it was.
// The description of the table is returned either way.
//
// The request is checked in the order DynamoDB Local reports it: the structure of every index
// change, then the attribute definitions, then what depends on them (the key attributes of an
// index to create must be defined, by the table or by this request).
func (t *Table) UpdateTable(name string, attrs []*types.AttributeDefinition, changes []*types.GlobalSecondaryIndexUpdate) (*types.TableDescription, error) {
	if err := t.validateIndexChanges(changes); err != nil {
		return t.Description(name), err
	}

	previous := maps.Clone(t.AttributesDef)

	if err := t.SetAttributeDefinition(attrs); err != nil {
		return t.Description(name), err
	}

	created, err := t.buildCreatedIndexes(changes)
	if err != nil {
		t.AttributesDef = previous

		return t.Description(name), err
	}

	return t.applyIndexChanges(name, changes, created), nil
}

// validateIndexChanges checks the structure of every entry of an UpdateTable request before any
// of them is applied, as DynamoDB does: a request with one bad entry changes nothing.
//
// DynamoDB allows one index to be created or deleted per request and counts that first, so the
// limit is reported ahead of any problem with an entry. The entries are then checked in order.
func (t *Table) validateIndexChanges(changes []*types.GlobalSecondaryIndexUpdate) error {
	onlineChanges := 0

	for _, change := range changes {
		if change == nil {
			continue
		}

		if change.Create != nil {
			onlineChanges++
		}

		if change.Delete != nil {
			onlineChanges++
		}
	}

	if onlineChanges > 1 {
		return types.NewError("LimitExceededException", types.WithDetail(msgIndexLimit, "table", t.Name, "changes", strconv.Itoa(onlineChanges)), nil)
	}

	touched := map[string]struct{}{}

	for _, change := range changes {
		if err := t.validateIndexChange(change, touched); err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) validateIndexChange(change *types.GlobalSecondaryIndexUpdate, touched map[string]struct{}) error {
	if change == nil || (change.Create == nil && change.Delete == nil && change.Update == nil) {
		return validationError(msgNullGSIAction)
	}

	var name *string

	switch {
	case change.Create != nil:
		name = change.Create.IndexName
	case change.Delete != nil:
		name = change.Delete.IndexName
	default:
		name = change.Update.IndexName
	}

	indexName := types.StringValue(name)

	// Local accepts or fails with a 500 on these combinations, the message is minidyn's pick
	if (change.Create != nil && change.Update != nil) || (change.Delete != nil && change.Update != nil) {
		return validationError(msgOneUpdatePerName,
			"reason", "create or delete combined with update in one entry", "index", indexName)
	}

	if indexName == "" {
		return validationError(msgInvalidGSIName)
	}

	if _, ok := touched[indexName]; ok {
		return validationError(msgOneUpdatePerName, "index", indexName)
	}

	touched[indexName] = struct{}{}

	_, exists := t.Indexes[indexName]

	switch {
	case change.Create != nil:
		if err := validateIndexShape(indexName, change.Create.Projection, change.Create.KeySchema); err != nil {
			return err
		}

		if exists {
			return validationError(msgIndexExists, "index", indexName, "table", t.Name)
		}
	case change.Delete != nil && !exists:
		return types.NewError("ResourceNotFoundException", types.WithDetail("Requested resource not found", "index", indexName, "table", t.Name), nil)
	case change.Update != nil && !exists:
		return validationError(msgInvalidGSIName, "index", indexName, "table", t.Name)
	}

	return nil
}

// buildCreatedIndexes builds, without registering them, the indexes the changes create, so every
// error applying them could give surfaces before the table is touched.
func (t *Table) buildCreatedIndexes(changes []*types.GlobalSecondaryIndexUpdate) (map[string]*index, error) {
	created := map[string]*index{}

	for _, change := range changes {
		if change.Create == nil {
			continue
		}

		i, err := buildGSI(t, &types.GlobalSecondaryIndex{
			IndexName:             change.Create.IndexName,
			KeySchema:             change.Create.KeySchema,
			Projection:            change.Create.Projection,
			ProvisionedThroughput: change.Create.ProvisionedThroughput,
		})
		if err != nil {
			return nil, err
		}

		created[*change.Create.IndexName] = i
	}

	return created, nil
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

// ApplyIndexChange applies the index change after checking it as UpdateTable does.
func (t *Table) ApplyIndexChange(change *types.GlobalSecondaryIndexUpdate) error {
	_, err := t.ApplyIndexChanges(t.Name, []*types.GlobalSecondaryIndexUpdate{change})

	return err
}

// AddGlobalIndexes adds global indexes to a table
func (t *Table) AddGlobalIndexes(input []*types.GlobalSecondaryIndex) error {
	return t.AddIndexes(input, nil)
}

// AddLocalIndexes adds local indexes to a table
func (t *Table) AddLocalIndexes(input []*types.LocalSecondaryIndex) error {
	return t.AddIndexes(nil, input)
}

// AddIndexes adds the indexes of a CreateTable request, or none of them. DynamoDB Local checks
// the local indexes (name, definition and key attributes) before the global ones, so a problem
// with a local index is reported ahead of any with a global one.
func (t *Table) AddIndexes(gsis []*types.GlobalSecondaryIndex, lsis []*types.LocalSecondaryIndex) error {
	if gsis != nil && len(gsis) == 0 {
		return types.NewError("ValidationException", "GSI list is empty/invalid", nil)
	}

	if lsis != nil && len(lsis) == 0 {
		return types.NewError("ValidationException", "ValidationException: LSI list is empty/invalid", nil)
	}

	built := map[string]*index{}

	for _, lsi := range lsis {
		name, i, err := t.buildCreateTableLSI(lsi)
		if err != nil {
			return err
		}

		built[name] = i
	}

	for _, gsi := range gsis {
		name, i, err := t.buildCreateTableGSI(gsi)
		if err != nil {
			return err
		}

		built[name] = i
	}

	maps.Copy(t.Indexes, built)

	return nil
}

// buildCreateTableLSI and buildCreateTableGSI check and build an index of a CreateTable request. A
// nil entry is an empty index, which has no name.
func (t *Table) buildCreateTableLSI(lsi *types.LocalSecondaryIndex) (string, *index, error) {
	if lsi == nil {
		lsi = &types.LocalSecondaryIndex{}
	}

	if err := validateIndexName(lsi.IndexName); err != nil {
		return "", nil, err
	}

	if err := validateIndexShape(*lsi.IndexName, lsi.Projection, lsi.KeySchema); err != nil {
		return "", nil, err
	}

	i, err := buildLSI(t, lsi)

	return *lsi.IndexName, i, err
}

func (t *Table) buildCreateTableGSI(gsi *types.GlobalSecondaryIndex) (string, *index, error) {
	if gsi == nil {
		gsi = &types.GlobalSecondaryIndex{}
	}

	if err := validateIndexName(gsi.IndexName); err != nil {
		return "", nil, err
	}

	if err := validateIndexShape(*gsi.IndexName, gsi.Projection, gsi.KeySchema); err != nil {
		return "", nil, err
	}

	i, err := buildGSI(t, gsi)
	if err == nil {
		i.createdAt = time.Now()
	}

	return *gsi.IndexName, i, err
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

	if err := t.validateKeyConditionTypes(input, input.Index, schema); err != nil {
		return types.NewError("ValidationException", err.Error(), nil)
	}

	return nil
}

// errKeyConditionType is a key condition value whose type differs from the key attribute's.
const errKeyConditionType = "One or more parameter values were invalid: Condition parameter type does not match schema type"

// validateKeyConditionTypes checks the values a (structurally valid) key condition compares
// key attributes to: their types against the ones declared for the attributes, that BETWEEN
// bounds are in order and that none is an empty S or B value. Missing placeholders are left
// to the evaluation, which reports them.
func (t *Table) validateKeyConditionTypes(input QueryInput, indexName string, schema keySchema) error {
	placeholders := interpreter.KeyConditionPlaceholders(input.KeyConditionExpression, input.Aliases)

	// DynamoDB checks the bounds of a BETWEEN before any other value of the condition.
	if rangeKey := schema.RangeKey; rangeKey != "" {
		bounds := make([]*types.Item, 0, 2)
		for _, ph := range placeholders[rangeKey] {
			bounds = append(bounds, input.ExpressionAttributeValues[ph])
		}

		if err := validateBetweenBounds(bounds, indexName, rangeKey); err != nil {
			return err
		}
	}

	for _, attr := range []string{schema.HashKey, schema.RangeKey} {
		phs := placeholders[attr]
		want := t.AttributesDef[attr]
		values := make([]*types.Item, 0, len(phs))

		for _, ph := range phs {
			v := input.ExpressionAttributeValues[ph]

			actual := itemTypeName(v)
			if actual != "" && want != "" && actual != want {
				return errors.New(types.WithDetail(errKeyConditionType, "attribute", attr, "expected", want, "actual", actual))
			}

			values = append(values, v)
		}

		for _, v := range values {
			if kind := emptyValueKind(v); kind != "" {
				return emptyKeyError(kind, indexName, attr)
			}
		}
	}

	return nil
}

// validateBetweenBounds checks the two bounds of a key condition BETWEEN on attribute attr as
// DynamoDB does: they must have the same type and be in order, compared by their type (S and
// B by bytes, N numerically). indexName is "" for the table's primary key. Bounds of any other
// type, or a number DynamoDB rejects, are left to the checks that follow.
func validateBetweenBounds(bounds []*types.Item, indexName, attr string) error {
	if len(bounds) != 2 || bounds[0] == nil || bounds[1] == nil {
		return nil
	}

	lo, hi := bounds[0], bounds[1]

	loType, hiType := itemTypeName(lo), itemTypeName(hi)
	if !isKeyType(loType) || !isKeyType(hiType) {
		return nil
	}

	detail := []string{"attribute", attr}
	if indexName != PrimaryIndexName {
		detail = append(detail, "index", indexName)
	}

	operands := fmt.Sprintf("lower bound operand: AttributeValue: %s, upper bound operand: AttributeValue: %s", //nolint:staticcheck,revive // AWS message
		boundText(lo), boundText(hi))

	if loType != hiType {
		return errors.New(types.WithDetail(betweenSameTypeText+operands, detail...))
	}

	order, ok := compareBounds(lo, hi)
	if ok && order > 0 {
		return errors.New(types.WithDetail(betweenOrderText+operands, detail...))
	}

	return nil
}

const (
	betweenSameTypeText = "Invalid KeyConditionExpression: The BETWEEN operator requires same data type for lower and upper bounds; "
	betweenOrderText    = "Invalid KeyConditionExpression: The BETWEEN operator requires upper bound to be greater than or equal to lower bound; "
)

// isKeyType reports whether typ is a type a key attribute can have.
func isKeyType(typ string) bool { return typ == "S" || typ == "N" || typ == "B" }

// boundText prints a bound the way DynamoDB Local does in its messages: S and N as given, and B
// as the Java buffer that holds it.
func boundText(it *types.Item) string {
	switch {
	case it.S != nil:
		return "{S:" + *it.S + "}"
	case it.N != nil:
		return "{N:" + *it.N + "}"
	}

	return fmt.Sprintf("{B:java.nio.HeapByteBuffer[pos=0 lim=%d cap=%d]}", len(it.B), len(it.B))
}

// compareBounds compares two bounds of the same type: S and B by bytes, N by value through the
// order-preserving encoding of stored keys. ok is false when a number cannot be encoded.
func compareBounds(lo, hi *types.Item) (order int, ok bool) {
	switch {
	case lo.S != nil:
		return strings.Compare(*lo.S, *hi.S), true
	case lo.N != nil:
		a, errA := appendNumber(nil, "", *lo.N)
		b, errB := appendNumber(nil, "", *hi.N)

		return bytes.Compare(a, b), errA == nil && errB == nil
	}

	return bytes.Compare(lo.B, hi.B), true
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
	if err := t.ValidateSearchIndex(input.Index); err != nil {
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

	// DynamoDB reports Select after the key condition.
	if err := t.validateSelect(input); err != nil {
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

// emptyIndexKey reports the first index with a key attribute that is an empty value in item.
// name is "" when there is none. DynamoDB Local reports the local secondary indexes before
// the global ones. Within a kind the order is by name: Local's own order among several
// local indexes is unverified, and the table does not keep the declaration order.
func (t *Table) emptyIndexKey(item map[string]*types.Item) (name, attr, kind string) {
	names := t.sortedIndexNames()
	slices.SortStableFunc(names, func(a, b string) int {
		return cmp.Compare(indexKindRank(t.Indexes[a]), indexKindRank(t.Indexes[b]))
	})

	for _, name := range names {
		if attr, kind := t.Indexes[name].keySchema.emptyKeyAttr(item); attr != "" {
			return name, attr, kind
		}
	}

	return "", "", ""
}

// indexKindRank orders local indexes before global ones.
func indexKindRank(idx *index) int {
	if idx.typ == indexTypeLocal {
		return 0
	}

	return 1
}

// validateItemKeyValues rejects an item whose primary key or index key attributes hold an
// empty value.
func (t *Table) validateItemKeyValues(item map[string]*types.Item) error {
	if attr, kind := t.KeySchema.emptyKeyAttr(item); attr != "" {
		return emptyKeyError(kind, PrimaryIndexName, attr)
	}

	if name, attr, kind := t.emptyIndexKey(item); name != "" {
		return emptyKeyError(kind, name, attr)
	}

	return nil
}

// validateUpdatedIndexKeys rejects an updated item whose index key attributes hold an empty
// value, with the message DynamoDB gives for an update expression.
func (t *Table) validateUpdatedIndexKeys(item map[string]*types.Item) error {
	if name, attr, kind := t.emptyIndexKey(item); name != "" {
		return errors.New(types.WithDetail(emptyKeyValuePrefix+updateIndexKeyText+fmt.Sprintf(emptyKeyValueText, kind),
			"index", name, "attribute", attr))
	}

	return nil
}

// indexKeysOf computes the key item has in every secondary index, by index name ("" when it
// is not indexed there). It fails when an index rejects the item, before anything is written.
func (t *Table) indexKeysOf(item map[string]*types.Item) (map[string]string, error) {
	keys := make(map[string]string, len(t.Indexes))

	for _, name := range t.sortedIndexNames() {
		indexKey, err := t.Indexes[name].keySchema.GetKey(t.AttributesDef, item)
		if err != nil {
			return nil, err
		}

		keys[name] = indexKey
	}

	return keys, nil
}

// writeItem stores item under key and points every index at the key indexKeys (from
// indexKeysOf) gives it. It cannot fail.
func (t *Table) writeItem(key string, item map[string]*types.Item, indexKeys map[string]string) {
	t.setItem(key, item)

	for name, indexKey := range indexKeys {
		t.Indexes[name].setRef(key, indexKey)
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

// WriteRequest is one put or delete of a batch write. Exactly one of Put (the whole item) and
// Delete (the key) is set.
type WriteRequest struct {
	Put    map[string]*types.Item
	Delete map[string]*types.Item
}

// ValidateBatchWrite rejects a whole batch write when any of its requests is invalid, so a
// front end can apply none of them (D8). lookup finds a table by name.
//
// Which error wins when a batch has both a missing table and an invalid request is not stable
// in DynamoDB Local: probes got either error depending on the order of the tables on the wire
// and on the table's key schema. minidyn reports invalid requests first, and a missing table
// only when every request of the existing tables is valid. When two existing tables are
// invalid, Local reports the first one in the request; a Go map carries no order, so minidyn
// uses table name order, and the requests of a table in order. Both are minidyn's choices (D1
// level 3), pinned by unit tests.
func ValidateBatchWrite(lookup func(name string) (*Table, bool), requests map[string][]WriteRequest) error {
	var missing error

	for _, name := range slices.Sorted(maps.Keys(requests)) {
		table, ok := lookup(name)
		if !ok {
			if missing == nil {
				missing = types.NewError("ResourceNotFoundException", "Cannot do operations on a non-existent table", nil)
			}

			continue
		}

		for _, req := range requests[name] {
			var err error

			switch {
			case req.Put != nil:
				err = table.ValidatePutItem(req.Put)
			case req.Delete != nil:
				err = table.ValidateDeleteKey(req.Delete)
			}

			if err != nil {
				return err
			}
		}
	}

	return missing
}

// ValidatePutItem runs the checks Put makes on an item that do not depend on a condition, in
// the same order. It changes nothing, so a batch can reject an invalid request before it
// applies any (D8).
func (t *Table) ValidatePutItem(item map[string]*types.Item) error {
	if err := validateItemValues(item); err != nil {
		return err
	}

	if err := t.checkItemKeyValues(item); err != nil {
		return err
	}

	if _, err := t.primaryKeyOf(item); err != nil {
		return err
	}

	_, err := t.checkedIndexKeysOf(item)

	return err
}

// ValidateDeleteKey runs the checks Delete makes on a key that do not depend on a condition, in
// the same order. It changes nothing.
func (t *Table) ValidateDeleteKey(key map[string]*types.Item) error {
	_, err := t.checkedKeyOf(key)

	return err
}

// checkedKeyOf runs the checks a key gets before it is read or deleted and returns its storage
// key: its values, that it holds exactly the primary key attributes, and that none is missing.
func (t *Table) checkedKeyOf(key map[string]*types.Item) (string, error) {
	if err := validateItemValues(key); err != nil {
		return "", err
	}

	if err := t.checkPrimaryKeyShape(key); err != nil {
		return "", err
	}

	return t.primaryKeyOf(key)
}

// The helpers below are the steps Put, Delete and the Validate* functions have in common. Each
// returns a ValidationException, so a check added to one reaches every path that calls it.

// validateItemValues rejects an attribute value DynamoDB does not accept.
func validateItemValues(m map[string]*types.Item) error {
	if err := types.ValidateItemMap(m); err != nil {
		return types.NewError("ValidationException", err.Error(), nil)
	}

	return nil
}

// checkItemKeyValues rejects empty values in the primary key and index key attributes of item.
func (t *Table) checkItemKeyValues(item map[string]*types.Item) error {
	if err := t.validateItemKeyValues(item); err != nil {
		return types.NewError("ValidationException", err.Error(), nil)
	}

	return nil
}

// checkPrimaryKeyShape rejects a key that does not hold exactly the primary key attributes.
func (t *Table) checkPrimaryKeyShape(key map[string]*types.Item) error {
	if err := t.KeySchema.validatePrimaryKeyMap(key); err != nil {
		return types.NewError("ValidationException", err.Error(), nil)
	}

	return nil
}

// primaryKeyOf returns the storage key of an item, or a key, and rejects one that is missing a
// key attribute or has one of the wrong type.
func (t *Table) primaryKeyOf(item map[string]*types.Item) (string, error) {
	key, err := t.KeySchema.GetKey(t.AttributesDef, item)
	if err != nil {
		return "", types.NewError("ValidationException", err.Error(), nil)
	}

	return key, nil
}

// checkedIndexKeysOf is indexKeysOf with its error as a ValidationException.
func (t *Table) checkedIndexKeysOf(item map[string]*types.Item) (map[string]string, error) {
	keys, err := t.indexKeysOf(item)
	if err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	return keys, nil
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

	if err := validateItemValues(input.Item); err != nil {
		return nil, err
	}

	if err := validateItemValues(input.ExpressionAttributeValues); err != nil {
		return nil, err
	}

	// DynamoDB reports the key values of the item before the placeholders of the condition.
	if err := t.checkItemKeyValues(input.Item); err != nil {
		return nil, err
	}

	if err := validateConditionExpression(input.ExpressionAttributeNames, input.ExpressionAttributeValues, input.ConditionExpression); err != nil {
		return nil, err
	}

	item := copyItem(input.Item)

	key, err := t.primaryKeyOf(input.Item)
	if err != nil {
		return nil, err
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

	indexKeys, err := t.checkedIndexKeysOf(item)
	if err != nil {
		return nil, err
	}

	old := t.Data[key]

	t.writeItem(key, item, indexKeys)

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

// ValidateUpdateItemReturnValues accepts the ReturnValues modes UpdateItem supports.
func ValidateUpdateItemReturnValues(rv *string) error {
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

// writtenAttributes returns, for the top-level attributes of the written paths, the ones whose
// path resolves in item. Like DynamoDB it keeps the whole top-level attribute for nested paths.
func writtenAttributes(item map[string]*types.Item, paths [][]language.PathElement) map[string]*types.Item {
	out := map[string]*types.Item{}

	for _, path := range paths {
		if _, ok := language.LookupProjectedPath(item, path); ok {
			out[path[0].Key] = deepCopyTypesItem(item[path[0].Key])
		}
	}

	return out
}

// buildUpdateItemReturnAttributes builds the UpdateItem Attributes response. UPDATED_OLD and
// UPDATED_NEW return the attributes the update expression writes (paths), whether or not their
// value changed, as they were in oldItem or are in newItem.
func buildUpdateItemReturnAttributes(rv *string, paths [][]language.PathElement,
	oldItem, newItem map[string]*types.Item,
) map[string]*types.Item {
	mode := ""

	if rv != nil {
		mode = *rv
	}

	switch mode {
	case "ALL_NEW":
		return copyItem(newItem)
	case "ALL_OLD":
		return copyItem(oldItem)
	case "UPDATED_OLD":
		return writtenAttributes(oldItem, paths)
	case "UPDATED_NEW":
		return writtenAttributes(newItem, paths)
	default:
		return nil
	}
}

// Update updates an item in the table based on the input
func (t *Table) Update(input *types.UpdateItemInput) (map[string]*types.Item, error) { //nolint:gocognit,gocyclo // conditional writes, interpreter update, GSI updates
	if err := validateUpdateItemInputKeysAndValues(input); err != nil {
		return nil, err
	}

	// DynamoDB reports ReturnValues, then the Key, then the rest of the request.
	if err := ValidateUpdateItemReturnValues(input.ReturnValues); err != nil {
		return nil, err
	}

	if err := t.KeySchema.validatePrimaryKeyMap(input.Key); err != nil {
		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	if err := ValidateReturnValuesOnConditionCheckFailure(input.ReturnValuesOnConditionCheckFailure); err != nil {
		return nil, err
	}

	// It reports the update expression before the condition.
	if err := validateWriteExpressions(input.ExpressionAttributeNames, input.ExpressionAttributeValues,
		interpreter.LabeledExpression{Label: "UpdateExpression", Expression: input.UpdateExpression},
		interpreter.LabeledExpression{Label: "ConditionExpression", Expression: derefString(input.ConditionExpression)},
	); err != nil {
		return nil, err
	}

	// A parse error leaves no paths: the interpreter reports it later.
	writtenPaths := interpreter.UpdateTargetPaths(input.UpdateExpression,
		input.ExpressionAttributeNames, input.ExpressionAttributeValues)

	if !t.KeySchema.Secondary {
		if err := language.ValidatePathsDoNotTargetPrimaryKey(
			writtenPaths,
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

	// An item the update creates has no old item to return.
	oldItem := map[string]*types.Item{}
	if ok {
		oldItem = deepCopyItemMap(item)
	}

	err = t.interpreterUpdate(interpreter.UpdateInput{
		TableName:  t.Name,
		Expression: input.UpdateExpression,
		Item:       item,
		Attributes: input.ExpressionAttributeValues,
		Aliases:    input.ExpressionAttributeNames,
	})
	if err == nil {
		err = t.validateUpdatedIndexKeys(item)
	}

	// Every index key is computed before anything is written, so an index that rejects the
	// item leaves the table and the indexes as they were.
	var indexKeys map[string]string
	if err == nil {
		indexKeys, err = t.indexKeysOf(item)
	}

	if err != nil {
		if ok {
			// the update ran on the stored item: put it back as it was
			clear(item)
			maps.Copy(item, oldItem)
		}

		return nil, types.NewError("ValidationException", err.Error(), nil)
	}

	t.writeItem(key, item, indexKeys)

	newItem := copyItem(item)

	return buildUpdateItemReturnAttributes(input.ReturnValues, writtenPaths, oldItem, newItem), nil
}

// Delete deletes an item in the table based on the input
func (t *Table) Delete(input *types.DeleteItemInput) (map[string]*types.Item, error) { //nolint:gocognit,gocyclo // conditional delete, key resolution, index cleanup
	if err := ValidateNoneOrAllOldReturnValues(input.ReturnValues); err != nil {
		return nil, err
	}

	if err := ValidateReturnValuesOnConditionCheckFailure(input.ReturnValuesOnConditionCheckFailure); err != nil {
		return nil, err
	}

	if err := validateItemValues(input.Key); err != nil {
		return nil, err
	}

	if err := validateItemValues(input.ExpressionAttributeValues); err != nil {
		return nil, err
	}

	if err := t.checkPrimaryKeyShape(input.Key); err != nil {
		return nil, err
	}

	if err := validateConditionExpression(deleteAliases(input.ExpressionAttributeNames), input.ExpressionAttributeValues, input.ConditionExpression); err != nil {
		return nil, err
	}

	key, err := t.primaryKeyOf(input.Key)
	if err != nil {
		return nil, err
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
	return t.UpdateTable(name, nil, changes)
}

// applyIndexChanges applies changes that validateIndexChanges accepted, registering the indexes
// buildCreatedIndexes built for them.
func (t *Table) applyIndexChanges(name string, changes []*types.GlobalSecondaryIndexUpdate, built map[string]*index) *types.TableDescription {
	var (
		created []string
		dropped []types.GlobalSecondaryIndexDescription
	)

	for _, change := range changes {
		switch {
		case change.Create != nil:
			indexName := *change.Create.IndexName

			built[indexName].createdAt = time.Now()
			t.Indexes[indexName] = built[indexName]
			created = append(created, indexName)
		case change.Delete != nil:
			indexName := *change.Delete.IndexName

			d := t.globalIndexDescription(indexName, t.Indexes[indexName])
			d.IndexStatus = new("DELETING")
			dropped = append(dropped, d)

			delete(t.Indexes, indexName)
		}
	}

	desc := t.Description(name)
	for i := range desc.GlobalSecondaryIndexes {
		if slices.Contains(created, *desc.GlobalSecondaryIndexes[i].IndexName) {
			t.markIndexCreating(&desc.GlobalSecondaryIndexes[i])
		}
	}

	desc.GlobalSecondaryIndexes = append(desc.GlobalSecondaryIndexes, dropped...)

	return desc
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
