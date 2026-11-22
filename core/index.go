package core

import (
	"errors"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/truora/minidyn/types"
)

const (
	msgInvalidGSIName     = "Invalid global secondary index name"
	msgInvalidIndexName   = "Invalid table/index name.  Table/index names must be between 3 and 255 characters long, and may contain only the characters a-z, A-Z, 0-9, '_', '-', and '.'"
	msgNoProjection       = "Indexes must have a projection specified"
	msgNoKeySchema        = "No defined key schema.  A key schema containing at least a hash key must be defined for all tables"
	msgEmptyAttributeName = "Names of key attributes and attributes projected into an index must be between 1 and 255 characters, inclusive"
	msgNoKeyAttribute     = "Attribute name must be specified."
	msgNoAttributeType    = "Invalid Attribute value. Must contain exactly one of the supported datatypes."
	msgNullGSIAction      = "Update global secondary index action is null"
	msgOneUpdatePerName   = "Only one global secondary index update per index is allowed simultaneously."
	msgIndexExists        = "Attempting to create an index which already exists"
	msgNoSuchIndex        = "The table does not have the specified index: "
	msgGSIConsistentRead  = "Consistent reads are not supported on global secondary indexes"
	msgIndexLimit         = "Subscriber limit exceeded: Only 1 online index can be created or deleted simultaneously per table"
)

// validateIndexName checks the name of an index sent to CreateTable.
func validateIndexName(name *string) error {
	if types.StringValue(name) == "" {
		return validationError(msgInvalidIndexName)
	}

	return nil
}

// validateIndexShape checks what every index definition needs, in the order DynamoDB reports it:
// a projection, then a key schema whose elements name an attribute.
func validateIndexShape(indexName string, projection *types.Projection, keySchema []*types.KeySchemaElement) error {
	if projection == nil || types.StringValue(projection.ProjectionType) == "" {
		return validationError(msgNoProjection, "index", indexName)
	}

	if len(keySchema) == 0 {
		return validationError(msgNoKeySchema, "index", indexName)
	}

	for _, element := range keySchema {
		if element == nil || element.AttributeName == "" {
			return validationError(msgNoKeyAttribute, "index", indexName)
		}
	}

	return nil
}

type indexType string

const (
	indexTypeGlobal indexType = "global"
	indexTypeLocal  indexType = "local"
)

type index struct {
	keySchema  keySchema
	sortedKeys []string
	sortedRefs [][2]string // used for searching
	// ordered is every (primary key, index key) pair sorted by index key then primary key,
	// rebuilt lazily after a write (nil means stale) so queries can binary-search it.
	ordered    [][2]string
	typ        indexType
	projection *types.Projection
	Table      *Table
	refs       map[string]string
	createdAt  time.Time
	// provisionedThroughput is what the index was created with (nil for on-demand tables).
	provisionedThroughput *types.ProvisionedThroughput
}

func newIndex(t *Table, typ indexType, ks keySchema) *index {
	ks.Secondary = true

	return &index{
		keySchema:  ks,
		sortedKeys: []string{},
		typ:        typ,
		Table:      t,
		refs:       map[string]string{},
	}
}

func (i *index) Clear() {
	i.sortedKeys = []string{}
	i.refs = map[string]string{}
	i.ordered = nil
}

type indexSnapshot struct {
	sortedKeys []string
	refs       map[string]string
}

func (i *index) snapshot() indexSnapshot {
	keys := make([]string, len(i.sortedKeys))
	copy(keys, i.sortedKeys)

	refs := make(map[string]string, len(i.refs))
	maps.Copy(refs, i.refs)

	return indexSnapshot{sortedKeys: keys, refs: refs}
}

func (i *index) restore(s indexSnapshot) {
	i.sortedKeys = s.sortedKeys
	i.refs = s.refs
	i.ordered = nil
}

// ProjectItem returns the attributes visible through this index's projection (ALL, KEYS_ONLY, INCLUDE).
// For ALL or unknown types, it returns a full shallow copy of the item.
func (i *index) ProjectItem(item map[string]*types.Item) map[string]*types.Item {
	if i == nil || i.projection == nil {
		return copyItem(item)
	}

	pt := ""
	if i.projection.ProjectionType != nil {
		pt = *i.projection.ProjectionType
	}

	switch pt {
	case "", "ALL":
		return copyItem(item)
	case "KEYS_ONLY":
		return i.projectKeysOnly(item)
	case "INCLUDE":
		return i.projectInclude(item)
	default:
		return copyItem(item)
	}
}

// readItem returns what a Query or Scan reading this index sees of a stored item: its projection,
// except that an LSI reads the attributes it does not project from the table when the request
// asks for ALL_ATTRIBUTES or names attributes in a ProjectionExpression (#202). A GSI never does.
func (i *index) readItem(item map[string]*types.Item, input QueryInput) map[string]*types.Item {
	if i.typ == indexTypeLocal {
		switch {
		case input.Select == selectAllAttributes:
			return copyItem(item)
		case input.ProjectionExpression != "" && input.projection != nil:
			// The caller's ProjectChecked builds a new item and does not mutate this one.
			return item
		case input.ProjectionExpression != "":
			return copyItem(item)
		}
	}

	return i.ProjectItem(item)
}

// unprojected returns the attributes among names this index does not project, sorted. It asks
// ProjectItem what survives for an item holding every name, so it follows the projection types.
// Running a probe item through ProjectItem is deliberate: it reuses the one projection
// implementation instead of keeping a parallel attribute-set helper that could drift from it.
func (i *index) unprojected(names []string) []string {
	probe := make(map[string]*types.Item, len(names))
	for _, name := range names {
		probe[name] = &types.Item{}
	}

	projected := i.ProjectItem(probe)

	var missing []string

	for _, name := range names {
		if _, ok := projected[name]; !ok {
			missing = append(missing, name)
		}
	}

	sort.Strings(missing)

	return missing
}

func (i *index) projectKeysOnly(item map[string]*types.Item) map[string]*types.Item {
	out := make(map[string]*types.Item)
	maps.Copy(out, i.Table.KeySchema.getKeyItem(item))
	maps.Copy(out, i.keySchema.getKeyItem(item))

	return out
}

func (i *index) projectInclude(item map[string]*types.Item) map[string]*types.Item {
	out := i.projectKeysOnly(item)

	for _, namePtr := range i.projection.NonKeyAttributes {
		if namePtr == nil {
			continue
		}

		name := *namePtr
		if v, ok := item[name]; ok {
			out[name] = v
		}
	}

	return out
}

// putData indexes item under its index key, moving or dropping the entry an earlier
// version of the item had.
func (i *index) putData(key string, item map[string]*types.Item) error {
	indexKey, err := i.keySchema.GetKey(i.Table.AttributesDef, item)
	if err != nil {
		return err
	}

	i.setRef(key, indexKey)

	return nil
}

// backfill indexes every item of the table, as DynamoDB does when it creates an index on a
// table that already holds items. An item is left out when:
//   - it has no index key attribute (the index is sparse);
//   - its index key is an empty string or binary;
//   - its index key attribute has another data type than the attribute definition
//     (ErrInvalidAtrributeValue).
//
// The developer guide, under "Phases of index creation", says the violating keys are not present
// in the index and do not interfere with its creation. An N value DynamoDB cannot store is not
// among them: every write validates N values (#226), so no stored item holds one.
//
// Any other GetKey error is an invariant failure: for a secondary schema GetKey gives no other.
// It is returned, not skipped, so a bug cannot silently hide an item from the index. The caller
// runs backfill while it builds the index, before the table is touched, so UpdateTable rejects
// the request and changes nothing instead of failing half-applied.
func (i *index) backfill() error {
	for _, key := range i.Table.SortedKeys {
		item := i.Table.Data[key]

		if attr, _ := i.keySchema.emptyKeyAttr(item); attr != "" {
			continue
		}

		indexKey, err := i.keySchema.GetKey(i.Table.AttributesDef, item)
		if err != nil {
			if errors.Is(err, ErrInvalidAtrributeValue) {
				continue
			}

			return err
		}

		i.setRef(key, indexKey)
	}

	return nil
}

// delete drops the entry of the item with primary key key. The entry is found through refs, not
// by encoding the stored item again: an item that violates the index key schema is not indexed
// and has no key to encode (see backfill).
func (i *index) delete(key string) {
	i.setRef(key, "")
}

// setRef points the item with primary key key at indexKey, or drops it from the index
// when indexKey is "" (the item lacks the index key), keeping sortedKeys in step.
func (i *index) setRef(key, indexKey string) {
	old, exists := i.refs[key]
	if exists && old == indexKey {
		return
	}

	// An item that was not indexed and still has no index key changes nothing.
	if !exists && indexKey == "" {
		return
	}

	i.ordered = nil

	if exists {
		delete(i.refs, key)

		pos := sort.SearchStrings(i.sortedKeys, old)
		if pos < len(i.sortedKeys) && i.sortedKeys[pos] == old {
			i.sortedKeys = slices.Delete(i.sortedKeys, pos, pos+1)
		}
	}

	if indexKey == "" {
		return
	}

	i.refs[key] = indexKey
	pos := sort.SearchStrings(i.sortedKeys, indexKey)
	i.sortedKeys = slices.Insert(i.sortedKeys, pos, indexKey)
}

// orderedRefs returns every (primary key, index key) pair sorted by index key, then primary
// key, rebuilding the order only after the index changed.
func (i *index) orderedRefs() [][2]string {
	if i.ordered != nil {
		return i.ordered
	}

	ordered := make([][2]string, 0, len(i.refs))
	for k, v := range i.refs {
		ordered = append(ordered, [2]string{k, v})
	}

	sort.Slice(ordered, func(x, y int) bool {
		if ordered[x][1] != ordered[y][1] {
			return ordered[x][1] < ordered[y][1]
		}

		return ordered[x][0] < ordered[y][0]
	})

	i.ordered = ordered

	return ordered
}

// startSearch prepares sortedRefs, the (primary key, index key) pairs in search order. With
// onlyPrefix, only index keys starting with prefix are kept (see Table.fetchQueryData), which
// must match the keys the search walks.
func (i *index) startSearch(scanForward bool, prefix string, onlyPrefix bool) {
	refs := i.orderedRefs()

	if onlyPrefix {
		lo := sort.Search(len(refs), func(j int) bool { return refs[j][1] >= prefix })
		n := sort.Search(len(refs)-lo, func(j int) bool { return !strings.HasPrefix(refs[lo+j][1], prefix) })
		refs = refs[lo : lo+n]
	}

	// sortedRefs is consumed by getPrimaryKey, so it gets its own copy.
	i.sortedRefs = make([][2]string, len(refs))

	if scanForward {
		copy(i.sortedRefs, refs)

		return
	}

	for j, r := range refs {
		i.sortedRefs[len(refs)-1-j] = r
	}
}

// skipPast drops the entries of sortedRefs a search resumed after an ExclusiveStartKey does
// not visit, and returns how many there were. The start key is a primary key and an index
// key, and it need not belong to an item (#258).
//
// Entries are ordered by (index key, primary key) and the search resumes right after the
// start key in that order, going either way. This is a D1 level-3 choice: DynamoDB Local
// orders entries that share an index key by an internal rule that can't be matched, so
// minidyn seeks in its own tie order, the one its LastEvaluatedKey comes from. A start key
// inside a run of equal index keys then never repeats or loses an entry.
func (i *index) skipPast(startPK, startIK string, forward bool) int {
	size := len(i.sortedRefs)

	// sortedRefs is in search order: ascending when going forward.
	asc := func(j int) [2]string {
		if forward {
			return i.sortedRefs[j]
		}

		return i.sortedRefs[size-1-j]
	}

	pos := sort.Search(size, func(j int) bool {
		r := asc(j)

		return r[1] > startIK || (r[1] == startIK && r[0] >= startPK)
	})
	exact := pos < size && asc(pos) == [2]string{startPK, startIK}

	n := skipped(pos, exact, size, forward)
	i.sortedRefs = i.sortedRefs[n:]

	return n
}

// skipped returns how many entries a search skips to resume after a start key, of size
// entries in all. pos is where the start key sorts in ascending order and exact tells if an
// entry equals it. Going forward the walk skips what sorts before it and the entry itself;
// going backward, what sorts at or after it.
func skipped(pos int, exact bool, size int, forward bool) int {
	if !forward {
		return size - pos
	}

	if exact {
		return pos + 1
	}

	return pos
}

func (i *index) getPrimaryKey(indexKey string) (string, bool) {
	if len(i.sortedRefs) > 0 {
		key := i.sortedRefs[0]
		pk, ik := key[0], key[1]

		i.sortedRefs = i.sortedRefs[1:len(i.sortedRefs)]

		if indexKey == ik {
			return pk, true
		}
	}

	return "", false
}

func (i *index) count() int64 {
	return int64(len(i.sortedKeys))
}

// ValidateConsistentRead rejects ConsistentRead true on a GSI, which DynamoDB serves from an
// eventually consistent copy. The table and an LSI accept it, and an unknown index is
// ValidateSearchIndex's to report. The error comes right after the unknown index for a Scan,
// so the front ends call it ahead of their pre-check of the expression attributes; a Query
// reports it after the placeholders, where SearchData runs it.
func (t *Table) ValidateConsistentRead(name string, consistentRead bool) error {
	if _, ok := t.globalIndex(name); consistentRead && ok {
		return validationError(msgGSIConsistentRead, "index", name)
	}

	return nil
}
