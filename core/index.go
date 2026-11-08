package core

import (
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

func (i *index) delete(key string, item map[string]*types.Item) error {
	delete(i.refs, key)
	i.ordered = nil

	indexKey, err := i.keySchema.GetKey(i.Table.AttributesDef, item)
	if err != nil || indexKey == "" {
		return err
	}

	pos := sort.SearchStrings(i.sortedKeys, indexKey)
	if pos == len(i.sortedKeys) {
		return err
	}

	copy(i.sortedKeys[pos:], i.sortedKeys[pos+1:])
	i.sortedKeys[len(i.sortedKeys)-1] = ""
	i.sortedKeys = i.sortedKeys[:len(i.sortedKeys)-1]

	return nil
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
