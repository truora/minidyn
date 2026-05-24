package core

import (
	"maps"
	"sort"

	"github.com/truora/minidyn/types"
)

type indexType string

const (
	indexTypeGlobal indexType = "global"
	indexTypeLocal  indexType = "local"
)

type index struct {
	keySchema  keySchema
	sortedKeys []string
	sortedRefs [][2]string // used for searching
	typ        indexType
	projection *types.Projection
	Table      *Table
	refs       map[string]string
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

func (i *index) putData(key string, item map[string]*types.Item) error {
	indexKey, err := i.keySchema.GetKey(i.Table.AttributesDef, item)
	if err != nil || indexKey == "" {
		return err
	}

	_, exists := i.refs[key]

	i.refs[key] = indexKey

	if !exists {
		i.sortedKeys = append(i.sortedKeys, indexKey)
		sort.Strings(i.sortedKeys)
	}

	return nil
}

func (i *index) updateData(key string, item, oldItem map[string]*types.Item) error {
	indexKey, err := i.keySchema.GetKey(i.Table.AttributesDef, item)
	if err != nil || indexKey == "" {
		return err
	}

	old := i.refs[key]
	i.refs[key] = indexKey

	if old != indexKey {
		pos := sort.SearchStrings(i.sortedKeys, old)
		if pos >= len(i.sortedKeys) {
			i.sortedKeys = append(i.sortedKeys, indexKey)
		} else {
			i.sortedKeys[pos] = indexKey
		}

		sort.Strings(i.sortedKeys)
	}

	return nil
}

func (i *index) delete(key string, item map[string]*types.Item) error {
	delete(i.refs, key)

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

func (i *index) lessKey(x, y int) bool {
	if i.sortedRefs[x][1] < i.sortedRefs[y][1] {
		return true
	}

	if i.sortedRefs[x][1] > i.sortedRefs[y][1] {
		return false
	}

	return i.sortedRefs[x][0] < i.sortedRefs[y][0]
}

func (i *index) startSearch(scanForward bool) {
	i.sortedRefs = make([][2]string, len(i.refs))
	pos := 0

	for k, v := range i.refs {
		i.sortedRefs[pos] = [2]string{k, v}
		pos++
	}

	sort.Slice(i.sortedRefs, func(x, y int) bool {
		less := i.lessKey(x, y)
		if scanForward {
			return less
		}

		return !less
	})
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
