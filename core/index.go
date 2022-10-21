package core

import (
	"sort"

	"github.com/ldelacruztruora/minidyn/types"
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
	projection *types.Projection // TODO use projection in queries
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
