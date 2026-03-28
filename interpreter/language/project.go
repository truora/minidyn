package language

import (
	"fmt"
	"strconv"

	"github.com/truora/minidyn/types"
)

// PathKind discriminates segments in a DynamoDB document path.
type PathKind int

const (
	// PathKindMapKey is a map attribute name (top-level or nested).
	PathKindMapKey PathKind = iota
	// PathKindListIndex is a list index (0-based).
	PathKindListIndex
)

// PathElement is one step in a projection path (map key or list index).
type PathElement struct {
	Kind  PathKind
	Key   string
	Index int64
}

func resolvePathName(name string, env *Environment) string {
	if env == nil {
		return name
	}

	if alias, ok := env.Aliases[name]; ok {
		return alias
	}

	return name
}

// ExtractPath walks a path expression AST and returns document path segments (root-first),
// resolving expression attribute name placeholders using env.Aliases.
func ExtractPath(n Expression, env *Environment) ([]PathElement, error) {
	switch node := n.(type) {
	case *Identifier:
		key := resolvePathName(node.Value, env)

		return []PathElement{{Kind: PathKindMapKey, Key: key}}, nil
	case *IndexExpression:
		return extractPathFromIndex(node, env)
	default:
		return nil, fmt.Errorf("projection path: unsupported expression %T", n)
	}
}

func extractPathFromIndex(node *IndexExpression, env *Environment) ([]PathElement, error) {
	prefix, err := ExtractPath(node.Left, env)
	if err != nil {
		return nil, err
	}

	switch node.Type {
	case ObjectTypeMap:
		return appendMapKeySegment(prefix, node.Index, env)
	case ObjectTypeList:
		return appendListIndexSegment(prefix, node.Index, env)
	default:
		return nil, fmt.Errorf("projection path: unsupported index type")
	}
}

func appendMapKeySegment(prefix []PathElement, indexExpr Expression, env *Environment) ([]PathElement, error) {
	keyIdent, ok := indexExpr.(*Identifier)
	if !ok {
		return nil, fmt.Errorf("projection path: map segment must be an identifier")
	}

	key := resolvePathName(keyIdent.Value, env)

	return append(prefix, PathElement{Kind: PathKindMapKey, Key: key}), nil
}

func appendListIndexSegment(prefix []PathElement, indexExpr Expression, env *Environment) ([]PathElement, error) {
	idx, err := extractPathListIndex(indexExpr, env)
	if err != nil {
		return nil, err
	}

	return append(prefix, PathElement{Kind: PathKindListIndex, Index: idx}), nil
}

func extractPathListIndex(idxExpr Expression, env *Environment) (int64, error) {
	ident, ok := idxExpr.(*Identifier)
	if !ok {
		return 0, fmt.Errorf("projection path: list index must be an identifier")
	}

	if n, err := strconv.Atoi(ident.Token.Literal); err == nil {
		return int64(n), nil
	}

	obj := Eval(ident, env)
	if isError(obj) {
		errObj, _ := obj.(*Error)

		return 0, fmt.Errorf("%s", errObj.Message)
	}

	number, ok := obj.(*Number)
	if !ok {
		return 0, fmt.Errorf("projection path: list index must be a number")
	}

	return int64(number.Value), nil
}

// SetProjectedPath writes val into target following path, creating nested M and L containers as needed.
// The first segment must be a map key (DynamoDB top-level attributes are maps).
func SetProjectedPath(target map[string]*types.Item, path []PathElement, val types.Item) {
	if len(path) == 0 {
		return
	}

	if path[0].Kind != PathKindMapKey {
		return
	}

	if len(path) == 1 {
		copyVal := val
		target[path[0].Key] = &copyVal

		return
	}

	key := path[0].Key

	node := target[key]
	if node == nil {
		node = &types.Item{}
		target[key] = node
	}

	setProjectedPathNested(node, path[1:], val)
}

func setProjectedPathNested(cur *types.Item, path []PathElement, val types.Item) {
	if len(path) == 0 {
		return
	}

	switch path[0].Kind {
	case PathKindMapKey:
		setNestedMapSegment(cur, path, val)
	case PathKindListIndex:
		setNestedListSegment(cur, path, val)
	}
}

func setNestedMapSegment(cur *types.Item, path []PathElement, val types.Item) {
	if len(path) == 1 {
		ensureItemMap(cur)
		copyVal := val
		cur.M[path[0].Key] = &copyVal

		return
	}

	ensureItemMap(cur)

	child := cur.M[path[0].Key]
	if child == nil {
		child = &types.Item{}
		cur.M[path[0].Key] = child
	}

	setProjectedPathNested(child, path[1:], val)
}

func setNestedListSegment(cur *types.Item, path []PathElement, val types.Item) {
	ensureItemList(cur)

	idx := int(path[0].Index)

	for len(cur.L) <= idx {
		cur.L = append(cur.L, nil)
	}

	if cur.L[idx] == nil {
		cur.L[idx] = &types.Item{}
	}

	if len(path) == 1 {
		copyVal := val
		cur.L[idx] = &copyVal

		return
	}

	setProjectedPathNested(cur.L[idx], path[1:], val)
}

func ensureItemMap(cur *types.Item) {
	if cur.M == nil {
		cur.M = map[string]*types.Item{}
	}
}

func ensureItemList(cur *types.Item) {
	if cur.L == nil {
		cur.L = []*types.Item{}
	}
}
