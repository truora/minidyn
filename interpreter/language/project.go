package language

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

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

// RootAttributes returns the top-level attribute each document path in the expressions starts
// at, once each and in order of first appearance, with expression attribute names resolved
// through aliases. Value placeholders and function names are not attributes. It only reads the
// tree, so it works before any item is read.
func RootAttributes(aliases map[string]string, exprs ...Expression) []string {
	var roots []string

	seen := map[string]bool{}

	for _, e := range exprs {
		collectRoots(e, aliases, func(name string) {
			if !seen[name] {
				seen[name] = true
				roots = append(roots, name)
			}
		})
	}

	return roots
}

func collectRoots(e Expression, aliases map[string]string, visit func(string)) {
	switch node := e.(type) {
	case *Identifier:
		if !strings.HasPrefix(node.Value, ":") {
			visit(resolvePathName(node.Value, &Environment{Aliases: aliases}))
		}
	case *IndexExpression:
		// Only the root of a path is an attribute of the item: the index is a segment.
		collectRoots(node.Left, aliases, visit)
	case *CallExpression:
		for _, arg := range node.Arguments {
			collectRoots(arg, aliases, visit)
		}
	case *PrefixExpression:
		collectRoots(node.Right, aliases, visit)
	case *InfixExpression:
		collectRoots(node.Left, aliases, visit)
		collectRoots(node.Right, aliases, visit)
	case *BetweenExpression:
		collectRoots(node.Left, aliases, visit)
		collectRoots(node.Range[0], aliases, visit)
		collectRoots(node.Range[1], aliases, visit)
	case *InExpression:
		collectRoots(node.Left, aliases, visit)

		for _, r := range node.Range {
			collectRoots(r, aliases, visit)
		}
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

// LookupProjectedPath returns the value path resolves to in source, the read counterpart of
// SetProjectedPath. The first segment must be a map key.
func LookupProjectedPath(source map[string]*types.Item, path []PathElement) (*types.Item, bool) {
	if len(path) == 0 || path[0].Kind != PathKindMapKey {
		return nil, false
	}

	cur := source[path[0].Key]
	if cur == nil {
		return nil, false
	}

	for _, el := range path[1:] {
		switch {
		case el.Kind == PathKindMapKey && cur.M != nil:
			cur = cur.M[el.Key]
		case el.Kind == PathKindListIndex && el.Index >= 0 && el.Index < int64(len(cur.L)):
			cur = cur.L[el.Index]
		default:
			return nil, false
		}

		if cur == nil {
			return nil, false
		}
	}

	return cur, true
}

// SetProjectedPath writes val into target following path, creating nested M and L containers as needed.
// The first segment must be a map key (DynamoDB top-level attributes are maps).
//
// A list index past the end of a list pads it with nil holes for the elements the projection
// does not select. SetProjectedPath returns the list items it padded; pass them to
// CompactPaddedLists once every path is set.
func SetProjectedPath(target map[string]*types.Item, path []PathElement, val types.Item) []*types.Item {
	if len(path) == 0 {
		return nil
	}

	if path[0].Kind != PathKindMapKey {
		return nil
	}

	if len(path) == 1 {
		copyVal := val
		target[path[0].Key] = &copyVal

		return nil
	}

	key := path[0].Key

	node := target[key]
	if node == nil {
		node = &types.Item{}
		target[key] = node
	}

	return setProjectedPathNested(node, path[1:], val)
}

// CompactPaddedLists drops the nil holes of the lists SetProjectedPath padded, so a projection
// holds only the selected elements, in list order, as DynamoDB returns them. The projected tree
// is built from copies of the evaluated values, never from the stored item, so no list shares
// memory with it. A nil element is therefore always a hole SetProjectedPath padded; a selected
// NULL is a non-nil element. Compacting a list twice is harmless.
func CompactPaddedLists(lists []*types.Item) {
	for _, list := range lists {
		list.L = slices.DeleteFunc(list.L, func(el *types.Item) bool { return el == nil })
	}
}

// setProjectedPathNested writes the rest of a path, which always has at least one segment: the
// map and list setters stop at their last one.
func setProjectedPathNested(cur *types.Item, path []PathElement, val types.Item) []*types.Item {
	if path[0].Kind == PathKindListIndex {
		return setNestedListSegment(cur, path, val)
	}

	return setNestedMapSegment(cur, path, val)
}

func setNestedMapSegment(cur *types.Item, path []PathElement, val types.Item) []*types.Item {
	if len(path) == 1 {
		ensureItemMap(cur)
		copyVal := val
		cur.M[path[0].Key] = &copyVal

		return nil
	}

	ensureItemMap(cur)

	child := cur.M[path[0].Key]
	if child == nil {
		child = &types.Item{}
		cur.M[path[0].Key] = child
	}

	return setProjectedPathNested(child, path[1:], val)
}

func setNestedListSegment(cur *types.Item, path []PathElement, val types.Item) []*types.Item {
	ensureItemList(cur)

	idx := int(path[0].Index)

	var padded []*types.Item

	if idx > len(cur.L) {
		padded = append(padded, cur)
	}

	for len(cur.L) <= idx {
		cur.L = append(cur.L, nil)
	}

	if cur.L[idx] == nil {
		cur.L[idx] = &types.Item{}
	}

	if len(path) == 1 {
		copyVal := val
		cur.L[idx] = &copyVal

		return padded
	}

	return append(padded, setProjectedPathNested(cur.L[idx], path[1:], val)...)
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
