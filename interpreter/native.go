package interpreter

import (
	"fmt"
	"sort"
	"strings"

	"github.com/truora/minidyn/types"
)

// UpdaterFunc function used emule to UpdateItem expressions
type UpdaterFunc func(map[string]*types.Item, map[string]*types.Item)

// MatcherFunc function used to filter data
type MatcherFunc func(map[string]*types.Item, map[string]*types.Item) bool

// Native simple interpreter using pure go functions
type Native struct {
	filterExpressions    map[string]MatcherFunc
	keyExpressions       map[string]MatcherFunc
	writeCondExpressions map[string]MatcherFunc
	updateExpressions    map[string]UpdaterFunc
}

// NewNativeInterpreter returns a new native interpreter
func NewNativeInterpreter() *Native {
	return &Native{
		filterExpressions:    map[string]MatcherFunc{},
		keyExpressions:       map[string]MatcherFunc{},
		writeCondExpressions: map[string]MatcherFunc{},
		updateExpressions:    map[string]UpdaterFunc{},
	}
}

// Match evalute the item with given expression and attributes
func (ni *Native) Match(input MatchInput) (bool, error) {
	matcher, err := ni.getMatcher(input.TableName, input.Expression, input.ExpressionType)
	if err != nil {
		return false, err
	}

	return matcher(input.Item, input.Attributes), nil
}

// Update change the item with given expression and attributes
func (ni *Native) Update(input UpdateInput) error {
	updater, found := ni.updateExpressions[input.TableName+"|"+hashExpressionKey(input.Expression)]
	if !found {
		return fmt.Errorf(
			"%w: updater not found for %q expression in table %q",
			ErrUnsupportedFeature,
			input.Expression,
			input.TableName,
		)
	}

	updater(input.Item, input.Attributes)

	return nil
}

func (ni *Native) getMatcher(tablename, expression string, kind ExpressionType) (MatcherFunc, error) {
	var (
		matcher MatcherFunc
		found   bool
	)

	switch kind {
	case ExpressionTypeKey:
		matcher, found = ni.keyExpressions[tablename+"|"+hashExpressionKey(expression)]
	case ExpressionTypeFilter:
		matcher, found = ni.filterExpressions[tablename+"|"+hashExpressionKey(expression)]
	case ExpressionTypeConditional:
		matcher, found = ni.writeCondExpressions[tablename+"|"+hashExpressionKey(expression)]
	}

	if !found {
		return matcher, fmt.Errorf(
			"%w: matcher %q not found for %q expression in table %q",
			ErrUnsupportedFeature,
			string(kind),
			expression,
			tablename,
		)
	}

	return matcher, nil
}

func hashExpressionKey(s string) string {
	out := strings.Split(strings.TrimSpace(s), "")
	sort.Strings(out)

	return strings.Join(out, "")
}

// AddUpdater add expression updater to use on key or filter queries
func (ni *Native) AddUpdater(tablename string, expr string, updater UpdaterFunc) {
	ni.updateExpressions[tablename+"|"+hashExpressionKey(expr)] = updater
}

// AddMatcher add expression matcher to use on key or filter queries
func (ni *Native) AddMatcher(tablename string, t ExpressionType, expr string, matcher MatcherFunc) {
	// TODO validate the expresion(expr)
	key := hashExpressionKey(expr)

	switch t {
	case ExpressionTypeKey:
		ni.keyExpressions[tablename+"|"+key] = matcher
	case ExpressionTypeFilter:
		ni.filterExpressions[tablename+"|"+key] = matcher
	case ExpressionTypeConditional:
		ni.writeCondExpressions[tablename+"|"+key] = matcher
	default:
		panic("NativeInterpreter: unsupported expression type")
	}
}
