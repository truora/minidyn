package interpreter

import (
	"strings"

	"github.com/truora/minidyn/interpreter/language"
)

// KeyConditionHashPlaceholder returns the expression attribute value placeholder (":v") that a
// key condition compares the partition key hashKey to, as in "pk = :v AND sk > :s". Aliases
// resolve "#name" references.
//
// It reports ok only when the equality is required for the whole condition (it is the
// condition, or a top-level AND operand), so callers may restrict a search to that partition.
// Any other shape, including invalid conditions, returns ok=false: callers then evaluate the
// condition on every item and report errors as before.
func KeyConditionHashPlaceholder(expr string, aliases map[string]string, hashKey string) (string, bool) {
	parsed := parseCondition(expr)
	if len(parsed.errs) != 0 || parsed.conditional == nil || parsed.conditional.Expression == nil {
		return "", false
	}

	return hashEquality(parsed.conditional.Expression, aliases, hashKey)
}

func hashEquality(e language.Expression, aliases map[string]string, hashKey string) (string, bool) {
	infix, ok := e.(*language.InfixExpression)
	if !ok {
		return "", false
	}

	switch strings.ToUpper(infix.Operator) {
	case "AND":
		if v, ok := hashEquality(infix.Left, aliases, hashKey); ok {
			return v, true
		}

		return hashEquality(infix.Right, aliases, hashKey)
	case "=":
		if attributeName(infix.Left, aliases) == hashKey {
			return placeholder(infix.Right)
		}

		if attributeName(infix.Right, aliases) == hashKey {
			return placeholder(infix.Left)
		}
	}

	return "", false
}

// attributeName returns the attribute a bare identifier or #alias refers to, or "".
func attributeName(e language.Expression, aliases map[string]string) string {
	id, ok := e.(*language.Identifier)
	if !ok || strings.HasPrefix(id.Value, ":") {
		return ""
	}

	if strings.HasPrefix(id.Value, "#") {
		return aliases[id.Value]
	}

	return id.Value
}

func placeholder(e language.Expression) (string, bool) {
	id, ok := e.(*language.Identifier)
	if !ok || !strings.HasPrefix(id.Value, ":") {
		return "", false
	}

	return id.Value, true
}
