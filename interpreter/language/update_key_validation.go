package language

import (
	"fmt"
	"strings"
)

func resolveExpressionAttributeName(name string, aliases map[string]string) string {
	if aliases == nil {
		return name
	}

	if resolved, ok := aliases[name]; ok {
		return resolved
	}

	return name
}

// updateActionRootAttributeName returns the top-level DynamoDB attribute name for the LHS of
// SET, ADD, REMOVE, or DELETE (Identifier or IndexExpression chain).
func updateActionRootAttributeName(expr Expression, aliases map[string]string) (string, bool) {
	switch n := expr.(type) {
	case *Identifier:
		return resolveExpressionAttributeName(n.Value, aliases), true
	case *IndexExpression:
		return updateActionRootAttributeName(n.Left, aliases)
	default:
		return "", false
	}
}

func walkUpdateTargets(expr Expression, aliases map[string]string, visit func(root string)) {
	switch n := expr.(type) {
	case *UpdateExpression:
		for _, sub := range n.Expressions {
			walkUpdateTargets(sub, aliases, visit)
		}
	case *ActionExpression:
		if root, ok := updateActionRootAttributeName(n.Left, aliases); ok {
			visit(root)
		}
	}
}

// ValidateUpdateExpressionDoesNotTargetPrimaryKey parses the update expression and returns an
// error when any action targets the given hash or range key attribute name (after resolving
// ExpressionAttributeNames). If parsing fails, it returns nil so the interpreter can report syntax errors.
func ValidateUpdateExpressionDoesNotTargetPrimaryKey(expression string, aliases map[string]string, hashKey, rangeKey string) error {
	if strings.TrimSpace(expression) == "" {
		return nil
	}

	l := NewLexer(expression)
	p := NewUpdateParser(l)
	stmt := p.ParseUpdateExpression()

	if len(p.Errors()) != 0 || stmt.Expression == nil {
		return nil
	}

	var hit string

	walkUpdateTargets(stmt.Expression, aliases, func(root string) {
		if hit != "" {
			return
		}

		if root == hashKey || (rangeKey != "" && root == rangeKey) {
			hit = root
		}
	})

	if hit == "" {
		return nil
	}

	//nolint:staticcheck,ST1005 // DynamoDB ValidationException message parity
	return fmt.Errorf(
		"One or more parameter values were invalid: Cannot update attribute %s. This attribute is part of the key",
		hit,
	)
}
