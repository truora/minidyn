package language

import (
	"fmt"

	"github.com/truora/minidyn/types"
)

// UpdateTargetPaths returns the document path of every action target (SET, REMOVE, ADD and
// DELETE) of a parsed update expression, in expression order, with expression attribute names
// resolved through aliases. values resolves value-based list indexes such as l[:i]. A target
// whose path can't be extracted still yields its longest extractable prefix (at least its root
// attribute), so one odd target never hides the others.
func UpdateTargetPaths(stmt *UpdateStatement, aliases map[string]string, values map[string]*types.Item) [][]PathElement {
	if stmt == nil || stmt.Expression == nil {
		return nil
	}

	env := NewEnvironment()
	env.Aliases = aliases
	valuesLoaded := false

	var paths [][]PathElement

	walkUpdateActions(stmt.Expression, func(action *ActionExpression) {
		path, err := ExtractPath(action.Left, env)
		if err != nil && !valuesLoaded {
			// Values are only needed by value-based indexes, so load them on first use.
			valuesLoaded = true

			if env.AddAttributes(values) == nil {
				path, err = ExtractPath(action.Left, env)
			}
		}

		if err != nil {
			path = extractablePrefix(action.Left, env)
		}

		if len(path) > 0 {
			paths = append(paths, path)
		}
	})

	return paths
}

// extractablePrefix returns the path of the innermost extractable part of expr, down to its root.
func extractablePrefix(expr Expression, env *Environment) []PathElement {
	for {
		if path, err := ExtractPath(expr, env); err == nil {
			return path
		}

		idx, ok := expr.(*IndexExpression)
		if !ok {
			return nil
		}

		expr = idx.Left
	}
}

func walkUpdateActions(expr Expression, visit func(*ActionExpression)) {
	switch n := expr.(type) {
	case *UpdateExpression:
		for _, sub := range n.Expressions {
			walkUpdateActions(sub, visit)
		}
	case *ActionExpression:
		visit(n)
	}
}

// ValidatePathsDoNotTargetPrimaryKey returns an error when any update target path starts at the
// given hash or range key attribute.
func ValidatePathsDoNotTargetPrimaryKey(paths [][]PathElement, hashKey, rangeKey string) error {
	for _, path := range paths {
		root := path[0].Key

		if root == hashKey || (rangeKey != "" && root == rangeKey) {
			//nolint:staticcheck,ST1005 // DynamoDB ValidationException message parity
			return fmt.Errorf(
				"One or more parameter values were invalid: Cannot update attribute %s. This attribute is part of the key",
				root,
			)
		}
	}

	return nil
}
