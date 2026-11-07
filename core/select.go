package core

import (
	"fmt"

	"github.com/truora/minidyn/types"
)

// Select values DynamoDB accepts for Query and Scan. An empty value means the default.
const (
	selectAllAttributes          = "ALL_ATTRIBUTES"
	selectAllProjectedAttributes = "ALL_PROJECTED_ATTRIBUTES"
	selectCount                  = "COUNT"
	selectSpecificAttributes     = "SPECIFIC_ATTRIBUTES"
)

func selectValidationError(msg string) error {
	return types.NewError("ValidationException", msg, nil)
}

// validateSelect rejects the Select values and combinations DynamoDB rejects, with its messages.
func (t *Table) validateSelect(input QueryInput) error {
	hasProjection := input.ProjectionExpression != ""

	switch input.Select {
	case "":
		return nil
	case selectCount:
		if hasProjection {
			return selectValidationError("Cannot specify the ProjectionExpression when choosing to get only the Count")
		}
	case selectSpecificAttributes:
		if !hasProjection {
			return selectValidationError("Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES")
		}
	case selectAllAttributes, selectAllProjectedAttributes:
		return t.validateSelectAll(input)
	default:
		return selectValidationError("Select must be ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, COUNT, or SPECIFIC_ATTRIBUTES")
	}

	return nil
}

func (t *Table) validateSelectAll(input QueryInput) error {
	if input.ProjectionExpression != "" {
		return selectValidationError("Cannot specify the ProjectionExpression when choosing to get " + input.Select)
	}

	if input.Select == selectAllProjectedAttributes && input.Index == "" {
		return selectValidationError("ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName")
	}

	if input.Select != selectAllAttributes {
		return nil
	}

	idx, ok := t.Indexes[input.Index]
	if !ok || idx.typ != indexTypeGlobal || idx.projection == nil {
		return nil
	}

	if pt := idx.projection.ProjectionType; pt != nil && *pt != "" && *pt != "ALL" {
		return selectValidationError(fmt.Sprintf(
			"One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not supported for global secondary index %s because its projection type is not ALL",
			input.Index,
		))
	}

	return nil
}
