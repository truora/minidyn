package partiql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/truora/minidyn/types"
)

var (
	// ErrInvalidStatement when the statement cannot be evaluated
	ErrInvalidStatement = errors.New("invalid PartiQL statement")
	// ErrUnsupportedOperation when an operation is not supported
	ErrUnsupportedOperation = errors.New("unsupported operation")
	// ErrParameterMismatch when parameters don't match
	ErrParameterMismatch = errors.New("parameter count mismatch")
)

// ExecutionResult represents the result of executing a PartiQL statement
type ExecutionResult struct {
	Items []map[string]*types.Item
	// For compatibility with DynamoDB operations
	Attributes      map[string]*types.Item // For single item operations
	LastEvaluatedKey map[string]*types.Item
	Count           int64
}

// Evaluator evaluates PartiQL statements
type Evaluator struct {
	parameters []interface{} // parameters passed with the statement
	paramIndex int           // current parameter index for positional params
	namedParams map[string]interface{} // named parameters
}

// NewEvaluator creates a new evaluator with parameters
func NewEvaluator(parameters []interface{}) *Evaluator {
	return &Evaluator{
		parameters:  parameters,
		paramIndex:  0,
		namedParams: make(map[string]interface{}),
	}
}

// TranslateSelectToQuery converts a SELECT statement to Query/Scan input
func (e *Evaluator) TranslateSelectToQuery(stmt *SelectStatement) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	result["TableName"] = stmt.TableName

	// Handle WHERE clause
	if stmt.Where != nil {
		// Attempt to determine if this is a Query (has key condition) or Scan
		keyCondition, filterExpression, err := e.analyzeWhereClause(stmt.Where)
		if err != nil {
			return nil, err
		}

		if keyCondition != "" {
			result["KeyConditionExpression"] = keyCondition
			result["IsQuery"] = true
		} else {
			result["IsQuery"] = false
		}

		if filterExpression != "" {
			result["FilterExpression"] = filterExpression
		}

		// Extract expression attribute values and names
		exprValues, exprNames := e.extractExpressionAttributes(stmt.Where)
		if len(exprValues) > 0 {
			result["ExpressionAttributeValues"] = exprValues
		}
		if len(exprNames) > 0 {
			result["ExpressionAttributeNames"] = exprNames
		}
	} else {
		result["IsQuery"] = false
	}

	// Handle LIMIT
	if stmt.Limit != nil {
		result["Limit"] = *stmt.Limit
	}

	// Handle projection
	if len(stmt.Projection) > 0 {
		if !e.isWildcardProjection(stmt.Projection) {
			projectionExpr := e.buildProjectionExpression(stmt.Projection)
			result["ProjectionExpression"] = projectionExpr
		}
	}

	return result, nil
}

// TranslateInsertToPutItem converts an INSERT statement to PutItem input
func (e *Evaluator) TranslateInsertToPutItem(stmt *InsertStatement) (map[string]*types.Item, error) {
	if stmt.Value == nil {
		return nil, fmt.Errorf("%w: missing VALUE clause", ErrInvalidStatement)
	}

	item, err := e.evaluateExpression(stmt.Value)
	if err != nil {
		return nil, err
	}

	mapItem, ok := item.(map[string]*types.Item)
	if !ok {
		return nil, fmt.Errorf("%w: VALUE must be a map/object", ErrInvalidStatement)
	}

	return mapItem, nil
}

// TranslateUpdateToUpdateItem converts an UPDATE statement to UpdateItem input
func (e *Evaluator) TranslateUpdateToUpdateItem(stmt *UpdateStatement) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	result["TableName"] = stmt.TableName

	// Build UPDATE expression
	updateExpr := e.buildUpdateExpression(stmt.SetClauses)
	result["UpdateExpression"] = updateExpr

	// Handle WHERE clause for key identification
	if stmt.Where != nil {
		keyCondition, _, err := e.analyzeWhereClause(stmt.Where)
		if err != nil {
			return nil, err
		}

		if keyCondition == "" {
			return nil, fmt.Errorf("%w: UPDATE requires key condition in WHERE clause", ErrInvalidStatement)
		}

		// Extract expression attribute values and names
		exprValues, exprNames := e.extractExpressionAttributes(stmt.Where)
		if len(exprValues) > 0 {
			result["ExpressionAttributeValues"] = exprValues
		}
		if len(exprNames) > 0 {
			result["ExpressionAttributeNames"] = exprNames
		}

		// Store key values for later extraction
		result["KeyConditionExpression"] = keyCondition
	} else {
		return nil, fmt.Errorf("%w: UPDATE requires WHERE clause", ErrInvalidStatement)
	}

	// Add SET clause values to expression attribute values
	setExprValues, setExprNames := e.extractSetExpressionAttributes(stmt.SetClauses)
	if existing, ok := result["ExpressionAttributeValues"]; ok {
		existingMap := existing.(map[string]*types.Item)
		for k, v := range setExprValues {
			existingMap[k] = v
		}
	} else if len(setExprValues) > 0 {
		result["ExpressionAttributeValues"] = setExprValues
	}

	if existing, ok := result["ExpressionAttributeNames"]; ok {
		existingMap := existing.(map[string]string)
		for k, v := range setExprNames {
			existingMap[k] = v
		}
	} else if len(setExprNames) > 0 {
		result["ExpressionAttributeNames"] = setExprNames
	}

	return result, nil
}

// TranslateDeleteToDeleteItem converts a DELETE statement to DeleteItem input
func (e *Evaluator) TranslateDeleteToDeleteItem(stmt *DeleteStatement) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	result["TableName"] = stmt.TableName

	// Handle WHERE clause for key identification
	if stmt.Where != nil {
		keyCondition, _, err := e.analyzeWhereClause(stmt.Where)
		if err != nil {
			return nil, err
		}

		if keyCondition == "" {
			return nil, fmt.Errorf("%w: DELETE requires key condition in WHERE clause", ErrInvalidStatement)
		}

		// Extract expression attribute values and names
		exprValues, exprNames := e.extractExpressionAttributes(stmt.Where)
		if len(exprValues) > 0 {
			result["ExpressionAttributeValues"] = exprValues
		}
		if len(exprNames) > 0 {
			result["ExpressionAttributeNames"] = exprNames
		}

		result["KeyConditionExpression"] = keyCondition
	} else {
		return nil, fmt.Errorf("%w: DELETE requires WHERE clause", ErrInvalidStatement)
	}

	return result, nil
}

// Helper functions

func (e *Evaluator) analyzeWhereClause(where Expression) (keyCondition string, filterExpression string, err error) {
	// For simplicity, we'll convert the WHERE clause to a DynamoDB expression
	// In a real implementation, we'd analyze the expression tree to determine
	// which parts are key conditions and which are filters
	expr, err := e.expressionToString(where)
	if err != nil {
		return "", "", err
	}

	// Simple heuristic: if it's a simple equality on 'id' or contains '=', treat as key condition
	// Otherwise, treat as filter
	if strings.Contains(expr, "=") && !strings.Contains(expr, "AND") {
		return expr, "", nil
	}

	// For now, treat everything as filter expression
	return "", expr, nil
}

func (e *Evaluator) expressionToString(expr Expression) (string, error) {
	switch exp := expr.(type) {
	case *InfixExpression:
		left, err := e.expressionToString(exp.Left)
		if err != nil {
			return "", err
		}
		right, err := e.expressionToString(exp.Right)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s %s %s", left, exp.Operator, right), nil

	case *PrefixExpression:
		right, err := e.expressionToString(exp.Right)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s %s", exp.Operator, right), nil

	case *Identifier:
		return exp.Value, nil

	case *StringLiteral:
		return fmt.Sprintf("'%s'", exp.Value), nil

	case *NumberLiteral:
		return exp.Value, nil

	case *BooleanLiteral:
		if exp.Value {
			return "true", nil
		}
		return "false", nil

	case *NullLiteral:
		return "NULL", nil

	case *ParameterExpression:
		// Replace with actual parameter value
		return e.getParameterValue(exp)

	case *BetweenExpression:
		val, err := e.expressionToString(exp.Value)
		if err != nil {
			return "", err
		}
		lower, err := e.expressionToString(exp.Lower)
		if err != nil {
			return "", err
		}
		upper, err := e.expressionToString(exp.Upper)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s BETWEEN %s AND %s", val, lower, upper), nil

	case *InExpression:
		val, err := e.expressionToString(exp.Value)
		if err != nil {
			return "", err
		}
		values := []string{}
		for _, v := range exp.Values {
			valStr, err := e.expressionToString(v)
			if err != nil {
				return "", err
			}
			values = append(values, valStr)
		}
		return fmt.Sprintf("%s IN (%s)", val, strings.Join(values, ", ")), nil

	case *AttributePath:
		base, err := e.expressionToString(exp.Base)
		if err != nil {
			return "", err
		}
		for _, elem := range exp.Path {
			if elem.Type == "field" {
				field, err := e.expressionToString(elem.Value)
				if err != nil {
					return "", err
				}
				base = fmt.Sprintf("%s.%s", base, field)
			} else {
				index, err := e.expressionToString(elem.Value)
				if err != nil {
					return "", err
				}
				base = fmt.Sprintf("%s[%s]", base, index)
			}
		}
		return base, nil

	case *FunctionCall:
		args := []string{}
		for _, arg := range exp.Arguments {
			argStr, err := e.expressionToString(arg)
			if err != nil {
				return "", err
			}
			args = append(args, argStr)
		}
		return fmt.Sprintf("%s(%s)", exp.Function, strings.Join(args, ", ")), nil

	default:
		return "", fmt.Errorf("%w: unknown expression type", ErrUnsupportedOperation)
	}
}

func (e *Evaluator) getParameterValue(param *ParameterExpression) (string, error) {
	if param.Name == "?" {
		// Positional parameter
		if e.paramIndex >= len(e.parameters) {
			return "", ErrParameterMismatch
		}
		val := e.parameters[e.paramIndex]
		e.paramIndex++
		return fmt.Sprintf("%v", val), nil
	}

	// Named parameter
	if val, ok := e.namedParams[param.Name]; ok {
		return fmt.Sprintf("%v", val), nil
	}

	return "", fmt.Errorf("parameter %s not found", param.Name)
}

func (e *Evaluator) evaluateExpression(expr Expression) (interface{}, error) {
	switch exp := expr.(type) {
	case *MapLiteral:
		result := make(map[string]*types.Item)
		for k, v := range exp.Pairs {
			keyStr := ""
			switch key := k.(type) {
			case *StringLiteral:
				keyStr = key.Value
			case *Identifier:
				keyStr = key.Value
			default:
				return nil, fmt.Errorf("map keys must be strings")
			}

			val, err := e.evaluateExpression(v)
			if err != nil {
				return nil, err
			}

			item, err := e.convertToItem(val)
			if err != nil {
				return nil, err
			}

			result[keyStr] = item
		}
		return result, nil

	case *StringLiteral:
		return exp.Value, nil

	case *NumberLiteral:
		return exp.Value, nil

	case *BooleanLiteral:
		return exp.Value, nil

	case *NullLiteral:
		return nil, nil

	case *ListLiteral:
		result := []interface{}{}
		for _, elem := range exp.Elements {
			val, err := e.evaluateExpression(elem)
			if err != nil {
				return nil, err
			}
			result = append(result, val)
		}
		return result, nil

	default:
		return nil, fmt.Errorf("%w: cannot evaluate expression type", ErrUnsupportedOperation)
	}
}

func (e *Evaluator) convertToItem(val interface{}) (*types.Item, error) {
	switch v := val.(type) {
	case string:
		return &types.Item{S: &v}, nil
	case int, int64:
		numStr := fmt.Sprintf("%d", v)
		return &types.Item{N: &numStr}, nil
	case float64:
		numStr := fmt.Sprintf("%f", v)
		return &types.Item{N: &numStr}, nil
	case bool:
		return &types.Item{BOOL: &v}, nil
	case nil:
		null := true
		return &types.Item{NULL: &null}, nil
	case map[string]*types.Item:
		return &types.Item{M: v}, nil
	case []interface{}:
		list := make([]*types.Item, len(v))
		for i, elem := range v {
			item, err := e.convertToItem(elem)
			if err != nil {
				return nil, err
			}
			list[i] = item
		}
		return &types.Item{L: list}, nil
	default:
		// Try to parse as number string
		if str, ok := val.(string); ok {
			if _, err := strconv.ParseFloat(str, 64); err == nil {
				return &types.Item{N: &str}, nil
			}
			return &types.Item{S: &str}, nil
		}
		return nil, fmt.Errorf("unsupported value type: %T", val)
	}
}

func (e *Evaluator) extractExpressionAttributes(expr Expression) (map[string]*types.Item, map[string]string) {
	// For simplicity, return empty maps
	// In a full implementation, we'd walk the expression tree and extract
	// attribute values and names that need to be parameterized
	return make(map[string]*types.Item), make(map[string]string)
}

func (e *Evaluator) extractSetExpressionAttributes(setClauses []SetClause) (map[string]*types.Item, map[string]string) {
	// For simplicity, return empty maps
	return make(map[string]*types.Item), make(map[string]string)
}

func (e *Evaluator) isWildcardProjection(projection []Expression) bool {
	if len(projection) == 1 {
		if ident, ok := projection[0].(*Identifier); ok {
			return ident.Value == "*"
		}
	}
	return false
}

func (e *Evaluator) buildProjectionExpression(projection []Expression) string {
	parts := []string{}
	for _, expr := range projection {
		if ident, ok := expr.(*Identifier); ok {
			parts = append(parts, ident.Value)
		}
	}
	return strings.Join(parts, ", ")
}

func (e *Evaluator) buildUpdateExpression(setClauses []SetClause) string {
	parts := []string{}
	for _, clause := range setClauses {
		if attr, ok := clause.Attribute.(*Identifier); ok {
			if val, ok := clause.Value.(*StringLiteral); ok {
				parts = append(parts, fmt.Sprintf("%s = '%s'", attr.Value, val.Value))
			} else if val, ok := clause.Value.(*NumberLiteral); ok {
				parts = append(parts, fmt.Sprintf("%s = %s", attr.Value, val.Value))
			} else if param, ok := clause.Value.(*ParameterExpression); ok {
				parts = append(parts, fmt.Sprintf("%s = %s", attr.Value, param.TokenLiteral()))
			}
		}
	}
	return "SET " + strings.Join(parts, ", ")
}
