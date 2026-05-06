package server

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/aws/smithy-go"
	"github.com/truora/minidyn/core"
	"github.com/truora/minidyn/interpreter"
	"github.com/truora/minidyn/types"
)

const (
	expressionAttributeNamesOnlyWithExpressionsMsg  = "ExpressionAttributeNames can only be specified when using expressions"
	expressionAttributeValuesOnlyWithExpressionsMsg = "ExpressionAttributeValues can only be specified when using expressions"
	unusedExpressionAttributeNamesMsg               = "Value provided in ExpressionAttributeNames unused in expressions"
	unusedExpressionAttributeValuesMsg              = "Value provided in ExpressionAttributeValues unused in expressions"
	invalidExpressionAttributeName                  = "ExpressionAttributeNames contains invalid key"
	invalidExpressionAttributeValue                 = "ExpressionAttributeValues contains invalid key"
)

var (
	expressionAttributeNamesRegex  = regexp.MustCompile("^#[A-Za-z0-9_]+$")
	expressionAttributeValuesRegex = regexp.MustCompile("^:[A-Za-z0-9_]+$")
)

// validateExpressionAttributes checks that every ExpressionAttributeNames / ExpressionAttributeValues
// key appears in the concatenated expression strings, and that placeholder keys match DynamoDB syntax.
// exprValueKeys are the map keys of ExpressionAttributeValues (e.g. ":x"); pass nil when unused.
func validateExpressionAttributes(exprNames map[string]string, exprValueKeys []string, genericExpressions ...string) error {
	genericExpression := strings.Join(genericExpressions, " ")
	genericExpression = strings.TrimSpace(genericExpression)

	if genericExpression == "" && len(exprNames) == 0 && len(exprValueKeys) == 0 {
		return nil
	}

	flattenNames := keysFromStringMap(exprNames)

	missingNames := getMissingSubstrs(genericExpression, flattenNames)
	missingValues := getMissingSubstrs(genericExpression, exprValueKeys)

	if len(missingNames) > 0 {
		sort.Strings(missingNames)

		msg := unusedExpressionAttributeNamesMsg
		if genericExpression == "" {
			msg = expressionAttributeNamesOnlyWithExpressionsMsg
		}

		return &smithy.GenericAPIError{Code: "ValidationException", Message: fmt.Sprintf("%s: keys: {%s}", msg, strings.Join(missingNames, ", "))}
	}

	err := validateSyntaxExpression(expressionAttributeNamesRegex, flattenNames, invalidExpressionAttributeName)
	if err != nil {
		return err
	}

	if len(missingValues) > 0 {
		if genericExpression == "" {
			return &smithy.GenericAPIError{Code: "ValidationException", Message: expressionAttributeValuesOnlyWithExpressionsMsg}
		}

		sort.Strings(missingValues)

		return &smithy.GenericAPIError{Code: "ValidationException", Message: fmt.Sprintf("%s: keys: {%s}", unusedExpressionAttributeValuesMsg, strings.Join(missingValues, ", "))}
	}

	err = validateSyntaxExpression(expressionAttributeValuesRegex, exprValueKeys, invalidExpressionAttributeValue)
	if err != nil {
		return err
	}

	return nil
}

func keysFromStringMap(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}

func keysFromAttributeValueMap(m map[string]*AttributeValue) []string {
	if len(m) == 0 {
		return nil
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}

func validateSyntaxExpression(regex *regexp.Regexp, expressions []string, errorMsg string) error {
	for _, exprName := range expressions {
		if !regex.MatchString(exprName) {
			return &smithy.GenericAPIError{Code: "ValidationException", Message: fmt.Sprintf("%s: Syntax error; key: %s", errorMsg, exprName)}
		}
	}

	return nil
}

func getMissingSubstrs(s string, substrs []string) []string {
	missingSubstrs := make([]string, 0, len(substrs))

	for _, substr := range substrs {
		if strings.Contains(s, substr) {
			continue
		}

		missingSubstrs = append(missingSubstrs, substr)
	}

	return missingSubstrs
}

func getItemAttributesForOutput(table *core.Table, stored map[string]*types.Item, projectionExpr string, aliases map[string]string) (map[string]*AttributeValue, error) {
	if projectionExpr == "" {
		return mapTypesMapToAttributeValue(stored), nil
	}

	projected, err := table.LangInterpreter.Project(interpreter.ProjectInput{
		Expression: projectionExpr,
		Item:       stored,
		Aliases:    aliases,
	})
	if err != nil {
		return nil, err
	}

	return mapTypesMapToAttributeValue(projected), nil
}
