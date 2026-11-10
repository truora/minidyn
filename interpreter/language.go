package interpreter

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/truora/minidyn/interpreter/language"
	"github.com/truora/minidyn/types"
)

// ProjectInput parameters for Project.
type ProjectInput struct {
	Expression string
	Item       map[string]*types.Item
	Aliases    map[string]string
}

// Language interpreter
type Language struct {
	Debug bool
}

// Match evalute the item with given expression and attributes
func (li *Language) Match(input MatchInput) (bool, error) {
	parsed := parseCondition(input.Expression)
	conditional := parsed.conditional

	if len(parsed.errs) != 0 {
		detail := strings.Join(parsed.errs, "; ")
		if input.ExpressionType == ExpressionTypeKey {
			return false, fmt.Errorf("Invalid KeyConditionExpression: %w; %s", ErrSyntaxError, detail) //nolint:staticcheck,ST1005 // DynamoDB ValidationException wording (parity)
		}

		return false, fmt.Errorf("%w: %s", ErrSyntaxError, strings.Join(parsed.errs, "\n"))
	}

	if conditional.Expression == nil {
		if input.ExpressionType == ExpressionTypeKey {
			return false, fmt.Errorf("Invalid KeyConditionExpression: %w; empty expression", ErrSyntaxError) //nolint:staticcheck,ST1005 // DynamoDB ValidationException wording (parity)
		}

		return false, fmt.Errorf("%w: empty expression", ErrSyntaxError)
	}

	if err := validateDeclaredNames(matchExpressionLabel(input.ExpressionType), parsed.names, input.Aliases); err != nil {
		return false, err
	}

	if err := validateDeclaredValues(matchExpressionLabel(input.ExpressionType), parsed.values, input.Attributes); err != nil {
		return false, err
	}

	if parsed.operandErr != nil {
		return false, &operandCountError{expressionType: matchExpressionLabel(input.ExpressionType), cause: parsed.operandErr}
	}

	env := language.NewEnvironment()

	aliases := map[string]string{}
	maps.Copy(aliases, input.Aliases)

	env.Aliases = aliases

	item := map[string]*types.Item{}

	maps.Copy(item, input.Item)

	err := env.AddAttributes(item)
	if err != nil {
		return false, fmt.Errorf("%w: %s", ErrUnsupportedFeature, err.Error())
	}

	err = env.AddAttributes(input.Attributes)
	if err != nil {
		return false, fmt.Errorf("%w: %s", ErrUnsupportedFeature, err.Error())
	}

	result := language.Eval(conditional, env)

	if li.Debug {
		fmt.Printf("evaluating: %q\nin: %s\n$>%s\n", conditional, env, result.Inspect())
	}

	if result.Type() == language.ObjectTypeError {
		if input.ExpressionType == ExpressionTypeKey {
			return false, fmt.Errorf("Invalid KeyConditionExpression: %w; %s", ErrSyntaxError, result.Inspect()) //nolint:staticcheck,ST1005 // DynamoDB ValidationException wording (parity)
		}

		return false, fmt.Errorf("%w: %s", ErrSyntaxError, result.Inspect())
	}

	return result == language.TRUE, nil
}

// matchExpressionLabel maps a MatchInput expression type to its DynamoDB expression label
// used in ValidationException messages.
func matchExpressionLabel(t ExpressionType) string {
	switch t {
	case ExpressionTypeKey:
		return "KeyConditionExpression"
	case ExpressionTypeFilter:
		return "FilterExpression"
	default:
		return "ConditionExpression"
	}
}

// CheckedProjection is a projection expression that passed CheckProjection, ready for
// ProjectChecked. It carries the parse and the aliases it was checked against, so the checks run
// once per request.
type CheckedProjection struct {
	exprs   []language.Expression
	aliases map[string]string
}

// CheckProjection returns the error DynamoDB reports for a projection expression: the syntax
// first, then a #name missing from aliases. It needs no item, so a request can reject a bad
// projection up front and project with the result afterwards.
func CheckProjection(expr string, aliases map[string]string) (*CheckedProjection, error) {
	pp := parseProjection(expr)

	if len(pp.errs) != 0 {
		return nil, fmt.Errorf("Invalid ProjectionExpression: %w; %s", ErrSyntaxError, strings.Join(pp.errs, "\n")) //nolint:stylecheck,staticcheck,ST1005 // consistent with AWS SDK errors
	}

	if err := ValidateExpressionAttributeNamesDeclared(aliases, LabeledExpression{"ProjectionExpression", expr}); err != nil {
		return nil, err
	}

	return &CheckedProjection{exprs: pp.exprs, aliases: aliases}, nil
}

// Project evaluates a projection expression and returns a new attribute map containing only the requested paths.
func (li *Language) Project(input ProjectInput) (map[string]*types.Item, error) {
	checked, err := CheckProjection(input.Expression, input.Aliases)
	if err != nil {
		return nil, err
	}

	return li.ProjectChecked(checked, input.Item)
}

// ProjectChecked projects item with a projection CheckProjection already accepted.
func (li *Language) ProjectChecked(checked *CheckedProjection, itemIn map[string]*types.Item) (map[string]*types.Item, error) {
	exprs := checked.exprs

	env := language.NewEnvironment()
	aliases := map[string]string{}
	maps.Copy(aliases, checked.aliases)
	env.Aliases = aliases

	item := map[string]*types.Item{}
	maps.Copy(item, itemIn)

	if err := env.AddAttributes(item); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedFeature, err.Error())
	}

	out := map[string]*types.Item{}

	for _, expr := range exprs {
		result := language.Eval(expr, env)
		if language.IsUndefinedObject(result) {
			continue
		}

		if result.Type() == language.ObjectTypeError {
			return nil, fmt.Errorf("%w: %s", ErrSyntaxError, result.Inspect())
		}

		path, err := language.ExtractPath(expr, env)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrSyntaxError, err.Error())
		}

		val := result.ToDynamoDB()
		language.SetProjectedPath(out, path, val)
	}

	return out, nil
}

func buildAliases(input UpdateInput) map[string]string {
	aliases := map[string]string{}
	maps.Copy(aliases, input.Aliases)

	return aliases
}

// UpdateTargetPaths returns the document path of every attribute an update expression writes,
// from the cached parse of expression. A blank or invalid expression has no paths: Update
// reports the error.
func UpdateTargetPaths(expression string, aliases map[string]string, values map[string]*types.Item) [][]language.PathElement {
	pu := parseUpdate(expression)
	if len(pu.errs) != 0 {
		return nil
	}

	return language.UpdateTargetPaths(pu.stmt, aliases, values)
}

// Update change the item with given expression and attributes
func (li *Language) Update(input UpdateInput) error {
	pu := parseUpdate(input.Expression)
	update := pu.stmt

	aliases := buildAliases(input)
	env := language.NewEnvironment()
	env.Aliases = aliases

	if len(pu.errs) != 0 {
		errType := ErrSyntaxError
		if pu.unsupported {
			errType = ErrUnsupportedFeature
		}

		return fmt.Errorf("%w: %s", errType, strings.Join(pu.errs, "\n"))
	}

	if err := ValidateExpressionAttributeNamesDeclared(aliases, LabeledExpression{"UpdateExpression", input.Expression}); err != nil {
		return err
	}

	if pu.operandErr != nil {
		return &operandCountError{expressionType: "UpdateExpression", cause: pu.operandErr}
	}

	item := map[string]*types.Item{}

	maps.Copy(item, input.Item)

	err := env.AddAttributes(item)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrUnsupportedFeature, err.Error())
	}

	attributes := map[string]bool{}
	for key := range input.Attributes {
		attributes[key] = true
	}

	err = env.AddAttributes(input.Attributes)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrUnsupportedFeature, err.Error())
	}

	if li.Debug {
		fmt.Printf("evaluating: %q\nin: %s\n", update, env)
	}

	result := language.EvalUpdate(update, env)

	if result.Type() == language.ObjectTypeError {
		if errObj, ok := result.(*language.Error); ok {
			return errors.New(errObj.Message)
		}

		return errors.New(result.Inspect())
	}

	env.Apply(input.Item, aliases, attributes)

	return nil
}
