package interpreter

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/truora/minidyn/interpreter/language"
)

// Language interpreter
type Language struct {
	Debug bool
}

// Match evalute the item with given expression and attributes
func (li *Language) Match(input MatchInput) (bool, error) {
	l := language.NewLexer(input.Expression)
	p := language.NewParser(l)
	conditional := p.ParseConditionalExpression()

	if len(p.Errors()) != 0 {
		return false, fmt.Errorf("%w: %s", ErrSyntaxError, strings.Join(p.Errors(), "\n"))
	}

	env := language.NewEnvironment()

	aliases := map[string]string{}
	for k, v := range input.Aliases {
		aliases[k] = *v
	}

	env.Aliases = aliases

	item := map[string]*dynamodb.AttributeValue{}

	for field, val := range input.Item {
		item[field] = val
	}

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
		return false, fmt.Errorf("%w: %s", ErrSyntaxError, result.Inspect())
	}

	return result == language.TRUE, nil
}

func setAliases(input UpdateInput) map[string]string {
	aliases := map[string]string{}
	for k, v := range input.Aliases {
		aliases[k] = *v
	}

	return aliases
}

// Update change the item with given expression and attributes
func (li *Language) Update(input UpdateInput) error {
	l := language.NewLexer(input.Expression)
	p := language.NewUpdateParser(l)
	update := p.ParseUpdateExpression()

	aliases := setAliases(input)
	env := language.NewEnvironment()
	env.Aliases = aliases

	if len(p.Errors()) != 0 {
		errType := ErrSyntaxError
		if p.IsUnsupportedExpression() {
			errType = ErrUnsupportedFeature
		}

		return fmt.Errorf("%w: %s", errType, strings.Join(p.Errors(), "\n"))
	}

	item := map[string]*dynamodb.AttributeValue{}

	for field, val := range input.Item {
		item[field] = val
	}

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
		return fmt.Errorf("%w: %s", ErrSyntaxError, result.Inspect())
	}

	env.Apply(input.Item, aliases, attributes)

	return nil
}
