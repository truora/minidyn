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
	program := p.ParseDynamoExpression()
	env := language.NewEnvironment()

	if len(p.Errors()) != 0 {
		return false, fmt.Errorf("%w: %s", ErrSyntaxError, strings.Join(p.Errors(), "\n"))
	}

	item := map[string]*dynamodb.AttributeValue{}

	alises := map[string]string{}
	for k, v := range input.Aliases {
		alises[*v] = k
	}

	for field, val := range input.Item {
		if n, ok := alises[field]; ok {
			field = n
		}

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

	result := language.Eval(program, env)

	if li.Debug {
		fmt.Printf("evaluating: %q\nin: %s\n$>%s\n", program, env, result.Inspect())
	}

	if result.Type() == language.ObjectTypeError {
		return false, fmt.Errorf("%w: %s", ErrSyntaxError, result.Inspect())
	}

	return result == language.TRUE, nil
}

// Update change the item with given expression and attributes
func (li *Language) Update(input UpdateInput) error {
	return fmt.Errorf(
		"%w: language interpreter do not support updates",
		ErrUnsupportedFeature,
	)
}
