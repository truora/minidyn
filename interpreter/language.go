package interpreter

import (
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
	l := language.NewLexer(input.Expression)
	p := language.NewParser(l)
	conditional := p.ParseConditionalExpression()

	if len(p.Errors()) != 0 {
		return false, fmt.Errorf("%w: %s", ErrSyntaxError, strings.Join(p.Errors(), "\n"))
	}

	if conditional.Expression == nil {
		return false, fmt.Errorf("%w: empty expression", ErrSyntaxError)
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
		return false, fmt.Errorf("%w: %s", ErrSyntaxError, result.Inspect())
	}

	return result == language.TRUE, nil
}

// Project evaluates a projection expression and returns a new attribute map containing only the requested paths.
func (li *Language) Project(input ProjectInput) (map[string]*types.Item, error) {
	l := language.NewLexer(input.Expression)
	p := language.NewParser(l)
	exprs := p.ParseProjectionExpression()

	if len(p.Errors()) != 0 {
		return nil, fmt.Errorf("%w: %s", ErrSyntaxError, strings.Join(p.Errors(), "\n"))
	}

	env := language.NewEnvironment()
	aliases := map[string]string{}
	maps.Copy(aliases, input.Aliases)
	env.Aliases = aliases

	item := map[string]*types.Item{}
	maps.Copy(item, input.Item)

	err := env.AddAttributes(item)
	if err != nil {
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

// Update change the item with given expression and attributes
func (li *Language) Update(input UpdateInput) error {
	l := language.NewLexer(input.Expression)
	p := language.NewUpdateParser(l)
	update := p.ParseUpdateExpression()

	aliases := buildAliases(input)
	env := language.NewEnvironment()
	env.Aliases = aliases

	if len(p.Errors()) != 0 {
		errType := ErrSyntaxError
		if p.IsUnsupportedExpression() {
			errType = ErrUnsupportedFeature
		}

		return fmt.Errorf("%w: %s", errType, strings.Join(p.Errors(), "\n"))
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
		return fmt.Errorf("%w: %s", ErrSyntaxError, result.Inspect())
	}

	env.Apply(input.Item, aliases, attributes)

	return nil
}
