package interpreter

import (
	"sync"

	"github.com/truora/minidyn/interpreter/language"
	"github.com/truora/minidyn/types"
)

// maxParsedExpressions bounds the parsed-expression cache. Expressions in tests are usually a
// small fixed set; generated ones (e.g. with unique placeholders) just reset the cache.
const maxParsedExpressions = 1024

// parsedCondition is a condition expression parsed once and reused for every item a request
// evaluates. The AST is never modified by evaluation, so sharing it is safe.
type parsedCondition struct {
	conditional *language.ConditionalExpression
	errs        []string
	// names are the #names the expression uses, in order of first appearance, so alias
	// validation doesn't need to lex the expression again.
	names []string
	// values are the :values the expression uses, in order of first appearance.
	values []string
	// operandErr is the first built-in call with the wrong number of operands, if any.
	operandErr *language.OperandCountError
}

// conditionCache maps expression text to its parsed form. Parsing depends only on the text,
// so one process-wide cache serves every Language (which tables copy by value).
var conditionCache = struct {
	mu sync.Mutex
	m  map[string]*parsedCondition
}{m: map[string]*parsedCondition{}}

// parseCondition returns the parsed form of expr, parsing it on first use.
func parseCondition(expr string) *parsedCondition {
	conditionCache.mu.Lock()
	defer conditionCache.mu.Unlock()

	if pc, ok := conditionCache.m[expr]; ok {
		return pc
	}

	p := language.NewParser(language.NewLexer(expr))
	pc := &parsedCondition{
		conditional: p.ParseConditionalExpression(),
		errs:        p.Errors(),
		names:       language.UndeclaredExpressionAttributeNames(expr, nil),
		values:      language.UndeclaredExpressionAttributeValues(expr, func(string) bool { return false }),
	}

	if len(pc.errs) == 0 && pc.conditional != nil {
		pc.operandErr = language.FindOperandCountError(pc.conditional)
	}

	if len(conditionCache.m) >= maxParsedExpressions {
		clear(conditionCache.m)
	}

	conditionCache.m[expr] = pc

	return pc
}

// cachedConditions reports how many expressions are cached (for tests).
func cachedConditions() int {
	conditionCache.mu.Lock()
	defer conditionCache.mu.Unlock()

	return len(conditionCache.m)
}

// validateDeclaredNames is ValidateExpressionAttributeNamesDeclared for an already parsed
// expression: the first #name missing from aliases is reported.
func validateDeclaredNames(expressionType string, names []string, aliases map[string]string) error {
	for _, name := range names {
		if _, ok := aliases[name]; !ok {
			return &undeclaredAttributeNameError{expressionType: expressionType, name: name}
		}
	}

	return nil
}

// validateDeclaredValues is ValidateExpressionAttributeValuesDeclared for an already parsed
// expression: the first :value missing from values is reported.
func validateDeclaredValues(expressionType string, placeholders []string, values map[string]*types.Item) error {
	for _, name := range placeholders {
		if _, ok := values[name]; !ok {
			return &undeclaredAttributeValueError{expressionType: expressionType, value: name}
		}
	}

	return nil
}
