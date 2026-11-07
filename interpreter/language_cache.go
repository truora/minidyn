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
	// names are the #names the expression uses, in order of first appearance, shared with the
	// placeholder cache so alias validation doesn't need to lex the expression again.
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

// placeholderCache maps expression text to its #names and :values, scanned in one lexer pass.
// Placeholders don't depend on the expression kind, so the cache serves condition, filter, key
// condition, projection and update expressions alike.
var placeholderCache = struct {
	mu sync.Mutex
	m  map[string]scannedPlaceholders
}{m: map[string]scannedPlaceholders{}}

// scannedPlaceholders are the placeholders of one expression, in order of first appearance.
type scannedPlaceholders struct {
	names, values []string
}

// exprPlaceholders returns the #names and :values expr uses, lexing it only on first use. The
// returned slices are shared and must not be modified.
func exprPlaceholders(expr string) (names, values []string) {
	placeholderCache.mu.Lock()
	defer placeholderCache.mu.Unlock()

	if sp, ok := placeholderCache.m[expr]; ok {
		return sp.names, sp.values
	}

	sp := scannedPlaceholders{}
	sp.names, sp.values = language.Placeholders(expr)

	if len(placeholderCache.m) >= maxParsedExpressions {
		clear(placeholderCache.m)
	}

	placeholderCache.m[expr] = sp

	return sp.names, sp.values
}

// cachedPlaceholders reports how many expressions have cached placeholders (for tests).
func cachedPlaceholders() int {
	placeholderCache.mu.Lock()
	defer placeholderCache.mu.Unlock()

	return len(placeholderCache.m)
}

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
	}
	pc.names, pc.values = exprPlaceholders(expr)

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

// parsedUpdate is an update expression parsed once, like parsedCondition.
type parsedUpdate struct {
	stmt        *language.UpdateStatement
	errs        []string
	unsupported bool
	operandErr  *language.OperandCountError
}

// updateCache maps update expression text to its parsed form, bounded by maxParsedExpressions.
var updateCache = struct {
	mu sync.Mutex
	m  map[string]*parsedUpdate
}{m: map[string]*parsedUpdate{}}

// parseUpdate returns the parsed form of an update expression, parsing it on first use.
func parseUpdate(expr string) *parsedUpdate {
	updateCache.mu.Lock()
	defer updateCache.mu.Unlock()

	if pu, ok := updateCache.m[expr]; ok {
		return pu
	}

	p := language.NewUpdateParser(language.NewLexer(expr))
	pu := &parsedUpdate{stmt: p.ParseUpdateExpression(), errs: p.Errors(), unsupported: p.IsUnsupportedExpression()}

	if len(pu.errs) == 0 && pu.stmt != nil {
		pu.operandErr = language.FindOperandCountError(pu.stmt)
	}

	if len(updateCache.m) >= maxParsedExpressions {
		clear(updateCache.m)
	}

	updateCache.m[expr] = pu

	return pu
}

// cachedUpdates reports how many update expressions are cached (for tests).
func cachedUpdates() int {
	updateCache.mu.Lock()
	defer updateCache.mu.Unlock()

	return len(updateCache.m)
}

// validateDeclaredNames reports the first #name missing from aliases.
func validateDeclaredNames(expressionType string, names []string, aliases map[string]string) error {
	for _, name := range names {
		if _, ok := aliases[name]; !ok {
			return &undeclaredAttributeNameError{expressionType: expressionType, name: name}
		}
	}

	return nil
}

// validateDeclaredValues reports the first :value missing from values.
func validateDeclaredValues(expressionType string, placeholders []string, values map[string]*types.Item) error {
	for _, name := range placeholders {
		if _, ok := values[name]; !ok {
			return &undeclaredAttributeValueError{expressionType: expressionType, value: name}
		}
	}

	return nil
}
