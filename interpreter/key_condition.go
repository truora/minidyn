package interpreter

import (
	"strings"

	"github.com/truora/minidyn/interpreter/language"
	"github.com/truora/minidyn/types"
)

// KeyConditionHashPlaceholder returns the expression attribute value placeholder (":v") that a
// key condition compares the partition key hashKey to, as in "pk = :v AND sk > :s". Aliases
// resolve "#name" references.
//
// It reports ok only when the equality is required for the whole condition (it is the
// condition, or a top-level AND operand), so callers may restrict a search to that partition.
// Any other shape, including invalid conditions, returns ok=false: callers then evaluate the
// condition on every item and report errors as before.
func KeyConditionHashPlaceholder(expr string, aliases map[string]string, hashKey string) (string, bool) {
	parsed := parseCondition(expr)
	if len(parsed.errs) != 0 || parsed.conditional == nil || parsed.conditional.Expression == nil {
		return "", false
	}

	return hashEquality(parsed.conditional.Expression, aliases, hashKey)
}

func hashEquality(e language.Expression, aliases map[string]string, hashKey string) (string, bool) {
	infix, ok := e.(*language.InfixExpression)
	if !ok {
		return "", false
	}

	switch strings.ToUpper(infix.Operator) {
	case "AND":
		if v, ok := hashEquality(infix.Left, aliases, hashKey); ok {
			return v, true
		}

		return hashEquality(infix.Right, aliases, hashKey)
	case "=":
		if attributeName(infix.Left, aliases) == hashKey {
			return placeholder(infix.Right)
		}

		if attributeName(infix.Right, aliases) == hashKey {
			return placeholder(infix.Left)
		}
	}

	return "", false
}

// attributeName returns the attribute a bare identifier or #alias refers to, or "".
func attributeName(e language.Expression, aliases map[string]string) string {
	id, ok := e.(*language.Identifier)
	if !ok || strings.HasPrefix(id.Value, ":") {
		return ""
	}

	if strings.HasPrefix(id.Value, "#") {
		return aliases[id.Value]
	}

	return id.Value
}

func placeholder(e language.Expression) (string, bool) {
	id, ok := e.(*language.Identifier)
	if !ok || !strings.HasPrefix(id.Value, ":") {
		return "", false
	}

	return id.Value, true
}

// KeyConditionError is a key condition DynamoDB rejects for the key schema it is queried with.
// Message is DynamoDB Local's text; Detail is minidyn-only debug detail.
type KeyConditionError struct {
	Message string
	Detail  []string
}

func (e *KeyConditionError) Error() string { return types.WithDetail(e.Message, e.Detail...) }

const (
	keyConditionMissedKeySchema = "Query condition missed key schema element"
	keyConditionNotSupported    = "Query key condition not supported"
	keyConditionOnePerKey       = "KeyConditionExpressions must only contain one condition per key"
	keyConditionTooLong         = "Conditions can be of length 1 or 2 only"
	keyConditionInvalidOperator = "Invalid operator used in KeyConditionExpression: "
)

// keyCondition is one top-level AND operand of a key condition.
type keyCondition struct {
	attribute string
	operator  string   // "=" for equality, anything else for a range comparison
	values    []string // value placeholders (":v") the attribute is compared to
}

// ValidateKeyCondition checks a Query key condition against the key schema of the table or
// index being queried: exactly one equality condition on hashKey, plus optionally one
// condition (=, <, <=, >, >=, BETWEEN, begins_with) on rangeKey ("" for none). Aliases
// resolve "#name" references. The first violation is returned as a *KeyConditionError;
// syntax errors return nil, as evaluating the condition reports them.
func ValidateKeyCondition(expr string, aliases map[string]string, hashKey, rangeKey string) error {
	parsed := parseCondition(expr)
	if len(parsed.errs) != 0 || parsed.conditional == nil || parsed.conditional.Expression == nil {
		return nil
	}

	conds, err := collectKeyConditions(parsed.conditional.Expression, aliases, nil)
	if err != nil {
		return err
	}

	seen := make(map[string]bool, len(conds))

	for _, c := range conds {
		if seen[c.attribute] {
			return &KeyConditionError{Message: keyConditionOnePerKey, Detail: []string{"attribute", c.attribute}}
		}

		seen[c.attribute] = true
	}

	if len(conds) > 2 {
		return &KeyConditionError{Message: keyConditionTooLong}
	}

	return checkKeySchema(conds, hashKey, rangeKey)
}

func checkKeySchema(conds []keyCondition, hashKey, rangeKey string) error {
	hashFound := false

	for _, c := range conds {
		if c.attribute != hashKey {
			continue
		}

		hashFound = true

		if c.operator != "=" {
			return &KeyConditionError{Message: keyConditionNotSupported, Detail: []string{"attribute", hashKey}}
		}
	}

	if !hashFound {
		return &KeyConditionError{Message: keyConditionMissedKeySchema, Detail: []string{"missing", hashKey}}
	}

	for _, c := range conds {
		if c.attribute != hashKey && c.attribute != rangeKey {
			return &KeyConditionError{Message: keyConditionMissedKeySchema, Detail: []string{"attribute", c.attribute, "schema", keySchemaDetail(hashKey, rangeKey)}}
		}
	}

	return nil
}

func keySchemaDetail(hashKey, rangeKey string) string {
	if rangeKey == "" {
		return hashKey
	}

	return hashKey + ", " + rangeKey
}

// collectKeyConditions flattens the top-level AND operands of e into conds, rejecting any
// operand that is not a key condition operator.
func collectKeyConditions(e language.Expression, aliases map[string]string, conds []keyCondition) ([]keyCondition, error) {
	if n, ok := e.(*language.InfixExpression); ok && strings.EqualFold(n.Operator, "AND") {
		left, err := collectKeyConditions(n.Left, aliases, conds)
		if err != nil {
			return nil, err
		}

		return collectKeyConditions(n.Right, aliases, left)
	}

	c, err := keyConditionOf(e, aliases)
	if err != nil {
		return nil, err
	}

	return append(conds, c), nil
}

// keyConditionOf analyzes one operand of a key condition's top-level AND.
func keyConditionOf(e language.Expression, aliases map[string]string) (keyCondition, error) {
	switch n := e.(type) {
	case *language.InfixExpression:
		switch op := strings.ToUpper(n.Operator); op {
		case "=", "<", "<=", ">", ">=":
			return comparisonCondition(op, n, aliases)
		}

		return keyCondition{}, invalidKeyOperator(n.Operator)
	case *language.BetweenExpression:
		return firstOperandCondition("BETWEEN", n.Left, n.Range[:], aliases)
	case *language.CallExpression:
		// "sk BETWEEN :a AND size(sk)" parses as a call of the BETWEEN expression.
		if _, ok := n.Function.(*language.Identifier); !ok {
			return keyCondition{}, &KeyConditionError{Message: keyConditionNested}
		}

		if strings.EqualFold(n.Function.String(), "begins_with") && len(n.Arguments) > 0 {
			return firstOperandCondition("begins_with", n.Arguments[0], n.Arguments[1:], aliases)
		}

		return keyCondition{}, invalidKeyOperator(n.Function.String())
	case *language.PrefixExpression:
		return keyCondition{}, invalidKeyOperator(n.Operator)
	case *language.InExpression:
		return keyCondition{}, invalidKeyOperator("IN")
	}

	return keyCondition{}, invalidKeyOperator(e.String())
}

func invalidKeyOperator(op string) error {
	return &KeyConditionError{Message: keyConditionInvalidOperator + strings.TrimSpace(op)}
}

// placeholders returns the value placeholders (":v") among the operands.
func placeholders(operands ...language.Expression) []string {
	var out []string

	for _, o := range operands {
		if ph, ok := placeholder(o); ok {
			out = append(out, ph)
		}
	}

	return out
}

// KeyConditionPlaceholders returns, by attribute, the value placeholders (":v") a key condition
// compares that attribute to, as in "pk = :p AND sk BETWEEN :a AND :b". Aliases resolve
// "#name" references. Invalid conditions return nil: ValidateKeyCondition reports them.
func KeyConditionPlaceholders(expr string, aliases map[string]string) map[string][]string {
	parsed := parseCondition(expr)
	if len(parsed.errs) != 0 || parsed.conditional == nil || parsed.conditional.Expression == nil {
		return nil
	}

	conds, err := collectKeyConditions(parsed.conditional.Expression, aliases, nil)
	if err != nil {
		return nil
	}

	out := make(map[string][]string, len(conds))
	for _, c := range conds {
		out[c.attribute] = append(out[c.attribute], c.values...)
	}

	return out
}

const (
	keyConditionNested      = "KeyConditionExpressions cannot contain nested operations"
	keyConditionNoAttribute = "Invalid condition in KeyConditionExpression: No key attribute specified"
	keyConditionManyNames   = "Invalid condition in KeyConditionExpression: Multiple attribute names used in one condition"
)

// comparisonCondition builds the condition of "a op b", where the attribute may be on either
// side: exactly one operand must be an attribute name and the other a value placeholder.
func comparisonCondition(op string, n *language.InfixExpression, aliases map[string]string) (keyCondition, error) {
	operands := []language.Expression{n.Left, n.Right}

	if err := rejectNested(operands); err != nil {
		return keyCondition{}, err
	}

	left, right := attributeName(n.Left, aliases), attributeName(n.Right, aliases)

	switch {
	case left == "" && right == "":
		return keyCondition{}, &KeyConditionError{Message: keyConditionNoAttribute}
	case left != "" && right != "":
		return keyCondition{}, &KeyConditionError{Message: keyConditionManyNames}
	case left == "":
		left = right
	}

	return keyCondition{attribute: left, operator: op, values: placeholders(operands...)}, nil
}

// firstOperandCondition builds the condition of a function or BETWEEN whose first operand
// is the attribute and whose remaining operands must be value placeholders.
func firstOperandCondition(op string, first language.Expression, rest []language.Expression, aliases map[string]string) (keyCondition, error) {
	if err := rejectNested(append([]language.Expression{first}, rest...)); err != nil {
		return keyCondition{}, err
	}

	attr := attributeName(first, aliases)
	if attr == "" {
		return keyCondition{}, &KeyConditionError{Message: "Invalid condition in KeyConditionExpression: " + op + " operator must have the key attribute as its first operand"}
	}

	for _, o := range rest {
		if attributeName(o, aliases) != "" {
			return keyCondition{}, &KeyConditionError{Message: keyConditionManyNames}
		}
	}

	return keyCondition{attribute: attr, operator: op, values: placeholders(rest...)}, nil
}

// rejectNested reports an operand that is neither an attribute name nor a value placeholder,
// such as size(sk).
func rejectNested(operands []language.Expression) error {
	for _, o := range operands {
		if _, ok := o.(*language.Identifier); !ok {
			return &KeyConditionError{Message: keyConditionNested}
		}
	}

	return nil
}
