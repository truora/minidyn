package language

import (
	"fmt"
	"testing"
)

func TestIdentifierExpression(t *testing.T) {
	input := "foobar"
	l := NewLexer(input)
	p := NewParser(l)

	p.ParseConditionalExpression()

	errors := p.Errors()
	if len(errors) == 0 {
		t.Fatalf("condition expression with only an identifier must fail")
	}

	expect := "Syntax error; token: <EOF>, near: \"foobar\""
	if errors[0] != expect {
		t.Fatalf("expect error to be equal to %s got=%s", expect, errors[0])
	}
}

func TestParsingPrefixExpressions(t *testing.T) {
	prefixTests := []struct {
		input    string
		operator string
		value    any
	}{
		{"NOT :b", "NOT", ":b"},
	}

	for _, tt := range prefixTests {
		l := NewLexer(tt.input)
		p := NewParser(l)
		conditional := p.ParseConditionalExpression()
		checkParserErrors(t, p)

		exp, ok := conditional.Expression.(*PrefixExpression)
		if !ok {
			t.Fatalf("stmt is not PrefixExpression. got=%T", conditional.Expression)
		}

		if exp.Operator != tt.operator {
			t.Fatalf("exp.Operator is not '%s'. got=%s",
				tt.operator, exp.Operator)
		}

		if !testLiteralExpression(t, exp.Right, tt.value) {
			return
		}
	}
}

func TestParsingInfixExpressions(t *testing.T) {
	infixTests := []struct {
		input      string
		leftValue  any
		operator   string
		rightValue any
	}{
		{"#a = :a", "#a", "=", ":a"},
		{"#a <> :a", "#a", "<>", ":a"},
		{"#a > :a", "#a", ">", ":a"},
		{"#a <= :a", "#a", "<=", ":a"},
		{"#a AND :a", "#a", "AND", ":a"},
		{"#a OR :a", "#a", "OR", ":a"},
	}

	for _, tt := range infixTests {
		l := NewLexer(tt.input)
		p := NewParser(l)
		conditional := p.ParseConditionalExpression()
		checkParserErrors(t, p)

		if !testInfixExpression(t, conditional.Expression, tt.leftValue,
			tt.operator, tt.rightValue) {
			return
		}
	}
}

func TestParsingIndexExpressions(t *testing.T) {
	indexTests := []struct {
		input    string
		left     any
		indexVal any
	}{
		{"a[2]", "a", "2"},
		{"#a[1]", "#a", "1"},
		{"#a[1][2]", "(#a[1])", "2"},
		{"a.f", "a", "f"},
	}

	for _, tt := range indexTests {
		l := NewLexer(tt.input)
		p := NewParser(l)
		conditional := p.ParseConditionalExpression()
		checkParserErrors(t, p)

		opExp, ok := conditional.Expression.(*IndexExpression)
		if !ok {
			t.Fatalf("exp is not IndexExpression. got=%T(%s)", conditional.Expression, conditional.Expression)
		}

		testIndexExpression(t, opExp, tt.left, tt.indexVal)
	}
}

func testIndexExpression(t *testing.T, opExp *IndexExpression, left, index any) bool {
	if !testLiteralExpression(t, opExp.Left, left) {
		return true
	}

	if !testLiteralExpression(t, opExp.Index, index) {
		return true
	}

	return false
}

func TestParsingBetweenExpression(t *testing.T) {
	betweenTests := []struct {
		input     string
		leftValue any
		min       any
		max       any
	}{
		{"#b BETWEEN :a AND :c", "#b", ":a", ":c"},
	}

	for _, tt := range betweenTests {
		l := NewLexer(tt.input)
		p := NewParser(l)
		conditional := p.ParseConditionalExpression()
		checkParserErrors(t, p)

		opExp, ok := conditional.Expression.(*BetweenExpression)
		if !ok {
			t.Fatalf("exp is not BetweenExpression. got=%T(%s)", conditional.Expression, conditional.Expression)
		}

		testBetweenExpression(t, opExp, tt.leftValue, tt.min, tt.max)
	}
}

func testBetweenExpression(t *testing.T, opExp *BetweenExpression, left, min, max any) bool {
	if !testLiteralExpression(t, opExp.Left, left) {
		return true
	}

	if !testLiteralExpression(t, opExp.Range[0], min) {
		return true
	}

	if !testLiteralExpression(t, opExp.Range[1], max) {
		return true
	}

	return false
}

func TestOperatorPrecedenceParsing(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			":a > :b = #a < #b",
			"((:a > :b) = (#a < #b))",
		},
		{
			":a < :b <> #a > #b",
			"((:a < :b) <> (#a > #b))",
		},
		{
			"NOT(:a = #a)",
			"(NOT(:a = #a))",
		},
		{
			"a OR b AND c",
			"(a OR (b AND c))",
		},
		{
			":a AND :x < :y",
			"(:a AND (:x < :y))",
		},
		{
			"attribute_exists(:b) AND begins_with(:b, #s) OR #c",
			"((attribute_exists(:b) AND begins_with(:b, #s)) OR #c)",
		},
		{
			":a = size(:s)",
			"(:a = size(:s))",
		},
		{
			":a > size(:s)",
			"(:a > size(:s))",
		},
		{
			":a > size(:s) OR size(:c) = :a",
			"((:a > size(:s)) OR (size(:c) = :a))",
		},
		{
			"NOT :a > size(:s) OR size(:c) = :a",
			"((NOT(:a > size(:s))) OR (size(:c) = :a))",
		},
		{
			"a[0][0]",
			"((a[0])[0])",
		},
		{
			":a IN (:b, :c)",
			"(:a IN (:b, :c))",
		},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		p := NewParser(l)
		conditional := p.ParseConditionalExpression()
		checkParserErrors(t, p)

		actual := conditional.String()
		if actual != tt.expected {
			t.Errorf("expected=%q, got=%q", tt.expected, actual)
		}
	}
}

func TestOperatorPrecedenceUpdateParsing(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			"SET a = :x + :y",
			"(SET (a = (:x + :y)))",
		},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		p := NewUpdateParser(l)
		conditional := p.ParseConditionalExpression()
		checkParserErrors(t, p)

		actual := conditional.String()
		if actual != tt.expected {
			t.Errorf("expected=%q, got=%q", tt.expected, actual)
		}
	}
}

func TestCallExpressionParsing(t *testing.T) {
	input := "begins_with(:a, #s)"
	l := NewLexer(input)
	p := NewParser(l)
	conditional := p.ParseConditionalExpression()

	checkParserErrors(t, p)

	exp, ok := conditional.Expression.(*CallExpression)
	if !ok {
		t.Fatalf("stmt.Expression is not ast.CallExpression. got=%T",
			conditional.Expression)
	}

	if !testIdentifier(t, exp.Function, "begins_with") {
		return
	}

	if len(exp.Arguments) != 2 {
		t.Fatalf("wrong length of arguments. got=%d", len(exp.Arguments))
	}

	testLiteralExpression(t, exp.Arguments[0], ":a")
	testLiteralExpression(t, exp.Arguments[1], "#s")
}

func TestParsingErrors(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			"a != b",
			"no prefix parse function for ILLEGAL found",
		},
		{
			"=a",
			"no prefix parse function for = found",
		},
		{
			"size(a",
			"expected next token to be ), got EOF instead",
		},
		{
			"b BETWEEN a c",
			"expected next token to be AND, got IDENT instead",
		},
	}

	for _, tt := range tests {
		l := NewLexer(tt.input)
		p := NewParser(l)
		p.ParseConditionalExpression()

		if len(p.errors) == 0 {
			t.Errorf("no errors found")
			return
		}

		actual := p.errors[0]
		if actual != tt.expected {
			t.Errorf("expected=%q, got=%q", tt.expected, actual)
		}
	}
}

func testIdentifier(t *testing.T, exp Expression, value string) bool {
	ident, ok := exp.(*Identifier)
	if !ok {
		return false
	}

	if ident.Value != value {
		return false
	}

	if ident.TokenLiteral() != value {
		return false
	}

	return true
}

func checkParserErrors(t *testing.T, p *Parser) {
	if p.IsUnsupportedExpression() {
		t.Errorf("parser error: %q", "unsupported feature")
	}

	errors := p.Errors()
	if len(errors) == 0 {
		return
	}

	t.Errorf("parser has %d errors", len(errors))

	for _, msg := range errors {
		t.Errorf("parser error: %q", msg)
	}

	t.FailNow()
}

func testLiteralExpression(t *testing.T, exp Expression, expected any) bool {
	v, ok := expected.(string)
	if ok {
		return testIdentifier(t, exp, v)
	}

	t.Errorf("type of exp not handled. got=%T", exp)

	return false
}

func testInfixExpression(t *testing.T, exp Expression, left any, operator string, right any) bool {
	opExp, ok := exp.(*InfixExpression)
	if !ok {
		t.Errorf("exp is not OperatorExpression. got=%T(%s)", exp, exp)
		return false
	}

	if !testLiteralExpression(t, opExp.Left, left) {
		return false
	}

	if opExp.Operator != operator {
		t.Errorf("exp.Operator is not '%s'. got=%q", operator, opExp.Operator)
		return false
	}

	if !testLiteralExpression(t, opExp.Right, right) {
		return false
	}

	return true
}

func TestParsingSetExpression(t *testing.T) {
	setTests := []struct {
		input       string
		actionsSize int
	}{
		{"SET ProductCategory = :c", 1},
		{"SET ProductCategory = :c, Price = :p", 2},
		{"SET ProductCategory = :c + :one", 1},
	}

	for _, tt := range setTests {
		l := NewLexer(tt.input)
		p := NewUpdateParser(l)
		update := p.ParseUpdateExpression()
		checkParserErrors(t, p)

		opExp, ok := update.Expression.(*UpdateExpression)
		if !ok {
			t.Fatalf("exp is not UpdateExpression. got=%T(%s)", update.Expression, update.Expression)
		}

		if len(opExp.Expressions) != tt.actionsSize {
			t.Fatalf("unexpected actions size. got=%d expected=(%d)", len(opExp.Expressions), tt.actionsSize)
		}
	}
}

func TestParsingAddExpression(t *testing.T) {
	setTests := []struct {
		input       string
		actionsSize int
	}{
		{"ADD ProductTotal :c", 1},
		{"ADD ProductTotal :c, Price :p", 2},
	}

	for _, tt := range setTests {
		l := NewLexer(tt.input)
		p := NewUpdateParser(l)
		update := p.ParseUpdateExpression()
		checkParserErrors(t, p)

		opExp, ok := update.Expression.(*UpdateExpression)
		if !ok {
			t.Fatalf("exp is not UpdateExpression. got=%T(%s)", update.Expression, update.Expression)
		}

		if len(opExp.Expressions) != tt.actionsSize {
			t.Fatalf("unexpected actions size. got=%d expected=(%d)", len(opExp.Expressions), tt.actionsSize)
		}
	}
}

func TestParsingRemoveExpression(t *testing.T) {
	setTests := []struct {
		input       string
		actionsSize int
	}{
		{"REMOVE ProductTotal", 1},
		{"REMOVE ProductTotal, Price", 2},
	}

	for _, tt := range setTests {
		l := NewLexer(tt.input)
		p := NewUpdateParser(l)
		update := p.ParseUpdateExpression()
		checkParserErrors(t, p)

		opExp, ok := update.Expression.(*UpdateExpression)
		if !ok {
			t.Fatalf("exp is not UpdateExpression. got=%T(%s)", update.Expression, update.Expression)
		}

		if len(opExp.Expressions) != tt.actionsSize {
			t.Fatalf("unexpected actions size. got=%d expected=(%d)", len(opExp.Expressions), tt.actionsSize)
		}
	}
}

// DynamoDB's grammar needs at least one operand in a function call: a call with none is a
// syntax error, not an operand count error.
func TestParsingCallWithoutOperands(t *testing.T) {
	for _, input := range []string{"size() > :v", "attribute_exists()", "contains()"} {
		p := NewParser(NewLexer(input))
		p.ParseConditionalExpression()

		if len(p.Errors()) == 0 {
			t.Fatalf("expected parse errors for %q, got none", input)
		}
	}

	p := NewUpdateParser(NewLexer("SET a = if_not_exists()"))
	p.ParseUpdateExpression()

	if len(p.Errors()) == 0 {
		t.Fatal("expected parse errors for an update call without operands, got none")
	}
}

func TestParsingDuplicateUpdateClauseHeaders(t *testing.T) {
	t.Parallel()

	duplicateCases := []string{
		"SET a = :x SET b = :y",
		"ADD n :one ADD m :two",
		"REMOVE a REMOVE b",
		"DELETE s :x DELETE s :y",
		"SET a = :x REMOVE b SET c = :z",
	}

	for _, input := range duplicateCases {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			l := NewLexer(input)
			p := NewUpdateParser(l)
			p.ParseUpdateExpression()

			if len(p.Errors()) == 0 {
				t.Fatalf("expected parse errors for duplicate clause headers, got none (input=%q)", input)
			}
		})
	}
}

func TestParsingCommaSeparatedSameClauseStillValid(t *testing.T) {
	t.Parallel()

	l := NewLexer("SET a = :x, b = :y")
	p := NewUpdateParser(l)
	update := p.ParseUpdateExpression()
	checkParserErrors(t, p)

	opExp, ok := update.Expression.(*UpdateExpression)
	if !ok {
		t.Fatalf("exp is not UpdateExpression. got=%T(%s)", update.Expression, update.Expression)
	}

	if len(opExp.Expressions) != 2 {
		t.Fatalf("unexpected actions size. got=%d want=2", len(opExp.Expressions))
	}
}

func TestParsingTwoUpdateActions(t *testing.T) {
	setTests := []struct {
		input       string
		actionsSize int
	}{
		{"SET ProductCategory = :c REMOVE ProductSize", 2},
		{"SET ProductCategory = :c ADD ProductSize :one", 2},
		{"SET ProductCategory = :c, ProductDesc = :text ADD ProductSize :one", 3},
	}

	for _, tt := range setTests {
		l := NewLexer(tt.input)
		p := NewUpdateParser(l)
		update := p.ParseUpdateExpression()
		checkParserErrors(t, p)

		opExp, ok := update.Expression.(*UpdateExpression)
		if !ok {
			t.Fatalf("exp is not UpdateExpression. got=%T(%s)", update.Expression, update.Expression)
		}

		if len(opExp.Expressions) != tt.actionsSize {
			t.Fatalf("unexpected actions size. got=%d expected=(%d)", len(opExp.Expressions), tt.actionsSize)
		}
	}
}

func TestParsingUnsupportedExpressions(t *testing.T) {
	l := NewLexer("f")
	p := NewUpdateParser(l)
	p.ParseUpdateExpression()

	p.parseUnsupportedExpression()

	if !p.unsupported {
		t.Fatal("parse.unsupported must be true")
	}
}

func BenchmarkParser(b *testing.B) {
	for n := 0; n < b.N; n++ {
		l := NewLexer(`attribute_exists(:b) AND begins_with(:b, #s) OR #c`)
		p := NewParser(l)
		p.ParseConditionalExpression()

		if len(p.errors) != 0 {
			b.Fatal("errors found")
		}
	}
}

// A list index is a literal: DynamoDB rejects a placeholder or a name between the brackets as a
// syntax error that names the token (#164). Every parser reports it at parse time, as the first
// error, so the cached parse carries it and no expression has to be evaluated to find it.
func TestParsingRejectsNonLiteralListIndex(t *testing.T) {
	t.Parallel()

	parsers := map[string]func(string) []string{
		"condition": func(s string) []string {
			p := NewParser(NewLexer(s))
			p.ParseConditionalExpression()

			return p.Errors()
		},
		"update": func(s string) []string {
			p := NewUpdateParser(NewLexer(s))
			p.ParseUpdateExpression()

			return p.Errors()
		},
		"projection": func(s string) []string {
			p := NewParser(NewLexer(s))
			p.ParseProjectionExpression()

			return p.Errors()
		},
	}

	cases := []struct {
		kinds []string
		input string
		token string
	}{
		{[]string{"condition", "projection"}, "l[:i] = :v", ":i"},
		{[]string{"update"}, "SET l[:i] = :v", ":i"},
		{[]string{"update"}, "SET l[#n] = :v", "#n"},
		{[]string{"update"}, "REMOVE l[:i]", ":i"},
		{[]string{"update"}, "SET x = l[:i]", ":i"},
		{[]string{"update"}, "SET m.l[:i] = :v", ":i"},
		{[]string{"update"}, "SET l[:i].m = :v", ":i"},
		{[]string{"update"}, "SET l[0][:i] = :v", ":i"},
		{[]string{"condition", "projection"}, "l[abc]", "abc"},
		{[]string{"condition"}, "attribute_exists(m.l[:i])", ":i"},
	}

	for _, tt := range cases {
		for _, kind := range tt.kinds {
			errs := parsers[kind](tt.input)
			want := `Syntax error; token: "` + tt.token + `", near: "[` + tt.token + `]"`

			if len(errs) == 0 || errs[0] != want {
				t.Errorf("%s parser, %q: want first error %q, got %q", kind, tt.input, want, errs)
			}
		}
	}

	for _, input := range []string{"l[0] = :v", "l[12][3].m = :v", "l[9223372036854775808] = :v", "#a[1] = :v"} {
		if errs := parsers["condition"](input); len(errs) != 0 {
			t.Errorf("%q: unexpected errors %q", input, errs)
		}
	}
}

func TestLocatedSyntaxError(t *testing.T) {
	t.Parallel()

	detail, ok := LocatedSyntaxError(`Syntax error; token: ":i", near: "[:i]"`)
	if !ok || detail != `token: ":i", near: "[:i]"` {
		t.Errorf("got %q, %v", detail, ok)
	}

	for _, msg := range []string{"Syntax error; token: <EOF>, near: \"x\"", "no prefix parse function for = found", ""} {
		if detail, ok := LocatedSyntaxError(msg); ok {
			t.Errorf("%q: unexpectedly located: %q", msg, detail)
		}
	}
}

// DynamoDB Local takes a literal list index of 0 to 2147483647 and no leading zero. A leading
// zero is a syntax error found while parsing, with the offending token and its neighbours; a
// literal above the range is found by the same pass but reported after the whole text parsed,
// so a syntax error anywhere in the text comes first (#164). Probed on DynamoDB Local.
func TestParserListIndexLiteralRules(t *testing.T) {
	t.Parallel()

	parse := map[string]func(string) *Parser{
		"condition": func(s string) *Parser {
			p := NewParser(NewLexer(s))
			p.ParseConditionalExpression()

			return p
		},
		"update": func(s string) *Parser {
			p := NewUpdateParser(NewLexer(s))
			p.ParseUpdateExpression()

			return p
		},
		"projection": func(s string) *Parser {
			p := NewParser(NewLexer(s))
			p.ParseProjectionExpression()

			return p
		},
	}

	const rangeText = "List index is not within the allowable range; index: [%s]"

	cases := []struct {
		kind   string
		input  string
		syntax string // the first syntax error, "" when the text parses
		index  string // the literal reported as out of range, "" when none
	}{
		// Leading zeros: the token is the one after the first zero, near is that token with its
		// neighbours.
		{"projection", "l[01]", `Syntax error; token: "1", near: "01]"`, ""},
		{"projection", "l[012]", `Syntax error; token: "12", near: "012]"`, ""},
		{"projection", "l[0123]", `Syntax error; token: "123", near: "0123]"`, ""},
		{"projection", "l[05]", `Syntax error; token: "5", near: "05]"`, ""},
		{"projection", "l[00]", `Syntax error; token: "0", near: "00]"`, ""},
		{"projection", "l[007]", `Syntax error; token: "0", near: "007"`, ""},
		{"projection", "l[0007]", `Syntax error; token: "0", near: "000"`, ""},
		{"projection", "a, b.l[007], c", `Syntax error; token: "0", near: "007"`, ""},
		{"condition", "l[007] = :v", `Syntax error; token: "0", near: "007"`, ""},
		{"condition", "attribute_exists(l[0][007])", `Syntax error; token: "0", near: "007"`, ""},
		{"update", "SET l[007] = :v", `Syntax error; token: "0", near: "007"`, ""},
		{"update", "REMOVE l[01]", `Syntax error; token: "1", near: "01]"`, ""},
		{"update", "SET x = l[00000000002147483648]", `Syntax error; token: "0", near: "000"`, ""},
		// A zero alone is the first element, and the largest index is accepted.
		{"projection", "l[0]", "", ""},
		{"projection", "l[2147483647]", "", ""},
		{"condition", "l[10] = :v", "", ""},
		// Above the range: recorded, not a syntax error, whatever the size.
		{"projection", "l[2147483648]", "", "2147483648"},
		{"projection", "l[4294967296]", "", "4294967296"},
		{"projection", "l[9223372036854775807]", "", "9223372036854775807"},
		{"projection", "l[9223372036854775808]", "", "9223372036854775808"},
		{"projection", "l[99999999999999999999999999]", "", "99999999999999999999999999"},
		{"condition", "l[2147483648] = :v", "", "2147483648"},
		{"update", "SET l[2147483648] = :v", "", "2147483648"},
		{"update", "REMOVE m.l[0][2147483648].k", "", "2147483648"},
		{"update", "SET x = l[2147483648]", "", "2147483648"},
		// The first one in the text is the one reported.
		{"update", "SET l[2147483649] = :v, m[2147483648] = :v", "", "2147483649"},
		// A syntax error anywhere in the text is reported instead, before or after the literal.
		{"update", "SET l[2147483648] = :v, m[:i] = :v", `Syntax error; token: ":i", near: "[:i]"`, "2147483648"},
		{"update", "SET m[:i] = :v, l[2147483648] = :v", `Syntax error; token: ":i", near: "[:i]"`, "2147483648"},
		{"update", "SET l[007] = :v, m[2147483648] = :v", `Syntax error; token: "0", near: "007"`, "2147483648"},
		{"update", "SET m[2147483648] = :v, l[007] = :v", `Syntax error; token: "0", near: "007"`, "2147483648"},
		{"projection", "l[2147483648], l[007]", `Syntax error; token: "0", near: "007"`, "2147483648"},
	}

	for _, tt := range cases {
		p := parse[tt.kind](tt.input)

		if tt.syntax == "" && len(p.Errors()) != 0 {
			t.Errorf("%s %q: unexpected errors %q", tt.kind, tt.input, p.Errors())
		}

		if tt.syntax != "" && (len(p.Errors()) == 0 || p.Errors()[0] != tt.syntax) {
			t.Errorf("%s %q: want first error %q, got %q", tt.kind, tt.input, tt.syntax, p.Errors())
		}

		want := ""
		if tt.index != "" {
			want = fmt.Sprintf(rangeText, tt.index)
		}

		if got := p.IndexRangeError(); got != want {
			t.Errorf("%s %q: IndexRangeError() = %q, want %q", tt.kind, tt.input, got, want)
		}
	}
}
