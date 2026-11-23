package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Parity for a list index that is not a literal (#164, #267). DynamoDB Local takes only digits
// between the brackets: a value placeholder, a name placeholder or a plain name is a syntax error
// that names the token ("Invalid UpdateExpression: Syntax error; token: \":i\", near:
// \"[:i]\""), in every kind of expression, nested or mixed with a literal index. Local reports it
// while parsing, before it compares the placeholders with the declared ones, so an undefined
// value, an unused value, an unused name and an undeclared name do not change the answer. The
// harness compares the text up to "Syntax error" (#188); the unit tests pin the whole message.

type listIndexReq struct {
	// kind is one of update, put, delete, scan, query, keycond, get, batchget, transact, transactcheck.
	kind   string
	expr   string
	cond   string
	names  map[string]string
	values map[string]dynamodbtypes.AttributeValue
}

func TestE2E_ListIndexSyntax(t *testing.T) {
	str := fzS("x")
	one := fzN("1")
	vals := func(kv ...any) map[string]dynamodbtypes.AttributeValue {
		m := map[string]dynamodbtypes.AttributeValue{}

		for i := 0; i < len(kv); i += 2 {
			m[kv[i].(string)] = kv[i+1].(dynamodbtypes.AttributeValue)
		}

		return m
	}

	i, v := vals(":i", one, ":v", str), vals(":v", str)
	withX := vals(":i", one, ":x", str)

	tests := map[string]listIndexReq{
		// UpdateExpression: SET, REMOVE, ADD and DELETE, and the right side.
		"SetIndex":                {kind: "update", expr: "SET l[:i] = :v", values: i},
		"SetIndexUndefinedValue":  {kind: "update", expr: "SET l[:i] = :v", values: v},
		"SetIndexNoValues":        {kind: "update", expr: "SET l[:i] = :v"},
		"SetIndexUnusedValue":     {kind: "update", expr: "SET l[:i] = :v", values: vals(":i", one, ":v", str, ":z", str)},
		"SetIndexUnusedName":      {kind: "update", expr: "SET l[:i] = :v", names: map[string]string{"#z": "z"}, values: i},
		"SetIndexStringValue":     {kind: "update", expr: "SET l[:i] = :v", values: vals(":i", str, ":v", str)},
		"SetIndexUndeclaredName":  {kind: "update", expr: "SET #q[:i] = :v", values: i},
		"SetNameIndex":            {kind: "update", expr: "SET l[#n] = :v", names: map[string]string{"#n": "n"}, values: v},
		"SetNameIndexUndeclared":  {kind: "update", expr: "SET l[#n] = :v", values: v},
		"SetBareNameIndex":        {kind: "update", expr: "SET l[abc] = :v", values: v},
		"SetRightSide":            {kind: "update", expr: "SET x = l[:i]", values: vals(":i", one)},
		"SetRightSidePlus":        {kind: "update", expr: "SET n = n + l[:i]", values: vals(":i", one)},
		"SetRightSideListAppend":  {kind: "update", expr: "SET l = list_append(l, l[:i])", values: vals(":i", one)},
		"SetRightSideIfNotExists": {kind: "update", expr: "SET x = if_not_exists(l[:i], :v)", values: i},
		"SetSecondClause":         {kind: "update", expr: "SET n = :v, l[:i] = :v", values: i},
		"SetThenSyntaxError":      {kind: "update", expr: "SET l[:i] = :v, = x", values: i},
		"SetMapKeyBrackets":       {kind: "update", expr: "SET m[:k] = :v", values: vals(":k", str, ":v", str)},
		"RemoveIndex":             {kind: "update", expr: "REMOVE l[:i]", values: vals(":i", one)},
		"RemoveIndexUndefined":    {kind: "update", expr: "REMOVE l[:i]"},
		"AddIndex":                {kind: "update", expr: "ADD l[:i] :v", values: vals(":i", one, ":v", one)},
		"DeleteIndex":             {kind: "update", expr: "DELETE l[:i] :v", values: vals(":i", one, ":v", &dynamodbtypes.AttributeValueMemberSS{Value: []string{"a"}})},
		// Nested and mixed with a literal.
		"NestedAfter":     {kind: "update", expr: "SET l[:i].m = :v", values: i},
		"NestedBefore":    {kind: "update", expr: "SET m.l[:i] = :v", values: i},
		"LiteralThenName": {kind: "update", expr: "SET l[0][:i] = :v", values: i},
		"NameThenLiteral": {kind: "update", expr: "SET l[:i][0] = :v", values: i},
		// ConditionExpression, on every write.
		"ConditionIndex":            {kind: "update", expr: "SET n = :v", cond: "l[:i] = :x", values: vals(":i", one, ":v", str, ":x", str)},
		"ConditionIndexUndefined":   {kind: "update", expr: "SET n = :v", cond: "l[:i] = :x", values: vals(":v", str, ":x", str)},
		"ConditionIndexUnusedValue": {kind: "update", expr: "SET n = :v", cond: "l[:i] = :x", values: vals(":i", one, ":v", str, ":x", str, ":z", str)},
		"ConditionFunction":         {kind: "update", expr: "SET n = :v", cond: "attribute_exists(l[:i])", values: i},
		"ConditionAfterLogic":       {kind: "update", expr: "SET n = :v", cond: "attribute_exists(n) AND l[:i] = :v", values: i},
		"ConditionNested":           {kind: "update", expr: "SET n = :v", cond: "l[:i].m = :v", values: i},
		"ConditionMixed":            {kind: "update", expr: "SET n = :v", cond: "l[0][:i] = :v", values: i},
		"ConditionNameIndex":        {kind: "update", expr: "SET n = :v", cond: "l[n] = :v", values: v},
		"UpdateBeforeCondition":     {kind: "update", expr: "SET l[:i] = :v", cond: "l[:i] = :x", values: vals(":i", one, ":v", str, ":x", str)},
		"UpdateUndefinedCondition":  {kind: "update", expr: "SET l[:i] = :v", cond: "#q = :v", values: i},
		"PutCondition":              {kind: "put", cond: "l[:i] = :x", values: withX},
		"DeleteCondition":           {kind: "delete", cond: "l[:i] = :x", values: withX},
		"TransactUpdate":            {kind: "transact", expr: "SET l[:i] = :v", values: i},
		"TransactCondition":         {kind: "transactcheck", cond: "l[:i] = :v", values: i},
		// FilterExpression, KeyConditionExpression and ProjectionExpression.
		"ScanFilter":          {kind: "scan", expr: "l[:i] = :x", values: withX},
		"ScanFilterUndefined": {kind: "scan", expr: "l[:i] = :x", values: vals(":x", str)},
		"ScanFilterNested":    {kind: "scan", expr: "m.l[:i] = :x", values: withX},
		"QueryFilter":         {kind: "query", expr: "l[:i] = :x", values: vals(":i", one, ":x", str, ":p", str)},
		"KeyCondition":        {kind: "keycond", expr: "pk = :p AND l[:i] = :x", values: vals(":i", one, ":x", str, ":p", str)},
		"KeyConditionUnused":  {kind: "keycond", expr: "pk = :p AND l[:i] = :x", values: vals(":i", one, ":x", str, ":p", str, ":z", str)},
		"KeyConditionOnSort":  {kind: "keycond", expr: "pk = :p AND sk[:i] = :x", values: vals(":i", one, ":x", str, ":p", str)},
		"KeyConditionOnHash":  {kind: "keycond", expr: "pk[:i] = :p", values: vals(":i", one, ":p", str)},
		"GetProjection":       {kind: "get", expr: "l[:i]"},
		"GetProjectionName":   {kind: "get", expr: "l[#n]", names: map[string]string{"#n": "n"}},
		"GetProjectionNested": {kind: "get", expr: "m.l[:i]"},
		"GetProjectionMixed":  {kind: "get", expr: "l[0][:i]"},
		"BatchGetProjection":  {kind: "batchget", expr: "l[:i]"},
		"QueryProjection":     {kind: "keycond", expr: "pk = :p", cond: "l[:i]", values: vals(":i", one, ":p", str)},
		// A literal index stays valid.
		"LiteralProjection": {kind: "get", expr: "l[0], l[1]"},
		"LiteralSet":        {kind: "update", expr: "SET l[1] = :v", values: v},
		"LiteralCondition":  {kind: "update", expr: "SET n = :v", cond: "l[1] = :v", values: vals(":v", fzS("q"))},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) string {
				t.Helper()
				ctx := context.Background()

				item := fuzzKey("a", "1")
				item["l"] = lpList("p", "q", "r")
				item["n"] = fzN("1")
				fuzzSetup(ctx, t, client, item)

				return fuzzErrString(listIndexRun(ctx, client, tt))
			})
		})
	}
}

// Parity for a literal list index (#164, #267). DynamoDB Local takes 0 to 2147483647: a larger
// literal, of any size, is "Invalid <X>Expression: List index is not within the allowable range;
// index: [N]", and a literal with a leading zero is a syntax error. Both are reported while the
// expression is checked, before the unused and undefined placeholders. A syntax error elsewhere
// in the same text comes before the range error, the first range error in the text is the one
// reported, and the same holds in every kind of expression. The index that is accepted is the
// boundary, 2147483647.
func TestE2E_ListIndexLiteral(t *testing.T) {
	str := fzS("x")
	v := map[string]dynamodbtypes.AttributeValue{":v": str}
	x := map[string]dynamodbtypes.AttributeValue{":x": str}
	px := map[string]dynamodbtypes.AttributeValue{":p": str, ":x": str}
	vz := map[string]dynamodbtypes.AttributeValue{":v": str, ":z": str}

	// every kind of expression and position, for each index text. A key condition cannot hold a
	// valid index, so the boundary is not tried there.
	positions := func(index string, boundary bool) map[string]listIndexReq {
		at := "[" + index + "]"
		cases := map[string]listIndexReq{
			"Set":                {kind: "update", expr: "SET l" + at + " = :v", values: v},
			"SetRightSide":       {kind: "update", expr: "SET n = :v, x = l" + at, values: v},
			"SetNested":          {kind: "update", expr: "SET m.l" + at + ".k = :v", values: v},
			"SetMixed":           {kind: "update", expr: "SET l[0]" + at + " = :v", values: v},
			"Remove":             {kind: "update", expr: "REMOVE l" + at},
			"Add":                {kind: "update", expr: "ADD l" + at + " :v", values: map[string]dynamodbtypes.AttributeValue{":v": fzN("1")}},
			"SecondClause":       {kind: "update", expr: "SET n = :v, l" + at + " = :v", values: v},
			"Condition":          {kind: "update", expr: "SET n = :v", cond: "l" + at + " = :v", values: v},
			"ConditionFn":        {kind: "update", expr: "SET n = :v", cond: "attribute_exists(l" + at + ")", values: v},
			"PutCondition":       {kind: "put", cond: "l" + at + " = :x", values: x},
			"DeleteCondition":    {kind: "delete", cond: "l" + at + " = :x", values: x},
			"TransactUpdate":     {kind: "transact", expr: "SET l" + at + " = :v", values: v},
			"TransactCheck":      {kind: "transactcheck", cond: "l" + at + " = :v", values: v},
			"ScanFilter":         {kind: "scan", expr: "l" + at + " = :x", values: x},
			"QueryFilter":        {kind: "query", expr: "l" + at + " = :x", values: px},
			"GetProjection":      {kind: "get", expr: "l" + at},
			"GetProjectionTwo":   {kind: "get", expr: "n, m.l" + at},
			"BatchGetProjection": {kind: "batchget", expr: "l" + at},
			"QueryProjection":    {kind: "keycond", expr: "pk = :p", cond: "l" + at, values: map[string]dynamodbtypes.AttributeValue{":p": str}},
		}

		if !boundary {
			cases["KeyCondition"] = listIndexReq{kind: "keycond", expr: "pk = :p AND l" + at + " = :x", values: px}
		}

		return cases
	}

	tests := map[string]listIndexReq{}

	for name, index := range map[string]struct {
		text     string
		boundary bool
	}{
		"Boundary":   {"2147483647", true},
		"Above":      {"2147483648", false},
		"Power32":    {"4294967296", false},
		"Int64Max":   {"9223372036854775807", false},
		"Int64Over":  {"9223372036854775808", false},
		"Huge":       {"99999999999999999999999999", false},
		"LeadZero":   {"007", false},
		"DoubleZero": {"00", false},
		"ZeroOne":    {"01", false},
		"ZeroPadded": {"00000000002147483648", false},
	} {
		for position, req := range positions(index.text, index.boundary) {
			tests[name+position] = req
		}
	}

	// The order against the other errors, in one expression.
	for name, req := range map[string]listIndexReq{
		"RangeUndefinedValue":          {kind: "update", expr: "SET l[2147483648] = :v"},
		"RangeUnusedValue":             {kind: "update", expr: "SET l[2147483648] = :v", values: vz},
		"RangeUnusedName":              {kind: "update", expr: "SET l[2147483648] = :v", names: map[string]string{"#z": "z"}, values: v},
		"RangeUndeclaredName":          {kind: "update", expr: "SET #q[2147483648] = :v", values: v},
		"RangeFirstOfTwo":              {kind: "update", expr: "SET l[2147483649] = :v, m[2147483648] = :v", values: v},
		"RangeNested":                  {kind: "update", expr: "SET l[0][2147483648].a = :v", values: v},
		"RangeThenPlaceholder":         {kind: "update", expr: "SET l[2147483648] = :v, m[:i] = :v", values: v},
		"PlaceholderThenRange":         {kind: "update", expr: "SET m[:i] = :v, l[2147483648] = :v", values: v},
		"RangeThenZero":                {kind: "update", expr: "SET m[2147483648] = :v, l[007] = :v", values: v},
		"ZeroThenRange":                {kind: "update", expr: "SET l[007] = :v, m[2147483648] = :v", values: v},
		"ZeroUndefinedValue":           {kind: "update", expr: "SET l[007] = :v"},
		"ZeroUnusedValue":              {kind: "update", expr: "SET l[007] = :v", values: vz},
		"ZeroUnusedName":               {kind: "update", expr: "SET l[007] = :v", names: map[string]string{"#z": "z"}, values: v},
		"ZeroUndeclaredName":           {kind: "update", expr: "SET #q[007] = :v", values: v},
		"ZeroNested":                   {kind: "update", expr: "SET l[0][007].a = :v", values: v},
		"RangeUpdateBeforeCond":        {kind: "update", expr: "SET l[2147483649] = :v", cond: "l[2147483648] = :v", values: v},
		"RangeCondUnusedValue":         {kind: "update", expr: "SET n = :v", cond: "l[2147483648] = :v", values: vz},
		"ZeroCondUnusedValue":          {kind: "update", expr: "SET n = :v", cond: "l[007] = :v", values: vz},
		"ScanRangeUndefinedValue":      {kind: "scan", expr: "l[2147483648] = :q", values: x},
		"ScanRangeUnusedValue":         {kind: "scan", expr: "l[2147483648] = :x", values: map[string]dynamodbtypes.AttributeValue{":x": str, ":z": str}},
		"ScanZeroUnusedValue":          {kind: "scan", expr: "l[007] = :x", values: map[string]dynamodbtypes.AttributeValue{":x": str, ":z": str}},
		"ProjectionRangeName":          {kind: "get", expr: "l[2147483648], #q"},
		"ProjectionZeroName":           {kind: "get", expr: "l[007], #q"},
		"ProjectionRangeUnusedName":    {kind: "get", expr: "l[2147483648]", names: map[string]string{"#z": "z"}},
		"ProjectionZeroThenRange":      {kind: "get", expr: "l[007], l[2147483648]"},
		"ProjectionRangeThenZero":      {kind: "get", expr: "l[2147483648], l[007]"},
		"QueryFilterRangeUnused":       {kind: "query", expr: "l[2147483648] = :x", values: map[string]dynamodbtypes.AttributeValue{":p": str, ":x": str, ":z": str}},
		"QueryFilterZeroUndefined":     {kind: "query", expr: "l[007] = :q", values: px},
		"QueryProjectionRangeName":     {kind: "keycond", expr: "pk = :p", cond: "l[2147483648], #q", values: map[string]dynamodbtypes.AttributeValue{":p": str}},
		"QueryKeyUndefinedFilterRange": {kind: "query", expr: "l[2147483648] = :x", values: px},
	} {
		tests[name] = req
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) string {
				t.Helper()
				ctx := context.Background()

				item := fuzzKey("a", "1")
				item["l"] = lpList("p", "q", "r")
				item["n"] = fzN("1")
				fuzzSetup(ctx, t, client, item)

				return fuzzErrString(listIndexRun(ctx, client, tt))
			})
		})
	}
}

// listIndexRun sends the request of tt against the item fuzzSetup stored.
func listIndexRun(ctx context.Context, client *dynamodb.Client, tt listIndexReq) error {
	table, key := aws.String(fuzzTable), fuzzKey("a", "1")
	optional := func(s string) *string {
		if s == "" {
			return nil
		}

		return aws.String(s)
	}

	var err error

	switch tt.kind {
	case "update":
		_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: table, Key: key, UpdateExpression: aws.String(tt.expr), ConditionExpression: optional(tt.cond),
			ExpressionAttributeNames: tt.names, ExpressionAttributeValues: tt.values,
		})
	case "put":
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: table, Item: key, ConditionExpression: optional(tt.cond), ExpressionAttributeValues: tt.values,
		})
	case "delete":
		_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: table, Key: key, ConditionExpression: optional(tt.cond), ExpressionAttributeValues: tt.values,
		})
	case "scan":
		_, err = client.Scan(ctx, &dynamodb.ScanInput{
			TableName: table, FilterExpression: aws.String(tt.expr), ExpressionAttributeNames: tt.names, ExpressionAttributeValues: tt.values,
		})
	case "query":
		_, err = client.Query(ctx, &dynamodb.QueryInput{
			TableName: table, KeyConditionExpression: aws.String("pk = :p"), FilterExpression: aws.String(tt.expr),
			ExpressionAttributeValues: tt.values,
		})
	case "keycond":
		// cond is the ProjectionExpression of this kind.
		_, err = client.Query(ctx, &dynamodb.QueryInput{
			TableName: table, KeyConditionExpression: aws.String(tt.expr), ProjectionExpression: optional(tt.cond),
			ExpressionAttributeValues: tt.values,
		})
	case "get":
		_, err = client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: table, Key: key, ProjectionExpression: aws.String(tt.expr), ExpressionAttributeNames: tt.names,
		})
	case "batchget":
		_, err = client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
			fuzzTable: {Keys: []map[string]dynamodbtypes.AttributeValue{key}, ProjectionExpression: aws.String(tt.expr)},
		}})
	case "transact":
		_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{{
			Update: &dynamodbtypes.Update{TableName: table, Key: key, UpdateExpression: aws.String(tt.expr), ExpressionAttributeValues: tt.values},
		}}})
	case "transactcheck":
		_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{{
			ConditionCheck: &dynamodbtypes.ConditionCheck{TableName: table, Key: key, ConditionExpression: aws.String(tt.cond), ExpressionAttributeValues: tt.values},
		}}})
	default:
		return fmt.Errorf("unknown kind %q", tt.kind)
	}

	return err
}
