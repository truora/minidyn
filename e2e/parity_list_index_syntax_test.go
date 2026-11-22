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
