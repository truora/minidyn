package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// DynamoDB rejects an empty String, Number or Binary Set, as an attribute of a written item
// (top level or nested in L and M) and as the value of an expression placeholder (#294). The
// text keeps Local's wording, double space included ("An string set  may not be empty"). The
// cases below also pin where the empty set sits among the other value checks.

type emptySetEnv = numValEnv

func emptySetSetup(t *testing.T, client *dynamodb.Client) emptySetEnv {
	t.Helper()

	return numValSetup(t, client)
}

func esSS(v ...string) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberSS{Value: v}
}

func esBS(v ...[]byte) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberBS{Value: v}
}

// esValues are the attribute values every placement is run with: the empty sets alone, nested,
// and next to each other kind of problem, in both orders.
func esValues() []struct {
	name string
	av   dynamodbtypes.AttributeValue
} {
	empty := map[string]dynamodbtypes.AttributeValue{"SS": esSS(), "NS": numValNS(), "BS": esBS()}
	nul := &dynamodbtypes.AttributeValueMemberNULL{Value: true}

	var out []struct {
		name string
		av   dynamodbtypes.AttributeValue
	}

	add := func(name string, av dynamodbtypes.AttributeValue) {
		out = append(out, struct {
			name string
			av   dynamodbtypes.AttributeValue
		}{name, av})
	}

	for _, k := range []string{"SS", "NS", "BS"} {
		add(k+"/empty", empty[k])
		add(k+"/inList", numValList(fzN("1"), empty[k]))
		add(k+"/inMap", numValMap("a", empty[k]))
		add(k+"/deep", numValList(numValMap("a", numValList(empty[k]))))
	}

	add("SS/nonEmpty", esSS("a"))
	add("NS/nonEmpty", numValNS("1"))
	add("BS/nonEmpty", esBS([]byte("a")))
	add("SS/emptyMember", esSS(""))
	add("BS/emptyMember", esBS([]byte{}))

	// the order against the other value checks, in both orders, in one list
	add("list/emptySSThenBadNumber", numValList(esSS(), fzN("abc")))
	add("list/badNumberThenEmptySS", numValList(fzN("abc"), esSS()))
	add("list/emptySSThenDupSS", numValList(esSS(), esSS("a", "a")))
	add("list/dupSSThenEmptySS", numValList(esSS("a", "a"), esSS()))
	add("list/emptyNSThenDupNS", numValList(numValNS(), numValNS("1", "1")))
	add("list/dupNSThenEmptyNS", numValList(numValNS("1", "1"), numValNS()))
	add("list/emptyBSThenDupBS", numValList(esBS(), esBS([]byte("a"), []byte("a"))))
	add("list/dupBSThenEmptyBS", numValList(esBS([]byte("a"), []byte("a")), esBS()))
	add("list/emptyNSThenBadNumberSet", numValList(numValNS(), numValNS("abc")))
	add("list/badNumberSetThenEmptyNS", numValList(numValNS("abc"), numValNS()))
	add("list/emptySSThenNilElement", numValList(esSS(), nil))
	add("list/emptySSThenNULL", numValList(esSS(), nul))
	add("list/emptySSThenEmptyNS", numValList(esSS(), numValNS()))
	add("list/emptyNSThenEmptySS", numValList(numValNS(), esSS()))
	add("list/emptyBSThenEmptySS", numValList(esBS(), esSS()))

	return out
}

func esItemOps() []struct {
	name string
	run  func(env emptySetEnv, av dynamodbtypes.AttributeValue) error
} {
	type op = struct {
		name string
		run  func(env emptySetEnv, av dynamodbtypes.AttributeValue) error
	}

	table := aws.String(fuzzTable)

	return []op{
		{"PutItem", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.PutItem(env.ctx, &dynamodb.PutItemInput{TableName: table, Item: numValItem(av)})

			return err
		}},
		{"BatchWriteItem", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.BatchWriteItem(env.ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
				fuzzTable: {{PutRequest: &dynamodbtypes.PutRequest{Item: numValItem(av)}}},
			}})

			return err
		}},
		{"TransactWriteItems/put", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Put: &dynamodbtypes.Put{TableName: table, Item: numValItem(av)}},
			}})

			return err
		}},
		{"UpdateItem/set", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.UpdateItem(env.ctx, &dynamodb.UpdateItemInput{
				TableName: table, Key: numValKey("1"), UpdateExpression: aws.String("SET x = :v"), ExpressionAttributeValues: numAV{":v": av},
			})

			return err
		}},
		{"UpdateItem/add", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.UpdateItem(env.ctx, &dynamodb.UpdateItemInput{
				TableName: table, Key: numValKey("1"), UpdateExpression: aws.String("ADD x :v"), ExpressionAttributeValues: numAV{":v": av},
			})

			return err
		}},
		{"UpdateItem/delete", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.UpdateItem(env.ctx, &dynamodb.UpdateItemInput{
				TableName: table, Key: numValKey("1"), UpdateExpression: aws.String("DELETE x :v"), ExpressionAttributeValues: numAV{":v": av},
			})

			return err
		}},
		{"UpdateItem/condition", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.UpdateItem(env.ctx, &dynamodb.UpdateItemInput{
				TableName: table, Key: numValKey("1"), UpdateExpression: aws.String("SET y = :one"),
				ConditionExpression:       aws.String("attribute_not_exists(x) OR x = :v"),
				ExpressionAttributeValues: numAV{":v": av, ":one": fzN("1")},
			})

			return err
		}},
		{"PutItem/condition", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.PutItem(env.ctx, &dynamodb.PutItemInput{
				TableName: table, Item: numValItem(fzS("c")),
				ConditionExpression: aws.String("attribute_not_exists(x) OR x = :v"), ExpressionAttributeValues: numAV{":v": av},
			})

			return err
		}},
		{"DeleteItem/condition", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.DeleteItem(env.ctx, &dynamodb.DeleteItemInput{
				TableName: table, Key: numValKey("3"),
				ConditionExpression: aws.String("attribute_not_exists(x) OR x = :v"), ExpressionAttributeValues: numAV{":v": av},
			})

			return err
		}},
		{"Query/filter", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.Query(env.ctx, &dynamodb.QueryInput{
				TableName: table, KeyConditionExpression: aws.String("pk = :p"), FilterExpression: aws.String("x = :v"),
				ExpressionAttributeValues: numAV{":p": fzS("a"), ":v": av},
			})

			return err
		}},
		{"Scan/filter", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.Scan(env.ctx, &dynamodb.ScanInput{
				TableName: table, FilterExpression: aws.String("x = :v"), ExpressionAttributeValues: numAV{":v": av},
			})

			return err
		}},
		{"Scan/contains", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.Scan(env.ctx, &dynamodb.ScanInput{
				TableName: table, FilterExpression: aws.String("contains(x, :v)"), ExpressionAttributeValues: numAV{":v": av},
			})

			return err
		}},
		{"TransactWriteItems/update", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Update: &dynamodbtypes.Update{
					TableName: table, Key: numValKey("1"), UpdateExpression: aws.String("SET x = :v"), ExpressionAttributeValues: numAV{":v": av},
				}},
			}})

			return err
		}},
		{"TransactWriteItems/conditionCheck", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{ConditionCheck: &dynamodbtypes.ConditionCheck{
					TableName: table, Key: numValKey("1"), ConditionExpression: aws.String("attribute_not_exists(x) OR x = :v"),
					ExpressionAttributeValues: numAV{":v": av},
				}},
			}})

			return err
		}},
	}
}

func esKeyOps() []struct {
	name string
	run  func(env emptySetEnv, av dynamodbtypes.AttributeValue) error
} {
	type op = struct {
		name string
		run  func(env emptySetEnv, av dynamodbtypes.AttributeValue) error
	}

	table := aws.String(fuzzTable)
	key := func(av dynamodbtypes.AttributeValue) numAV { return numAV{"pk": fzS("a"), "sk": av} }

	return []op{
		{"GetItem/key", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.GetItem(env.ctx, &dynamodb.GetItemInput{TableName: table, Key: key(av)})

			return err
		}},
		{"DeleteItem/key", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.DeleteItem(env.ctx, &dynamodb.DeleteItemInput{TableName: table, Key: key(av)})

			return err
		}},
		{"UpdateItem/key", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.UpdateItem(env.ctx, &dynamodb.UpdateItemInput{
				TableName: table, Key: key(av), UpdateExpression: aws.String("SET y = :one"), ExpressionAttributeValues: numAV{":one": fzN("1")},
			})

			return err
		}},
		{"PutItem/key", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.PutItem(env.ctx, &dynamodb.PutItemInput{TableName: table, Item: key(av)})

			return err
		}},
		{"BatchGetItem/key", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.BatchGetItem(env.ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
				fuzzTable: {Keys: []numAV{key(av)}},
			}})

			return err
		}},
		{"Query/exclusiveStartKey", func(env emptySetEnv, av dynamodbtypes.AttributeValue) error {
			_, err := env.client.Query(env.ctx, &dynamodb.QueryInput{
				TableName: table, KeyConditionExpression: aws.String("pk = :p"), ExpressionAttributeValues: numAV{":p": fzS("a")},
				ExclusiveStartKey: key(av),
			})

			return err
		}},
	}
}

func emptySetName(name string) bool {
	for _, suffix := range []string{"/empty", "/inList", "/inMap", "/deep"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}

	return false
}

func esRun(t *testing.T, client *dynamodb.Client) []string {
	t.Helper()

	env := emptySetSetup(t, client)
	ctx := context.Background()

	var out []string

	ops := esItemOps()

	for _, op := range append(ops, esKeyOps()...) {
		isKeyOp := strings.Contains(op.name, "key") || strings.Contains(op.name, "Key")

		for _, v := range esValues() {
			// A set that is not empty in a key is a wrong key type, which minidyn words
			// differently from Local ("Type mismatch for key" vs "Invalid attribute value
			// type"): not what these cases are about.
			if isKeyOp && !strings.HasPrefix(v.name, "list/") && !emptySetName(v.name) {
				continue
			}

			line := fmt.Sprintf("%s %s => %s", op.name, v.name, numValResult(op.run(env, v.av)))
			out = append(out, line)

			// Leave the table as setup made it.
			_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(fuzzTable), Key: numValKey("2")})
			require.NoError(t, err)

			_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(fuzzTable), Item: numValKey("1")})
			require.NoError(t, err)
		}
	}

	return out
}

func TestE2E_EmptySets(t *testing.T) {
	RunE2E(t, func(t *testing.T, client *dynamodb.Client) []string {
		t.Helper()

		out := esRun(t, client)
		if testing.Verbose() {
			for _, l := range out {
				t.Log(l)
			}
		}

		return out
	})
}
