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

// DynamoDB rejects an N value that is not a number, whose exponent text does not fit in 32 bits,
// whose magnitude is outside 1E-130 to 9.99...E+125, or that has more than 38 significant digits
// (#226, #273). It does so for every N of a written item (top level, nested in L and M, every
// member of an NS), for the numbers in the key of any operation, and for the values of an
// expression, where the message names the placeholder. It reports them before any other problem
// of the request, once the request is known to be about an existing table.

// numValEnv is a client on the fuzz table: pk (S) / sk (N), with a GSI on g (S) / r (B).
type numValEnv struct {
	ctx    context.Context
	client *dynamodb.Client
}

func numValSetup(t *testing.T, client *dynamodb.Client) numValEnv {
	t.Helper()

	ctx := context.Background()
	fuzzSetupFor(ctx, t, client, fuzzSchemaN, numValKey("1"))

	return numValEnv{ctx: ctx, client: client}
}

type numAV = map[string]dynamodbtypes.AttributeValue

func numValKey(sk string) numAV { return numAV{"pk": fzS("a"), "sk": fzN(sk)} }

func numValItem(attr dynamodbtypes.AttributeValue) numAV {
	item := numValKey("2")
	item["x"] = attr

	return item
}

func numValList(v ...dynamodbtypes.AttributeValue) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberL{Value: v}
}

func numValMap(k string, v dynamodbtypes.AttributeValue) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{k: v}}
}

func numValNS(v ...string) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberNS{Value: v}
}

// numValResult is "ok" or the normalized error. A duplicate set member is worded differently by
// the two engines (the sets themselves are covered elsewhere), so it is reduced to its meaning.
func numValResult(err error) string {
	s := fuzzErrString(err)
	if strings.Contains(s, "ollection") && strings.Contains(s, "duplicates") {
		return "duplicates"
	}

	return s
}

// numValInvalid are the inputs of the table in #226, one or more per row.
var numValInvalid = []string{
	"abc", "", "-", "1e", "0x10", " 5", "NaN", "Infinity", "1e2147483648", "1e-2147483649", "1e100000000000", "0e99999999999",
	"1e126", "1e2147483647", "-1e200",
	"1e-131", "1e-2147483647", "-1e-200",
	"123456789012345678901234567890123456789", "-123456789012345678901234567890123456789",
	"1234567890123456789012345678901234567890", "0.123456789012345678901234567890123456789",
	"123456789012345678901234567890123456789e-40", "123456789012345678901234567890123456789e130",
}

// numValValid are spellings DynamoDB accepts: leading and trailing zeros, a signed zero, 38
// digits with a trailing zero, and the limits of the range. Only that they are accepted is
// compared: what is stored is covered by the normalization of #226 and the number tests.
var numValValid = []string{
	"-0", "0.0", "00012345678901234567890123456789012345678", "123456789012345678901234567890123456780",
	"1.00000000000000000000000000000000000000000", "5.", ".5", "+5", "1e+2", "1e125", "9.9999999999999999999999999999999999999E+125",
	"1e-130", "-1e-130", "12345678901234567890123456789012345678e5",
}

type numValOp struct {
	name string
	run  func(env numValEnv, n string) error
}

func numValItemOps() []numValOp {
	put := func(attr func(n string) dynamodbtypes.AttributeValue) func(env numValEnv, n string) error {
		return func(env numValEnv, n string) error {
			_, err := env.client.PutItem(env.ctx, &dynamodb.PutItemInput{TableName: aws.String(fuzzTable), Item: numValItem(attr(n))})

			return err
		}
	}
	batch := func(attr func(n string) dynamodbtypes.AttributeValue) func(env numValEnv, n string) error {
		return func(env numValEnv, n string) error {
			_, err := env.client.BatchWriteItem(env.ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
				fuzzTable: {{PutRequest: &dynamodbtypes.PutRequest{Item: numValItem(attr(n))}}},
			}})

			return err
		}
	}
	transact := func(attr func(n string) dynamodbtypes.AttributeValue) func(env numValEnv, n string) error {
		return func(env numValEnv, n string) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Put: &dynamodbtypes.Put{TableName: aws.String(fuzzTable), Item: numValItem(attr(n))}},
			}})

			return err
		}
	}

	top := fzN
	inList := func(n string) dynamodbtypes.AttributeValue { return numValList(fzN("1"), fzN(n)) }
	inMap := func(n string) dynamodbtypes.AttributeValue { return numValMap("a", fzN(n)) }
	deep := func(n string) dynamodbtypes.AttributeValue { return numValList(numValMap("a", numValList(fzN(n)))) }
	inSet := func(n string) dynamodbtypes.AttributeValue { return numValNS(n) }
	inSetNext := func(n string) dynamodbtypes.AttributeValue { return numValNS("2", n) }

	return []numValOp{
		{"PutItem/top", put(top)},
		{"PutItem/list", put(inList)},
		{"PutItem/map", put(inMap)},
		{"PutItem/deep", put(deep)},
		{"PutItem/set", put(inSet)},
		{"PutItem/setSecondMember", put(inSetNext)},
		{"BatchWriteItem/put", batch(top)},
		{"BatchWriteItem/putDeep", batch(deep)},
		{"BatchWriteItem/putSet", batch(inSet)},
		{"TransactWriteItems/put", transact(top)},
		{"TransactWriteItems/putDeep", transact(deep)},
		{"TransactWriteItems/putSet", transact(inSet)},
	}
}

func numValExprOps() []numValOp {
	table := aws.String(fuzzTable)
	val := func(n string) map[string]dynamodbtypes.AttributeValue { return numAV{":v": fzN(n)} }

	return []numValOp{
		{"UpdateItem/set", func(env numValEnv, n string) error {
			_, err := env.client.UpdateItem(env.ctx, &dynamodb.UpdateItemInput{
				TableName: table, Key: numValKey("1"), UpdateExpression: aws.String("SET x = :v"), ExpressionAttributeValues: val(n),
			})

			return err
		}},
		{"UpdateItem/setNested", func(env numValEnv, n string) error {
			_, err := env.client.UpdateItem(env.ctx, &dynamodb.UpdateItemInput{
				TableName: table, Key: numValKey("1"), UpdateExpression: aws.String("SET x = :v"),
				ExpressionAttributeValues: numAV{":v": numValList(numValMap("a", fzN(n)))},
			})

			return err
		}},
		{"UpdateItem/add", func(env numValEnv, n string) error {
			_, err := env.client.UpdateItem(env.ctx, &dynamodb.UpdateItemInput{
				TableName: table, Key: numValKey("1"), UpdateExpression: aws.String("ADD x :v"), ExpressionAttributeValues: val(n),
			})

			return err
		}},
		{"UpdateItem/addSet", func(env numValEnv, n string) error {
			_, err := env.client.UpdateItem(env.ctx, &dynamodb.UpdateItemInput{
				TableName: table, Key: numValKey("1"), UpdateExpression: aws.String("ADD s :v"), ExpressionAttributeValues: numAV{":v": numValNS(n)},
			})

			return err
		}},
		{"UpdateItem/condition", func(env numValEnv, n string) error {
			_, err := env.client.UpdateItem(env.ctx, &dynamodb.UpdateItemInput{
				TableName: table, Key: numValKey("1"), UpdateExpression: aws.String("SET y = :one"),
				ConditionExpression:       aws.String("attribute_not_exists(x) OR x = :v"),
				ExpressionAttributeValues: numAV{":v": fzN(n), ":one": fzN("1")},
			})

			return err
		}},
		{"PutItem/condition", func(env numValEnv, n string) error {
			_, err := env.client.PutItem(env.ctx, &dynamodb.PutItemInput{
				TableName: table, Item: numValItem(fzS("c")),
				ConditionExpression: aws.String("attribute_not_exists(x) OR x = :v"), ExpressionAttributeValues: val(n),
			})

			return err
		}},
		{"DeleteItem/condition", func(env numValEnv, n string) error {
			_, err := env.client.DeleteItem(env.ctx, &dynamodb.DeleteItemInput{
				TableName: table, Key: numValKey("3"),
				ConditionExpression: aws.String("attribute_not_exists(x) OR x = :v"), ExpressionAttributeValues: val(n),
			})

			return err
		}},
		{"Query/keyCondition", func(env numValEnv, n string) error {
			_, err := env.client.Query(env.ctx, &dynamodb.QueryInput{
				TableName: table, KeyConditionExpression: aws.String("pk = :p AND sk = :v"),
				ExpressionAttributeValues: numAV{":p": fzS("a"), ":v": fzN(n)},
			})

			return err
		}},
		{"Query/between", func(env numValEnv, n string) error {
			_, err := env.client.Query(env.ctx, &dynamodb.QueryInput{
				TableName: table, KeyConditionExpression: aws.String("pk = :p AND sk BETWEEN :a AND :v"),
				ExpressionAttributeValues: numAV{":p": fzS("a"), ":a": fzN("-1"), ":v": fzN(n)},
			})

			return err
		}},
		{"Query/filter", func(env numValEnv, n string) error {
			_, err := env.client.Query(env.ctx, &dynamodb.QueryInput{
				TableName: table, KeyConditionExpression: aws.String("pk = :p"), FilterExpression: aws.String("x = :v"),
				ExpressionAttributeValues: numAV{":p": fzS("a"), ":v": fzN(n)},
			})

			return err
		}},
		{"Query/gsiFilter", func(env numValEnv, n string) error {
			_, err := env.client.Query(env.ctx, &dynamodb.QueryInput{
				TableName: table, IndexName: aws.String(fuzzGSI), KeyConditionExpression: aws.String("g = :g"),
				FilterExpression:          aws.String("sk = :v"),
				ExpressionAttributeValues: numAV{":g": fzS("g"), ":v": fzN(n)},
			})

			return err
		}},
		{"Scan/filter", func(env numValEnv, n string) error {
			_, err := env.client.Scan(env.ctx, &dynamodb.ScanInput{
				TableName: table, FilterExpression: aws.String("x = :v"), ExpressionAttributeValues: val(n),
			})

			return err
		}},
		{"Scan/filterNested", func(env numValEnv, n string) error {
			_, err := env.client.Scan(env.ctx, &dynamodb.ScanInput{
				TableName: table, FilterExpression: aws.String("x = :v"), ExpressionAttributeValues: numAV{":v": numValList(fzN(n))},
			})

			return err
		}},
		{"TransactWriteItems/update", func(env numValEnv, n string) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Update: &dynamodbtypes.Update{
					TableName: table, Key: numValKey("1"), UpdateExpression: aws.String("SET x = :v"), ExpressionAttributeValues: val(n),
				}},
			}})

			return err
		}},
		{"TransactWriteItems/conditionCheck", func(env numValEnv, n string) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{ConditionCheck: &dynamodbtypes.ConditionCheck{
					TableName: table, Key: numValKey("1"), ConditionExpression: aws.String("attribute_not_exists(x) OR x = :v"),
					ExpressionAttributeValues: val(n),
				}},
			}})

			return err
		}},
		{"TransactWriteItems/putCondition", func(env numValEnv, n string) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Put: &dynamodbtypes.Put{
					TableName: table, Item: numValItem(fzS("c")), ConditionExpression: aws.String("attribute_not_exists(x) OR x = :v"),
					ExpressionAttributeValues: val(n),
				}},
			}})

			return err
		}},
		{"TransactWriteItems/deleteCondition", func(env numValEnv, n string) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Delete: &dynamodbtypes.Delete{
					TableName: table, Key: numValKey("3"), ConditionExpression: aws.String("attribute_not_exists(x) OR x = :v"),
					ExpressionAttributeValues: val(n),
				}},
			}})

			return err
		}},
	}
}

func numValKeyOps() []numValOp {
	table := aws.String(fuzzTable)
	key := func(n string) numAV { return numAV{"pk": fzS("a"), "sk": fzN(n)} }

	return []numValOp{
		{"PutItem/key", func(env numValEnv, n string) error {
			_, err := env.client.PutItem(env.ctx, &dynamodb.PutItemInput{TableName: table, Item: key(n)})

			return err
		}},
		{"GetItem/key", func(env numValEnv, n string) error {
			_, err := env.client.GetItem(env.ctx, &dynamodb.GetItemInput{TableName: table, Key: key(n)})

			return err
		}},
		{"UpdateItem/key", func(env numValEnv, n string) error {
			_, err := env.client.UpdateItem(env.ctx, &dynamodb.UpdateItemInput{
				TableName: table, Key: key(n), UpdateExpression: aws.String("SET y = :one"), ExpressionAttributeValues: numAV{":one": fzN("1")},
			})

			return err
		}},
		{"DeleteItem/key", func(env numValEnv, n string) error {
			_, err := env.client.DeleteItem(env.ctx, &dynamodb.DeleteItemInput{TableName: table, Key: key(n)})

			return err
		}},
		{"BatchGetItem/key", func(env numValEnv, n string) error {
			_, err := env.client.BatchGetItem(env.ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
				fuzzTable: {Keys: []numAV{key(n)}},
			}})

			return err
		}},
		{"TransactGetItems/key", func(env numValEnv, n string) error {
			_, err := env.client.TransactGetItems(env.ctx, &dynamodb.TransactGetItemsInput{TransactItems: []dynamodbtypes.TransactGetItem{
				{Get: &dynamodbtypes.Get{TableName: table, Key: key(n)}},
			}})

			return err
		}},
		{"BatchWriteItem/putKey", func(env numValEnv, n string) error {
			_, err := env.client.BatchWriteItem(env.ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
				fuzzTable: {{PutRequest: &dynamodbtypes.PutRequest{Item: key(n)}}},
			}})

			return err
		}},
		{"BatchWriteItem/deleteKey", func(env numValEnv, n string) error {
			_, err := env.client.BatchWriteItem(env.ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
				fuzzTable: {{DeleteRequest: &dynamodbtypes.DeleteRequest{Key: key(n)}}},
			}})

			return err
		}},
		{"TransactWriteItems/putKey", func(env numValEnv, n string) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Put: &dynamodbtypes.Put{TableName: table, Item: key(n)}},
			}})

			return err
		}},
		{"TransactWriteItems/updateKey", func(env numValEnv, n string) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Update: &dynamodbtypes.Update{
					TableName: table, Key: key(n), UpdateExpression: aws.String("SET y = :one"), ExpressionAttributeValues: numAV{":one": fzN("1")},
				}},
			}})

			return err
		}},
		{"TransactWriteItems/deleteKey", func(env numValEnv, n string) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Delete: &dynamodbtypes.Delete{TableName: table, Key: key(n)}},
			}})

			return err
		}},
		{"TransactWriteItems/conditionCheckKey", func(env numValEnv, n string) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{ConditionCheck: &dynamodbtypes.ConditionCheck{
					TableName: table, Key: key(n), ConditionExpression: aws.String("attribute_exists(pk)"),
				}},
			}})

			return err
		}},
		{"Query/exclusiveStartKey", func(env numValEnv, n string) error {
			_, err := env.client.Query(env.ctx, &dynamodb.QueryInput{
				TableName: table, KeyConditionExpression: aws.String("pk = :p"), ExpressionAttributeValues: numAV{":p": fzS("a")},
				ExclusiveStartKey: key(n),
			})

			return err
		}},
		{"Scan/exclusiveStartKey", func(env numValEnv, n string) error {
			_, err := env.client.Scan(env.ctx, &dynamodb.ScanInput{TableName: table, ExclusiveStartKey: key(n)})

			return err
		}},
	}
}

// numValRun runs every operation with every input on one table, and returns what each reported.
func numValRun(t *testing.T, client *dynamodb.Client, ops []numValOp, inputs []string) []string {
	t.Helper()

	env := numValSetup(t, client)
	out := make([]string, 0, len(ops)*len(inputs))

	for _, op := range ops {
		for _, in := range inputs {
			out = append(out, fmt.Sprintf("%s %q => %s", op.name, in, numValResult(op.run(env, in))))

			// Leave the table as setup made it, so what one input stores does not change the next.
			_, err := client.DeleteItem(env.ctx, &dynamodb.DeleteItemInput{TableName: aws.String(fuzzTable), Key: numValKey("2")})
			require.NoError(t, err)

			_, err = client.PutItem(env.ctx, &dynamodb.PutItemInput{TableName: aws.String(fuzzTable), Item: numValKey("1")})
			require.NoError(t, err)
		}
	}

	return out
}

func TestE2E_NumberValidation(t *testing.T) {
	groups := []struct {
		name   string
		ops    []numValOp
		inputs []string
	}{
		{"ItemsInvalid", numValItemOps(), numValInvalid},
		{"ExpressionValuesInvalid", numValExprOps(), numValInvalid},
		{"KeysInvalid", numValKeyOps(), numValInvalid},
		{"ItemsValid", numValItemOps(), numValValid},
		{"ExpressionValuesValid", numValExprOps(), numValValid},
		{"KeysValid", numValKeyOps(), numValValid},
	}

	for _, g := range groups {
		t.Run(g.name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
				t.Helper()

				return numValRun(t, client, g.ops, g.inputs)
			})
		})
	}
}

// A number key is validated whatever the type of the attribute: an N under a string key is a
// number error before it is a type mismatch.
func TestE2E_NumberValidationKeyOfAnotherType(t *testing.T) {
	RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
		t.Helper()

		env := numValSetup(t, client)
		table := aws.String(fuzzTable)
		wrong := func(n string) numAV { return numAV{"pk": fzN(n), "sk": fzN("1")} }

		var out []string

		for _, n := range []string{"1", "abc", "1e126", "123456789012345678901234567890123456789"} {
			_, err := client.GetItem(env.ctx, &dynamodb.GetItemInput{TableName: table, Key: wrong(n)})
			out = append(out, "GetItem "+n+" => "+numValResult(err))

			_, err = client.PutItem(env.ctx, &dynamodb.PutItemInput{TableName: table, Item: wrong(n)})
			out = append(out, "PutItem "+n+" => "+numValResult(err))

			if n == "1" {
				// A valid number under a key of another type is a type mismatch, which the
				// transactions report as a cancellation (#181).
				continue
			}

			_, err = client.TransactGetItems(env.ctx, &dynamodb.TransactGetItemsInput{TransactItems: []dynamodbtypes.TransactGetItem{
				{Get: &dynamodbtypes.Get{TableName: table, Key: wrong(n)}},
			}})
			out = append(out, "TransactGetItems "+n+" => "+numValResult(err))

			_, err = client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{ConditionCheck: &dynamodbtypes.ConditionCheck{TableName: table, Key: wrong(n), ConditionExpression: aws.String("attribute_exists(pk)")}},
			}})
			out = append(out, "ConditionCheck "+n+" => "+numValResult(err))
		}

		return out
	})
}

// numValCase is one request with several problems, to pin which one DynamoDB reports first.
type numValCase struct {
	name string
	run  func(env numValEnv) error
}

func numValOrderCases() []numValCase {
	table := aws.String(fuzzTable)
	bad := fzN("abc")
	put := func(item numAV, mod func(*dynamodb.PutItemInput)) func(env numValEnv) error {
		return func(env numValEnv) error {
			in := &dynamodb.PutItemInput{TableName: table, Item: item}
			if mod != nil {
				mod(in)
			}

			_, err := env.client.PutItem(env.ctx, in)

			return err
		}
	}
	update := func(mod func(*dynamodb.UpdateItemInput)) func(env numValEnv) error {
		return func(env numValEnv) error {
			in := &dynamodb.UpdateItemInput{TableName: table, Key: numValKey("1")}
			mod(in)
			_, err := env.client.UpdateItem(env.ctx, in)

			return err
		}
	}
	del := func(mod func(*dynamodb.DeleteItemInput)) func(env numValEnv) error {
		return func(env numValEnv) error {
			in := &dynamodb.DeleteItemInput{TableName: table, Key: numValKey("1")}
			mod(in)
			_, err := env.client.DeleteItem(env.ctx, in)

			return err
		}
	}
	query := func(mod func(*dynamodb.QueryInput)) func(env numValEnv) error {
		return func(env numValEnv) error {
			in := &dynamodb.QueryInput{TableName: table}
			mod(in)
			_, err := env.client.Query(env.ctx, in)

			return err
		}
	}
	scan := func(mod func(*dynamodb.ScanInput)) func(env numValEnv) error {
		return func(env numValEnv) error {
			in := &dynamodb.ScanInput{TableName: table}
			mod(in)
			_, err := env.client.Scan(env.ctx, in)

			return err
		}
	}
	transact := func(items ...dynamodbtypes.TransactWriteItem) func(env numValEnv) error {
		return func(env numValEnv) error {
			_, err := env.client.TransactWriteItems(env.ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})

			return err
		}
	}
	txPut := func(item numAV, mod func(*dynamodbtypes.Put)) dynamodbtypes.TransactWriteItem {
		p := &dynamodbtypes.Put{TableName: table, Item: item}
		if mod != nil {
			mod(p)
		}

		return dynamodbtypes.TransactWriteItem{Put: p}
	}
	item := func(sk string, attrs ...any) numAV {
		it := numValKey(sk)
		for i := 0; i < len(attrs); i += 2 {
			it[attrs[i].(string)] = attrs[i+1].(dynamodbtypes.AttributeValue)
		}

		return it
	}
	vals := func(kv ...any) map[string]dynamodbtypes.AttributeValue {
		m := numAV{}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i].(string)] = kv[i+1].(dynamodbtypes.AttributeValue)
		}

		return m
	}
	cond := aws.String("x = :v")

	return []numValCase{
		// PutItem: the numbers of the item come before the other problems of the item.
		{"PutEmptyKey", put(numAV{"pk": fzS(""), "sk": fzN("2"), "x": bad}, nil)},
		{"PutMissingKey", put(numAV{"sk": fzN("2"), "x": bad}, nil)},
		{"PutKeyWrongType", put(numAV{"pk": fzN("1"), "sk": fzN("2"), "x": bad}, nil)},
		{"PutIndexKeyWrongType", put(item("2", "g", fzN("1"), "x", bad), nil)},
		{"PutEmptyIndexKey", put(item("2", "g", fzS(""), "x", bad), nil)},
		{"PutBadSortKeyEmptyIndexKey", put(item("abc", "g", fzS("")), nil)},
		{"PutUndeclaredName", put(item("2", "x", bad), func(in *dynamodb.PutItemInput) { in.ConditionExpression = aws.String("attribute_not_exists(#n)") })},
		{"PutUndefinedValue", put(item("2", "x", bad), func(in *dynamodb.PutItemInput) { in.ConditionExpression = aws.String("x = :nope") })},
		{"PutSyntaxError", put(item("2", "x", bad), func(in *dynamodb.PutItemInput) { in.ConditionExpression = aws.String("x = = 1") })},
		{"PutReturnValuesFirst", put(item("2", "x", bad), func(in *dynamodb.PutItemInput) { in.ReturnValues = dynamodbtypes.ReturnValueUpdatedNew })},
		{"PutUnusedValueFirst", put(item("2", "x", bad), func(in *dynamodb.PutItemInput) { in.ExpressionAttributeValues = vals(":v", fzN("1")) })},
		{"PutItemBeforeValues", put(item("2", "x", fzN("1e126")), func(in *dynamodb.PutItemInput) {
			in.ConditionExpression, in.ExpressionAttributeValues = cond, vals(":v", bad)
		})},
		{"PutValuesBeforeItem", put(item("2", "x", bad), func(in *dynamodb.PutItemInput) {
			in.ConditionExpression, in.ExpressionAttributeValues = cond, vals(":v", fzN("1e126"))
		})},
		{"PutReturnValuesOnConditionCheckFailureBeforeValues", put(item("2"), func(in *dynamodb.PutItemInput) {
			in.ConditionExpression, in.ExpressionAttributeValues = cond, vals(":v", bad)
			in.ReturnValuesOnConditionCheckFailure = "BOGUS"
		})},
		{"PutItemNumberBeforeReturnValuesOnConditionCheckFailure", put(item("2", "x", bad), func(in *dynamodb.PutItemInput) {
			in.ReturnValuesOnConditionCheckFailure = "BOGUS"
		})},
		{"PutMissingTableBeforeReturnValuesOnConditionCheckFailure", put(item("2"), func(in *dynamodb.PutItemInput) {
			in.TableName, in.ReturnValuesOnConditionCheckFailure = aws.String("nope"), "BOGUS"
		})},
		{"PutUnusedValueBeforeReturnValuesOnConditionCheckFailure", put(item("2"), func(in *dynamodb.PutItemInput) {
			in.ExpressionAttributeValues, in.ReturnValuesOnConditionCheckFailure = vals(":v", fzN("1")), "BOGUS"
		})},
		{"PutPrecisionBeforeOverflow", put(item("2", "x", fzN("1234567890123456789012345678901234567890e130")), nil)},
		{"PutPrecisionBeforeUnderflow", put(item("2", "x", fzN("1234567890123456789012345678901234567890e-200")), nil)},
		{"PutListFirstProblem", put(item("2", "x", numValList(fzN("1e126"), bad)), nil)},
		{"PutListPrecisionThenOverflow", put(item("2", "x", numValList(fzN("1234567890123456789012345678901234567890"), fzN("1e126"))), nil)},
		{"PutListOverflowThenPrecision", put(item("2", "x", numValList(fzN("1e126"), fzN("1234567890123456789012345678901234567890"))), nil)},
		{"PutListNumberThenDuplicate", put(item("2", "x", numValList(bad, &dynamodbtypes.AttributeValueMemberSS{Value: []string{"a", "a"}})), nil)},
		{"PutListDuplicateThenNumber", put(item("2", "x", numValList(&dynamodbtypes.AttributeValueMemberSS{Value: []string{"a", "a"}}, bad)), nil)},
		{"PutSetNumberThenDuplicate", put(item("2", "x", numValNS("abc", "1", "1")), nil)},
		{"PutSetDuplicateThenNumber", put(item("2", "x", numValNS("1", "1", "abc")), nil)},
		{"PutSetRepeatedInvalid", put(item("2", "x", numValNS("abc", "abc")), nil)},
		{"PutItemDuplicateThenValuesNumber", put(item("2", "x", numValNS("1", "1")), func(in *dynamodb.PutItemInput) {
			in.ConditionExpression, in.ExpressionAttributeValues = cond, vals(":v", bad)
		})},
		{"PutItemNumberThenValuesDuplicate", put(item("2", "x", bad), func(in *dynamodb.PutItemInput) {
			in.ConditionExpression, in.ExpressionAttributeValues = cond, vals(":v", numValNS("1", "1"))
		})},

		// UpdateItem: ReturnValues, then the key (numbers, shape, type), then the numbers of the
		// values, and only then the expressions.
		{"UpdateValuesBeforeSyntax", update(func(in *dynamodb.UpdateItemInput) {
			in.UpdateExpression, in.ExpressionAttributeValues = aws.String("SET x = = :v"), vals(":v", bad)
		})},
		{"UpdateValuesBeforeUndeclaredName", update(func(in *dynamodb.UpdateItemInput) {
			in.UpdateExpression, in.ExpressionAttributeValues = aws.String("SET #n = :v"), vals(":v", bad)
		})},
		{"UpdateValuesBeforeUndefinedValue", update(func(in *dynamodb.UpdateItemInput) {
			in.UpdateExpression, in.ExpressionAttributeValues = aws.String("SET x = :v, y = :nope"), vals(":v", bad)
		})},
		{"UpdateKeyNumberBeforeValues", update(func(in *dynamodb.UpdateItemInput) {
			in.Key = numAV{"pk": fzS("a"), "sk": fzN("1e126")}
			in.UpdateExpression, in.ExpressionAttributeValues = aws.String("SET x = :v"), vals(":v", bad)
		})},
		{"UpdateKeyNumberBeforeKeyShape", update(func(in *dynamodb.UpdateItemInput) {
			in.Key = numAV{"pk": fzS(""), "sk": bad}
			in.UpdateExpression, in.ExpressionAttributeValues = aws.String("SET x = :v"), vals(":v", fzN("1"))
		})},
		{"UpdateMissingKeyBeforeValues", update(func(in *dynamodb.UpdateItemInput) {
			in.Key = numAV{"pk": fzS("a")}
			in.UpdateExpression, in.ExpressionAttributeValues = aws.String("SET x = :v"), vals(":v", bad)
		})},
		{"UpdateEmptyKeyBeforeValues", update(func(in *dynamodb.UpdateItemInput) {
			in.Key = numAV{"pk": fzS(""), "sk": fzN("1")}
			in.UpdateExpression, in.ExpressionAttributeValues = aws.String("SET x = :v"), vals(":v", bad)
		})},
		{"UpdateKeyTypeBeforeValues", update(func(in *dynamodb.UpdateItemInput) {
			in.Key = numAV{"pk": fzS("a"), "sk": fzS("1")}
			in.UpdateExpression, in.ExpressionAttributeValues = aws.String("SET x = :v"), vals(":v", bad)
		})},
		{"UpdateEmptyIndexKeyAndValues", update(func(in *dynamodb.UpdateItemInput) {
			in.UpdateExpression, in.ExpressionAttributeValues = aws.String("SET g = :e, x = :v"), vals(":v", bad, ":e", fzS(""))
		})},
		{"UpdateReturnValuesFirst", update(func(in *dynamodb.UpdateItemInput) {
			in.UpdateExpression, in.ExpressionAttributeValues, in.ReturnValues = aws.String("SET x = :v"), vals(":v", bad), "BOGUS"
		})},
		{"UpdateSetDuplicateValues", update(func(in *dynamodb.UpdateItemInput) {
			in.UpdateExpression, in.ExpressionAttributeValues = aws.String("ADD s :v"), vals(":v", numValNS("1", "1"))
		})},
		{"UpdateResultOverflow", update(func(in *dynamodb.UpdateItemInput) {
			in.UpdateExpression, in.ExpressionAttributeValues = aws.String("SET x = :v + :v"), vals(":v", fzN("9e125"))
		})},
		{"UpdateResultPrecision", update(func(in *dynamodb.UpdateItemInput) {
			in.UpdateExpression = aws.String("SET x = :v + :w")
			in.ExpressionAttributeValues = vals(":v", fzN("12345678901234567890123456789012345678"), ":w", fzN("0.1"))
		})},
		{"UpdateResultUnderflow", update(func(in *dynamodb.UpdateItemInput) {
			in.UpdateExpression, in.ExpressionAttributeValues = aws.String("SET x = :v - :w"), vals(":v", fzN("1e-130"), ":w", fzN("1e-131"))
		})},

		// DeleteItem
		{"DeleteKeyNumberBeforeShape", del(func(in *dynamodb.DeleteItemInput) { in.Key = numAV{"pk": fzS(""), "sk": bad} })},
		{"DeleteEmptyKeyBeforeValues", del(func(in *dynamodb.DeleteItemInput) {
			in.Key, in.ConditionExpression, in.ExpressionAttributeValues = numAV{"pk": fzS(""), "sk": fzN("1")}, cond, vals(":v", bad)
		})},
		{"DeleteMissingKeyBeforeValues", del(func(in *dynamodb.DeleteItemInput) {
			in.Key, in.ConditionExpression, in.ExpressionAttributeValues = numAV{"pk": fzS("a")}, cond, vals(":v", bad)
		})},
		{"DeleteKeyTypeBeforeValues", del(func(in *dynamodb.DeleteItemInput) {
			in.Key, in.ConditionExpression, in.ExpressionAttributeValues = numAV{"pk": fzN("1"), "sk": fzN("1")}, cond, vals(":v", bad)
		})},
		{"DeleteValuesBeforeUndeclaredName", del(func(in *dynamodb.DeleteItemInput) {
			in.ConditionExpression, in.ExpressionAttributeValues = aws.String("#n = :v"), vals(":v", bad)
		})},
		{"DeleteReturnValuesOnConditionCheckFailureFirst", del(func(in *dynamodb.DeleteItemInput) {
			in.ConditionExpression, in.ExpressionAttributeValues = cond, vals(":v", bad)
			in.ReturnValuesOnConditionCheckFailure = "BOGUS"
		})},

		// Query and Scan
		{"QueryValuesBeforeUndeclaredName", query(func(in *dynamodb.QueryInput) {
			in.KeyConditionExpression, in.FilterExpression = aws.String("pk = :p"), aws.String("#n = :v")
			in.ExpressionAttributeValues = vals(":p", fzS("a"), ":v", bad)
		})},
		{"QueryValuesBeforeKeyCondition", query(func(in *dynamodb.QueryInput) {
			in.KeyConditionExpression, in.FilterExpression = aws.String("pk > :p"), cond
			in.ExpressionAttributeValues = vals(":p", fzS("a"), ":v", bad)
		})},
		{"QueryValuesBeforeSelect", query(func(in *dynamodb.QueryInput) {
			in.Select, in.KeyConditionExpression = "BOGUS", aws.String("pk = :p AND sk = :v")
			in.ExpressionAttributeValues = vals(":p", fzS("a"), ":v", bad)
		})},
		{"QueryIndexFirst", query(func(in *dynamodb.QueryInput) {
			in.IndexName, in.KeyConditionExpression = aws.String("nope"), aws.String("pk = :p AND sk = :v")
			in.ExpressionAttributeValues = vals(":p", fzS("a"), ":v", bad)
		})},
		{"QueryValuesBeforeStartKey", query(func(in *dynamodb.QueryInput) {
			in.KeyConditionExpression = aws.String("pk = :p AND sk = :v")
			in.ExpressionAttributeValues = vals(":p", fzS("a"), ":v", fzN("1e126"))
			in.ExclusiveStartKey = numValKey("abc")
		})},
		{"QueryHashKeyNumber", query(func(in *dynamodb.QueryInput) {
			in.KeyConditionExpression, in.ExpressionAttributeValues = aws.String("pk = :p"), vals(":p", bad)
		})},
		{"QueryGsiNumber", query(func(in *dynamodb.QueryInput) {
			in.IndexName, in.KeyConditionExpression = aws.String(fuzzGSI), aws.String("g = :p AND r = :v")
			in.ExpressionAttributeValues = vals(":p", fzS("a"), ":v", bad)
		})},
		{"ScanValuesBeforeUndeclaredName", scan(func(in *dynamodb.ScanInput) {
			in.FilterExpression, in.ExpressionAttributeValues = aws.String("#n = :v"), vals(":v", bad)
		})},
		{"ScanValuesBeforeStartKey", scan(func(in *dynamodb.ScanInput) {
			in.FilterExpression, in.ExpressionAttributeValues = cond, vals(":v", fzN("1e126"))
			in.ExclusiveStartKey = numValKey("abc")
		})},
		{"ScanUnusedValuesBeforeStartKey", scan(func(in *dynamodb.ScanInput) {
			in.ExpressionAttributeValues, in.ExclusiveStartKey = vals(":v", fzN("1")), numValKey("abc")
		})},

		// Batch and transactional writes: a number anywhere in the request comes first.
		{"BatchNumberAfterEmptyKey", func(env numValEnv) error {
			_, err := env.client.BatchWriteItem(env.ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
				fuzzTable: {
					{PutRequest: &dynamodbtypes.PutRequest{Item: numAV{"pk": fzS(""), "sk": fzN("4")}}},
					{PutRequest: &dynamodbtypes.PutRequest{Item: item("3", "x", bad)}},
				},
			}})

			return err
		}},
		{"BatchNumberAfterValidRequest", func(env numValEnv) error {
			_, err := env.client.BatchWriteItem(env.ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
				fuzzTable: {
					{PutRequest: &dynamodbtypes.PutRequest{Item: item("4")}},
					{PutRequest: &dynamodbtypes.PutRequest{Item: item("3", "x", bad)}},
					{PutRequest: &dynamodbtypes.PutRequest{Item: numAV{"pk": fzS(""), "sk": fzN("5")}}},
				},
			}})

			return err
		}},
		{"TransactNumberAfterValidItem", transact(txPut(item("2"), nil), txPut(item("3", "x", bad), nil))},
		{"TransactNumberAfterEmptyKey", transact(txPut(numAV{"pk": fzS(""), "sk": fzN("2")}, nil), txPut(item("3", "x", bad), nil))},
		{"TransactNumberBeforeDuplicateTarget", transact(txPut(item("3", "x", bad), nil), txPut(item("3"), nil))},
		{"TransactNumberInSecondOperation", transact(txPut(item("3"), nil), txPut(item("3", "x", bad), nil))},
		{"TransactNumberBeforeReturnValuesOnConditionCheckFailure", transact(txPut(item("3", "x", bad), func(p *dynamodbtypes.Put) {
			p.ReturnValuesOnConditionCheckFailure = "BOGUS"
		}))},
		{"TransactValuesBeforeItem", transact(txPut(item("3", "x", fzN("1e126")), func(p *dynamodbtypes.Put) {
			p.ConditionExpression, p.ExpressionAttributeValues = cond, vals(":v", bad)
		}))},
		{"TransactFirstOperationFirst", transact(
			txPut(item("3", "x", fzN("1e126")), nil),
			txPut(item("2"), func(p *dynamodbtypes.Put) { p.ConditionExpression, p.ExpressionAttributeValues = cond, vals(":v", bad) }),
		)},
		{"TransactValuesBeforeKeyShape", transact(dynamodbtypes.TransactWriteItem{Update: &dynamodbtypes.Update{
			TableName: table, Key: numAV{"pk": fzS("a")}, UpdateExpression: aws.String("SET x = :v"), ExpressionAttributeValues: vals(":v", bad),
		}})},
		{"TransactDeleteKeyNumberBeforeShape", transact(dynamodbtypes.TransactWriteItem{Delete: &dynamodbtypes.Delete{
			TableName: table, Key: numAV{"pk": fzS(""), "sk": bad},
		}})},
		{"TransactConditionCheckValuesBeforeKeyShape", transact(dynamodbtypes.TransactWriteItem{ConditionCheck: &dynamodbtypes.ConditionCheck{
			TableName: table, Key: numAV{"pk": fzS("")}, ConditionExpression: cond, ExpressionAttributeValues: vals(":v", fzN("1e126")),
		}})},

		// Reads: the number of a key comes before its shape and, for a GetItem, after its projection.
		{"GetKeyNumberBeforeShape", func(env numValEnv) error {
			_, err := env.client.GetItem(env.ctx, &dynamodb.GetItemInput{TableName: table, Key: numAV{"pk": fzS(""), "sk": bad}})

			return err
		}},
		{"GetProjectionBeforeKeyNumber", func(env numValEnv) error {
			_, err := env.client.GetItem(env.ctx, &dynamodb.GetItemInput{
				TableName: table, Key: numAV{"pk": fzS("a"), "sk": bad}, ProjectionExpression: aws.String("#n"),
			})

			return err
		}},
		{"BatchGetKeyNumberBeforeShape", func(env numValEnv) error {
			_, err := env.client.BatchGetItem(env.ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
				fuzzTable: {Keys: []numAV{{"pk": fzS("a"), "sk": bad, "z": fzS("q")}}},
			}})

			return err
		}},
	}
}

func TestE2E_NumberValidationOrder(t *testing.T) {
	cases := numValOrderCases()

	RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
		t.Helper()

		env := numValSetup(t, client)
		out := make([]string, 0, len(cases))

		for _, c := range cases {
			out = append(out, c.name+" => "+numValResult(c.run(env)))

			// A case that is accepted must not change what the next one sees.
			_, err := client.DeleteItem(env.ctx, &dynamodb.DeleteItemInput{TableName: aws.String(fuzzTable), Key: numValKey("2")})
			require.NoError(t, err)

			_, err = client.PutItem(env.ctx, &dynamodb.PutItemInput{TableName: aws.String(fuzzTable), Item: numValKey("1")})
			require.NoError(t, err)
		}

		return out
	})
}
