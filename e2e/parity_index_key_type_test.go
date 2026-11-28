package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Index key attributes holding a value of another type than the one the index declares
// (#213). They use the tables of parity_empty_key_values_test.go. The Put_ cases at the end pin
// the order of the checks: the primary key, then the local indexes, then the global ones, the hash
// key before the range key, an empty value and a wrong type on an attribute in one pass.

type ikCase struct {
	name       string
	knownIssue string
	fn         func(ctx context.Context, c *dynamodb.Client) error
}

func indexKeyTypeCases() []ikCase {
	tbl := aws.String(emptyKeyTable)
	wrong := map[string]dynamodbtypes.AttributeValue{
		"NULL": &dynamodbtypes.AttributeValueMemberNULL{Value: true},
		"N":    voN("1"),
		"B":    ekB(1),
		"BOOL": &dynamodbtypes.AttributeValueMemberBOOL{Value: true},
		"L":    &dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{ekS("x")}},
		"M":    &dynamodbtypes.AttributeValueMemberM{Value: ekItem{"a": ekS("x")}},
		"SS":   &dynamodbtypes.AttributeValueMemberSS{Value: []string{"x"}},
		"NS":   &dynamodbtypes.AttributeValueMemberNS{Value: []string{"1"}},
		"BS":   &dynamodbtypes.AttributeValueMemberBS{Value: [][]byte{{1}}},
	}
	failing := aws.String("attribute_exists(zzz)")

	withKeys := func(extra ekItem) ekItem {
		item := ekItem{"pk": ekS("a"), "sk": ekS("2")}
		for k, v := range extra {
			item[k] = v
		}

		return item
	}
	okKey := ekItem{"pk": ekS("a"), "sk": ekS("1")}

	put := func(item ekItem, cond *string) func(ctx context.Context, c *dynamodb.Client) error {
		return func(ctx context.Context, c *dynamodb.Client) error {
			in := &dynamodb.PutItemInput{TableName: tbl, Item: item, ConditionExpression: cond}

			_, err := c.PutItem(ctx, in)

			return err
		}
	}
	update := func(expr string, v dynamodbtypes.AttributeValue, cond *string) func(ctx context.Context, c *dynamodb.Client) error {
		return func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: tbl, Key: okKey, UpdateExpression: aws.String(expr),
				ExpressionAttributeValues: ekItem{":v": v}, ConditionExpression: cond,
			})

			return err
		}
	}
	batch := func(item ekItem) func(ctx context.Context, c *dynamodb.Client) error {
		return func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{
				emptyKeyTable: {{PutRequest: &dynamodbtypes.PutRequest{Item: item}}},
			}})

			return err
		}
	}
	transactPut := func(item ekItem, cond *string) func(ctx context.Context, c *dynamodb.Client) error {
		return func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Put: &dynamodbtypes.Put{TableName: tbl, Item: item, ConditionExpression: cond}},
			}})

			return err
		}
	}
	transactUpdate := func(expr string, v dynamodbtypes.AttributeValue, cond *string) func(ctx context.Context, c *dynamodb.Client) error {
		return func(ctx context.Context, c *dynamodb.Client) error {
			_, err := c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
				{Update: &dynamodbtypes.Update{
					TableName: tbl, Key: okKey, UpdateExpression: aws.String(expr),
					ExpressionAttributeValues: ekItem{":v": v}, ConditionExpression: cond,
				}},
			}})

			return err
		}
	}

	var tests []ikCase

	for kind, v := range wrong {
		for _, attr := range []string{"g", "gs", "l"} {
			s := attr + "_" + kind

			// DynamoDB Local does not validate an N or B value an update writes to a local
			// index key (see TestE2E_IndexKeyType_LSIUpdateDivergence).
			if attr == "l" && (kind == "N" || kind == "B") {
				tests = append(tests,
					ikCase{name: "Put_" + s, fn: put(withKeys(ekItem{attr: v}), nil)},
					ikCase{name: "PutFailingCondition_" + s, fn: put(withKeys(ekItem{attr: v}), failing)},
					ikCase{name: "UpdateFailingCondition_" + s, fn: update("SET "+attr+" = :v", v, failing)},
					ikCase{name: "BatchWrite_" + s, fn: batch(withKeys(ekItem{attr: v}))},
					ikCase{name: "TransactPut_" + s, fn: transactPut(withKeys(ekItem{attr: v}), nil)},
					ikCase{name: "TransactPutFailingCondition_" + s, fn: transactPut(withKeys(ekItem{attr: v}), failing)},
					ikCase{name: "TransactUpdateFailingCondition_" + s, fn: transactUpdate("SET "+attr+" = :v", v, failing)},
				)

				continue
			}

			tests = append(tests,
				ikCase{name: "Put_" + s, fn: put(withKeys(ekItem{attr: v}), nil)},
				ikCase{name: "PutFailingCondition_" + s, fn: put(withKeys(ekItem{attr: v}), failing)},
				ikCase{name: "Update_" + s, fn: update("SET "+attr+" = :v", v, nil)},
				ikCase{name: "UpdateFailingCondition_" + s, fn: update("SET "+attr+" = :v", v, failing)},
				ikCase{name: "BatchWrite_" + s, fn: batch(withKeys(ekItem{attr: v}))},
				ikCase{name: "TransactPut_" + s, fn: transactPut(withKeys(ekItem{attr: v}), nil)},
				ikCase{name: "TransactPutFailingCondition_" + s, fn: transactPut(withKeys(ekItem{attr: v}), failing)},
				ikCase{name: "TransactUpdate_" + s, fn: transactUpdate("SET "+attr+" = :v", v, nil)},
				ikCase{name: "TransactUpdateFailingCondition_" + s, fn: transactUpdate("SET "+attr+" = :v", v, failing)},
			)
		}
	}

	n, num := wrong["NULL"], wrong["N"]
	tests = append(tests,
		ikCase{name: "Put_LSIAndGSI", fn: put(withKeys(ekItem{"g": num, "l": num}), nil)},
		ikCase{name: "Put_GSIHashAndRange", fn: put(withKeys(ekItem{"g": num, "gs": n}), nil)},
		ikCase{name: "Put_WrongThenEmptyOtherIndex", fn: put(withKeys(ekItem{"g": num, "l": ekS("")}), nil)},
		ikCase{name: "Put_EmptyThenWrongOtherIndex", fn: put(withKeys(ekItem{"g": ekS(""), "l": num}), nil)},
		ikCase{name: "Put_WrongRangeEmptyHashSameIndex", fn: put(withKeys(ekItem{"g": ekS(""), "gs": num}), nil)},
		ikCase{name: "Put_WrongHashEmptyRangeSameIndex", fn: put(withKeys(ekItem{"g": num, "gs": ekS("")}), nil)},
		ikCase{name: "Put_WrongPrimaryKey", fn: put(ekItem{"pk": ekS("a"), "sk": num, "g": num}, nil)},
		ikCase{name: "Put_WrongPrimaryAndEmptyIndex", fn: put(ekItem{"pk": ekS("a"), "sk": num, "g": ekS("")}, nil)},
		ikCase{name: "Put_WrongPrimaryAndNullIndex", fn: put(ekItem{"pk": ekS("a"), "sk": num, "g": n}, nil)},
		ikCase{name: "Put_WrongIndexAndBadNumber", fn: put(withKeys(ekItem{"g": num, "n": voN("1e1000")}), nil)},
		ikCase{name: "Put_CorrectTypes", fn: put(withKeys(ekItem{"g": ekS("g"), "gs": ekS("s"), "l": ekS("l")}), nil)},
		ikCase{name: "Put_NonKeyAttrAnyType", fn: put(withKeys(ekItem{"n": n}), nil)},
	)

	return tests
}

func TestE2E_IndexKeyType(t *testing.T) {
	for _, tt := range indexKeyTypeCases() {
		t.Run(tt.name, func(t *testing.T) { RunE2EKnown(t, tt.knownIssue, ekRun(tt.fn)) })
	}
}

// Intentional divergence (#213): DynamoDB Local accepts an UpdateItem that sets a local
// secondary index key to an N or B value under an S key (the item is stored with it), while it
// rejects the same value in a PutItem, in a global index key, and any other type in a local one.
// minidyn rejects it like those, as DynamoDB does: the developer guide says an index key
// attribute must be of the type the index declares. See "Intentional divergences" in
// e2e/README.md.
func TestE2E_IndexKeyType_LSIUpdateDivergence(t *testing.T) {
	const rejected = "ValidationException: One or more parameter values were invalid: Type mismatch for Index Key"

	const (
		minidyn = "minidyn"
		local   = "dynamodb-local"
	)

	want := map[string]string{minidyn: rejected, local: "ok"}

	for engine, endpoint := range map[string]func(*testing.T) string{
		minidyn: setupMinidynEndpoint,
		local:   setupDynamoDBLocalEndpoint,
	} {
		t.Run(engine, func(t *testing.T) {
			client := newSDKClient(t, endpoint(t))
			ctx := context.Background()

			ekCreateTables(ctx, t, client)

			for _, v := range []dynamodbtypes.AttributeValue{voN("1"), ekB(1)} {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String(emptyKeyTable), Key: ekItem{"pk": ekS("a"), "sk": ekS("1")},
					UpdateExpression: aws.String("SET l = :v"), ExpressionAttributeValues: ekItem{":v": v},
				})

				got := "ok"
				if err != nil {
					got = fuzzErrString(err)
				}

				if want[engine] == "ok" {
					require.Equal(t, "ok", got)
				} else {
					require.Contains(t, got, rejected[len("ValidationException: "):])
				}
			}
		})
	}
}
