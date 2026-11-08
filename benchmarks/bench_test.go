// Package benchmarks measures minidyn's per-operation cost as tables grow, through the
// in-process aws-v2 client (what most users run in their tests).
//
// Every benchmark runs at several table sizes with a fixed number of items per partition,
// so the cost of a point operation (GetItem, PutItem, UpdateItem, DeleteItem) and of a
// one-partition Query should stay roughly flat as the table grows. A cost that grows with
// the table size is a performance bug: TestScaling turns that into a failing check.
//
//	go test ./benchmarks -bench . -benchmem                  # all benchmarks
//	go test ./benchmarks -bench 'GetItem|Query' -benchmem    # a subset
//	MINIDYN_BENCH_SCALING=1 go test ./benchmarks -run TestScaling -v
package benchmarks

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/truora/minidyn/aws-v2/client"
)

const (
	benchTable = "bench"
	benchGSI   = "by-g"

	// itemsPerPartition is constant across sizes, so a one-partition Query returns the same
	// number of items whatever the table size.
	itemsPerPartition = 10
	// itemsPerGroup is the number of items sharing a GSI hash key.
	itemsPerGroup = 10
)

var benchSizes = []int{100, 1_000, 10_000}

func s(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func n(v int) types.AttributeValue    { return &types.AttributeValueMemberN{Value: strconv.Itoa(v)} }

func pkOf(i int) string { return fmt.Sprintf("p%06d", i/itemsPerPartition) }
func skOf(i int) string { return fmt.Sprintf("s%03d", i%itemsPerPartition) }
func gOf(i int) string  { return fmt.Sprintf("g%06d", i/itemsPerGroup) }

func key(i int) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": s(pkOf(i)), "sk": s(skOf(i))}
}

func item(i int) map[string]types.AttributeValue {
	it := key(i)
	it["g"] = s(gOf(i))
	it["n"] = n(i)
	it["status"] = s([]string{"active", "inactive"}[i%2])
	it["tags"] = &types.AttributeValueMemberSS{Value: []string{"a", "b"}}
	it["info"] = &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
		"name": s("item " + strconv.Itoa(i)),
		"size": n(i % 7),
	}}

	return it
}

// newTable returns a client with a table of size items: pk/sk keys and a GSI on g/sk.
func newTable(tb testing.TB, size int) *client.Client {
	tb.Helper()

	ctx := context.Background()
	c := client.NewClient()

	_, err := c.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(benchTable),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("g"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String(benchGSI),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("g"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		tb.Fatal(err)
	}

	for i := range size {
		if _, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(benchTable), Item: item(i)}); err != nil {
			tb.Fatal(err)
		}
	}

	return c
}

// spread maps iteration i to an item index spread evenly over the table (a prime stride),
// so a benchmark doesn't only touch the first partitions, which the table walk reaches first.
func spread(i, size int) int { return (i * 7919) % size }

// benchOp is one operation to measure; run performs it for iteration i on a table of size.
type benchOp struct {
	name string
	run  func(ctx context.Context, c *client.Client, size, i int) error
}

// benchOps are the measured operations. Keep cheap, per-item operations first: they are the
// ones TestScaling expects to stay flat.
var benchOps = []benchOp{
	{"GetItem", func(ctx context.Context, c *client.Client, size, i int) error {
		_, err := c.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(benchTable), Key: key(spread(i, size))})

		return err
	}},
	{"PutItemOverwrite", func(ctx context.Context, c *client.Client, size, i int) error {
		_, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(benchTable), Item: item(spread(i, size))})

		return err
	}},
	{"PutItemNewThenDelete", func(ctx context.Context, c *client.Client, size, i int) error {
		// Insert a new key and delete it, so the table size stays constant.
		it := item(size + i%itemsPerPartition)
		if _, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(benchTable), Item: it}); err != nil {
			return err
		}

		_, err := c.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(benchTable),
			Key:       map[string]types.AttributeValue{"pk": it["pk"], "sk": it["sk"]},
		})

		return err
	}},
	{"UpdateItemCounter", func(ctx context.Context, c *client.Client, size, i int) error {
		_, err := c.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 aws.String(benchTable),
			Key:                       key(spread(i, size)),
			UpdateExpression:          aws.String("SET n = n + :one"),
			ConditionExpression:       aws.String("attribute_exists(pk)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":one": n(1)},
		})

		return err
	}},
	{"UpdateItemNamedCondition", func(ctx context.Context, c *client.Client, size, i int) error {
		// Update and condition expressions with #names and :values, all validated up front.
		_, err := c.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                aws.String(benchTable),
			Key:                      key(spread(i, size)),
			UpdateExpression:         aws.String("SET #n = #n + :one, #st = :st"),
			ConditionExpression:      aws.String("attribute_exists(#pk) AND #n >= :zero"),
			ExpressionAttributeNames: map[string]string{"#n": "n", "#st": "status", "#pk": "pk"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":one": n(1), ":st": s("active"), ":zero": n(0),
			},
		})

		return err
	}},
	{"UpdateItemIndexKey", func(ctx context.Context, c *client.Client, size, i int) error {
		// Moves an item between GSI groups, which re-sorts the index.
		_, err := c.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 aws.String(benchTable),
			Key:                       key(spread(i, size)),
			UpdateExpression:          aws.String("SET g = :g"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":g": s(gOf(spread(i+1, size)))},
		})

		return err
	}},
	{"QueryPartition", func(ctx context.Context, c *client.Client, size, i int) error {
		_, err := c.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(benchTable),
			KeyConditionExpression:    aws.String("pk = :p"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":p": s(pkOf(spread(i, size)))},
		})

		return err
	}},
	{"QueryRangeLimit", func(ctx context.Context, c *client.Client, size, i int) error {
		_, err := c.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(benchTable),
			KeyConditionExpression: aws.String("pk = :p AND sk > :s"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":p": s(pkOf(spread(i, size))), ":s": s(skOf(2)),
			},
			Limit: aws.Int32(3),
		})

		return err
	}},
	{"QueryWithFilter", func(ctx context.Context, c *client.Client, size, i int) error {
		_, err := c.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(benchTable),
			KeyConditionExpression:    aws.String("pk = :p"),
			FilterExpression:          aws.String("#st = :st AND info.size > :z"),
			ExpressionAttributeNames:  map[string]string{"#st": "status"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":p": s(pkOf(spread(i, size))), ":st": s("active"), ":z": n(2)},
		})

		return err
	}},
	{"QueryGSI", func(ctx context.Context, c *client.Client, size, i int) error {
		_, err := c.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(benchTable),
			IndexName:                 aws.String(benchGSI),
			KeyConditionExpression:    aws.String("g = :g"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":g": s(gOf(spread(i, size)))},
		})

		return err
	}},
	{"QueryGSIAfterWrite", func(ctx context.Context, c *client.Client, size, i int) error {
		// A write between GSI queries: the sorted copy of the index entries is rebuilt on the
		// next query (P3), so this measures a write-heavy workload's query cost.
		if _, err := c.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(benchTable), Item: item(spread(i, size))}); err != nil {
			return err
		}

		_, err := c.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(benchTable),
			IndexName:                 aws.String(benchGSI),
			KeyConditionExpression:    aws.String("g = :g"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":g": s(gOf(spread(i, size)))},
		})

		return err
	}},
	{"BatchGetItem25", func(ctx context.Context, c *client.Client, size, i int) error {
		keys := make([]map[string]types.AttributeValue, 25)
		for j := range keys {
			keys[j] = key(spread(i*25+j, size))
		}

		_, err := c.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{benchTable: {Keys: keys}},
		})

		return err
	}},
	{"TransactWritePutUpdate", func(ctx context.Context, c *client.Client, size, i int) error {
		_, err := c.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{Put: &types.Put{TableName: aws.String(benchTable), Item: item(spread(i, size))}},
				{Update: &types.Update{
					TableName:                 aws.String(benchTable),
					Key:                       key(spread(i+1, size)),
					UpdateExpression:          aws.String("SET n = n + :one"),
					ExpressionAttributeValues: map[string]types.AttributeValue{":one": n(1)},
				}},
			},
		})

		return err
	}},
	{"ScanFilterFullTable", func(ctx context.Context, c *client.Client, _, _ int) error {
		// Linear in the table size by design; reported per item by the scaling check.
		_, err := c.Scan(ctx, &dynamodb.ScanInput{
			TableName:                 aws.String(benchTable),
			FilterExpression:          aws.String("#st = :st"),
			ExpressionAttributeNames:  map[string]string{"#st": "status"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":st": s("active")},
		})

		return err
	}},
}

func BenchmarkOps(b *testing.B) {
	for _, op := range benchOps {
		b.Run(op.name, func(b *testing.B) {
			for _, size := range benchSizes {
				b.Run("items="+strconv.Itoa(size), func(b *testing.B) {
					runBench(b, op, size)
				})
			}
		})
	}
}

func runBench(b *testing.B, op benchOp, size int) {
	b.Helper()

	c := newTable(b, size)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		if err := op.run(ctx, c, size, i); err != nil {
			b.Fatal(err)
		}
	}
}
