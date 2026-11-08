package e2e

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Typed key parity (#147, #148, #149): the same values on the table, a GSI and an LSI must
// sort by type (numbers by value, binary as unsigned bytes) and never collide, wherever a
// "." or an equal number spelled differently appears in a key.

const (
	parityTypedTable = "typed_keys"
	parityTypedGSI   = "by-rk2"
	parityTypedLSI   = "by-lk"
)

// Numbers across the whole range DynamoDB accepts: negatives, decimals, exponents and 38
// significant digits, so neither text order nor float64 order gives the right answer.
var parityWideNumberValues = []parityRangeValue{
	parityN("10"), parityN("2"), parityN("-5"), parityN("100"), parityN("0"), parityN("1.5"),
	parityN("-10.25"), parityN("9"), parityN("1E3"), parityN("0.001"), parityN("-0.5"),
	parityN("99999999999"), parityN("20"), parityN("-1E10"), parityN("1E-10"), parityN("-1E-10"),
	parityN("99999999999999999999999999999999999999"), parityN("-99999999999999999999999999999999999999"),
	parityN("1E125"), parityN("-1E125"), parityN("1E-130"), parityN("-1E-130"),
	parityN("1.2345678901234567890123456789012345678"), parityN("1.2345678901234567890123456789012345679"),
	parityN("0.1"), parityN("0.10000000000000001"),
}

var parityWideBinaryValues = []parityRangeValue{
	parityB("00", 0x00), parityB("0000", 0x00, 0x00), parityB("0001", 0x00, 0x01), parityB("01", 0x01),
	parityB("0100", 0x01, 0x00), parityB("0102", 0x01, 0x02), parityB("02", 0x02), parityB("0a", 0x0a),
	parityB("10", 0x10), parityB("7f", 0x7f), parityB("80", 0x80), parityB("fe", 0xfe), parityB("ff", 0xff),
	parityB("ff00", 0xff, 0x00), parityB("ffff", 0xff, 0xff),
}

func parityTypedCreate(
	ctx context.Context,
	t *testing.T,
	client *dynamodb.Client,
	hashType, rangeType dynamodbtypes.ScalarAttributeType,
) {
	t.Helper()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(parityTypedTable),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
			{AttributeName: aws.String("rk"), KeyType: dynamodbtypes.KeyTypeRange},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: hashType},
			{AttributeName: aws.String("rk"), AttributeType: rangeType},
			{AttributeName: aws.String("grp"), AttributeType: hashType},
			{AttributeName: aws.String("rk2"), AttributeType: rangeType},
			{AttributeName: aws.String("lk"), AttributeType: rangeType},
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName: aws.String(parityTypedGSI),
			KeySchema: []dynamodbtypes.KeySchemaElement{
				{AttributeName: aws.String("grp"), KeyType: dynamodbtypes.KeyTypeHash},
				{AttributeName: aws.String("rk2"), KeyType: dynamodbtypes.KeyTypeRange},
			},
			Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
		}},
		LocalSecondaryIndexes: []dynamodbtypes.LocalSecondaryIndex{{
			IndexName: aws.String(parityTypedLSI),
			KeySchema: []dynamodbtypes.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
				{AttributeName: aws.String("lk"), KeyType: dynamodbtypes.KeyTypeRange},
			},
			Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
		}},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)
}

func parityTypedPut(ctx context.Context, t *testing.T, client *dynamodb.Client, item map[string]dynamodbtypes.AttributeValue) {
	t.Helper()

	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(parityTypedTable), Item: item})
	require.NoError(t, err)
}

// parityTypedOrders queries one partition on the table, the GSI and the LSI, in both
// directions, in full and in pages of 3, and returns the labels of each run.
func parityTypedOrders(ctx context.Context, t *testing.T, client *dynamodb.Client, hash, grp dynamodbtypes.AttributeValue) [][]string {
	t.Helper()

	var out [][]string

	for _, target := range []struct {
		index, cond string
		value       dynamodbtypes.AttributeValue
		placeholder string
	}{
		{"", "pk = :h", hash, ":h"},
		{parityTypedGSI, "grp = :h", grp, ":h"},
		{parityTypedLSI, "pk = :h", hash, ":h"},
	} {
		for _, forward := range []bool{true, false} {
			for _, limit := range []int32{0, 3} {
				input := &dynamodb.QueryInput{
					TableName:                 aws.String(parityTypedTable),
					KeyConditionExpression:    aws.String(target.cond),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{target.placeholder: target.value},
					ScanIndexForward:          aws.Bool(forward),
				}
				if target.index != "" {
					input.IndexName = aws.String(target.index)
				}

				if limit > 0 {
					input.Limit = aws.Int32(limit)
				}

				out = append(out, parityQueryLabels(ctx, t, client, input))
			}
		}
	}

	return out
}

func parityTypedSeedRange(ctx context.Context, t *testing.T, client *dynamodb.Client, vals []parityRangeValue) {
	t.Helper()

	for _, v := range vals {
		parityTypedPut(ctx, t, client, map[string]dynamodbtypes.AttributeValue{
			"pk":    &dynamodbtypes.AttributeValueMemberS{Value: "series"},
			"rk":    v.value,
			"grp":   &dynamodbtypes.AttributeValueMemberS{Value: "all"},
			"rk2":   v.value,
			"lk":    v.value,
			"label": &dynamodbtypes.AttributeValueMemberS{Value: v.label},
		})
	}
}

func TestE2E_TypedKeys(t *testing.T) {
	s := dynamodbtypes.ScalarAttributeTypeS
	n := dynamodbtypes.ScalarAttributeTypeN

	str := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberS{Value: v} }
	num := func(v string) dynamodbtypes.AttributeValue { return &dynamodbtypes.AttributeValueMemberN{Value: v} }

	t.Run("NumberRangeOrderTableGSIAndLSI", func(t *testing.T) {
		RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
			t.Helper()
			ctx := context.Background()

			parityTypedCreate(ctx, t, client, s, n)
			parityTypedSeedRange(ctx, t, client, parityWideNumberValues)

			return parityTypedOrders(ctx, t, client, str("series"), str("all"))
		})
	})

	t.Run("BinaryRangeOrderTableGSIAndLSI", func(t *testing.T) {
		RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
			t.Helper()
			ctx := context.Background()

			parityTypedCreate(ctx, t, client, s, dynamodbtypes.ScalarAttributeTypeB)
			parityTypedSeedRange(ctx, t, client, parityWideBinaryValues)

			return parityTypedOrders(ctx, t, client, str("series"), str("all"))
		})
	})

	t.Run("NumberRangeConditionsOnLSI", func(t *testing.T) {
		RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
			t.Helper()
			ctx := context.Background()

			parityTypedCreate(ctx, t, client, s, n)
			parityTypedSeedRange(ctx, t, client, parityWideNumberValues)

			var out [][]string

			for _, c := range []struct {
				cond     string
				a, b     string
				forward  bool
				hasUpper bool
			}{
				{"pk = :h AND lk BETWEEN :a AND :b", "-1", "10", true, true},
				{"pk = :h AND lk > :a", "9", "", true, false},
				{"pk = :h AND lk <= :a", "0.5", "", false, false},
				{"pk = :h AND lk = :a", "1000", "", true, false},
				{"pk = :h AND lk >= :a", "1E-130", "", false, false},
			} {
				values := map[string]dynamodbtypes.AttributeValue{":h": str("series"), ":a": num(c.a)}
				if c.hasUpper {
					values[":b"] = num(c.b)
				}

				out = append(out, parityQueryLabels(ctx, t, client, &dynamodb.QueryInput{
					TableName:                 aws.String(parityTypedTable),
					IndexName:                 aws.String(parityTypedLSI),
					KeyConditionExpression:    aws.String(c.cond),
					ExpressionAttributeValues: values,
					ScanIndexForward:          aws.Bool(c.forward),
				}))
			}

			return out
		})
	})

	// "7", "7.0", "07" and "7e0" are the same number, so one primary key. Values are never
	// returned, only labels, because each engine may canonicalize the stored text.
	t.Run("EqualNumberSpellingsAreOneKey", func(t *testing.T) {
		RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
			t.Helper()
			ctx := context.Background()

			parityTypedCreate(ctx, t, client, n, n)

			for i, spelling := range []string{"7", "7.0", "07", "7e0"} {
				parityTypedPut(ctx, t, client, map[string]dynamodbtypes.AttributeValue{
					"pk":    num(spelling),
					"rk":    num(spelling),
					"grp":   num(spelling),
					"rk2":   num(spelling),
					"lk":    num(spelling),
					"label": str("put" + string(rune('0'+i))),
				})
			}

			scan, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(parityTypedTable)})
			require.NoError(t, err)

			var out []any

			out = append(out, scan.Count)

			for _, spelling := range []string{"7", "7.00", "0.7E1"} {
				get, gerr := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(parityTypedTable),
					Key:       map[string]dynamodbtypes.AttributeValue{"pk": num(spelling), "rk": num(spelling)},
				})
				require.NoError(t, gerr)

				out = append(out, get.Item["label"].(*dynamodbtypes.AttributeValueMemberS).Value)
			}

			out = append(out, parityTypedOrders(ctx, t, client, num("7.0"), num("07")))

			_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(parityTypedTable),
				Key:       map[string]dynamodbtypes.AttributeValue{"pk": num("7e0"), "rk": num("7.000")},
			})
			require.NoError(t, err)

			scan, err = client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(parityTypedTable)})
			require.NoError(t, err)

			return append(out, scan.Count)
		})
	})

	t.Run("SeparatorInKeysOnTableGSIAndLSI", func(t *testing.T) {
		RunE2E(t, func(t *testing.T, client *dynamodb.Client) any {
			t.Helper()
			ctx := context.Background()

			parityTypedCreate(ctx, t, client, s, s)

			// (pk, rk), (grp, rk2) and (pk, lk) pairs that join to the same text with ".".
			rows := [][5]string{
				{"a", "b.c", "g", "h.i", "x.y"},
				{"a.b", "c", "g.h", "i", "y"},
				{"a", "a", "g", "i", "x"},
				{"a", "b", "g.h", "j", "x.z"},
				{"a.b.c", "d", "g", "h", "z"},
			}

			for i, r := range rows {
				item := map[string]dynamodbtypes.AttributeValue{
					"pk": str(r[0]), "rk": str(r[1]), "grp": str(r[2]), "rk2": str(r[3]), "lk": str(r[4]),
					"label": str("row" + string(rune('0'+i))),
				}
				parityTypedPut(ctx, t, client, item)
			}

			scan, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(parityTypedTable)})
			require.NoError(t, err)

			out := []any{scan.Count}
			for _, pair := range [][2]string{{"a", "g"}, {"a.b", "g.h"}, {"a.b.c", "g"}} {
				out = append(out, parityTypedOrders(ctx, t, client, str(pair[0]), str(pair[1])))
			}

			return out
		})
	})
}
