package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// The order DynamoDB runs the checks of a request in (empty and mistyped key values, the
// bounds of a BETWEEN, the placeholders of the expressions), and a write an index rejects
// leaving nothing behind. They use the tables of parity_empty_key_values_test.go, plus
// "ekn" with pk S / sk N.

func voN(v string) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberN{Value: v}
}

func voRun(fn func(ctx context.Context, c *dynamodb.Client) string) func(t *testing.T, client *dynamodb.Client) string {
	return func(t *testing.T, client *dynamodb.Client) string {
		t.Helper()
		ctx := context.Background()

		ekCreateTables(ctx, t, client)

		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:   aws.String("ekn"),
			BillingMode: dynamodbtypes.BillingModePayPerRequest,
			KeySchema: []dynamodbtypes.KeySchemaElement{
				ekKey("pk", dynamodbtypes.KeyTypeHash), ekKey("sk", dynamodbtypes.KeyTypeRange),
			},
			AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
				ekAttr("pk", dynamodbtypes.ScalarAttributeTypeS), ekAttr("sk", dynamodbtypes.ScalarAttributeTypeN),
			},
		})
		require.NoError(t, err)

		return fn(ctx, client)
	}
}

// voState reads the stored item and what the by-g index holds for g1, to see a rejected
// write left both as they were.
func voState(ctx context.Context, c *dynamodb.Client) string {
	got, err := c.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(emptyKeyTable), Key: ekItem{"pk": ekS("a"), "sk": ekS("1")}, ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return "get: " + fuzzErrString(err)
	}

	q, err := c.Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String(emptyKeyTable), IndexName: aws.String("by-g"),
		KeyConditionExpression: aws.String("g = :g"), ExpressionAttributeValues: ekItem{":g": ekS("g1")},
	})
	if err != nil {
		return "query: " + fuzzErrString(err)
	}

	return fmt.Sprintf("g=%s n=%s index items=%d", parityAttrValueString(got.Item["g"]), parityAttrValueString(got.Item["n"]), len(q.Items))
}

func TestE2E_ValidationOrder_Writes(t *testing.T) {
	tbl := aws.String(emptyKeyTable)
	e := ekS("")
	okKey := ekItem{"pk": ekS("a"), "sk": ekS("1")}
	emptySK := ekItem{"pk": ekS("a"), "sk": e}
	noSK := ekItem{"pk": ekS("a")}

	put := func(table string, in ekItem, mod func(*dynamodb.PutItemInput)) func(ctx context.Context, c *dynamodb.Client) string {
		return func(ctx context.Context, c *dynamodb.Client) string {
			p := &dynamodb.PutItemInput{TableName: aws.String(table), Item: in}
			if mod != nil {
				mod(p)
			}

			_, err := c.PutItem(ctx, p)

			return fuzzErrString(err)
		}
	}
	update := func(key ekItem, expr string, vals ekItem, mod func(*dynamodb.UpdateItemInput)) func(ctx context.Context, c *dynamodb.Client) string {
		return func(ctx context.Context, c *dynamodb.Client) string {
			u := &dynamodb.UpdateItemInput{TableName: tbl, Key: key, UpdateExpression: aws.String(expr), ExpressionAttributeValues: vals}
			if mod != nil {
				mod(u)
			}

			_, err := c.UpdateItem(ctx, u)

			return fuzzErrString(err)
		}
	}
	del := func(key ekItem, mod func(*dynamodb.DeleteItemInput)) func(ctx context.Context, c *dynamodb.Client) string {
		return func(ctx context.Context, c *dynamodb.Client) string {
			d := &dynamodb.DeleteItemInput{TableName: tbl, Key: key}
			if mod != nil {
				mod(d)
			}

			_, err := c.DeleteItem(ctx, d)

			return fuzzErrString(err)
		}
	}
	vx := ekItem{":v": ekS("x")}
	rvoccf := dynamodbtypes.ReturnValuesOnConditionCheckFailure("BOGUS")
	unknownTableUpdate := func(mod func(*dynamodb.UpdateItemInput)) func(ctx context.Context, c *dynamodb.Client) string {
		return update(okKey, "SET n = :v", vx, func(u *dynamodb.UpdateItemInput) {
			u.TableName = aws.String("no-such-table")
			mod(u)
		})
	}
	badRV := func(u *dynamodb.UpdateItemInput) { u.ReturnValues = dynamodbtypes.ReturnValue("BOGUS") }
	badRVAndRVOCCF := func(u *dynamodb.UpdateItemInput) {
		u.ReturnValues = dynamodbtypes.ReturnValue("BOGUS")
		u.ReturnValuesOnConditionCheckFailure = rvoccf
	}

	tests := []struct {
		name string
		fn   func(ctx context.Context, c *dynamodb.Client) string
	}{
		// UpdateItem: ReturnValues, the key, RVOCCF, then the placeholders is what minidyn does.
		// Local reports RVOCCF after the unused names and values and the undefined placeholders
		// (#243), so no case here sends an invalid RVOCCF together with a placeholder problem.
		{"UpdateEmptyKeyUndefinedValue", update(emptySK, "SET n = :v", nil, nil)},
		{"UpdateEmptyKeyUndeclaredName", update(emptySK, "SET #n = :v", vx, nil)},
		{"UpdateEmptyKeySyntaxError", update(emptySK, "SET", nil, nil)},
		{"UpdateEmptyKeyUnusedValue", update(emptySK, "SET n = :v", ekItem{":v": ekS("x"), ":unused": ekS("y")}, nil)},
		{"UpdateEmptyKeyInvalidRVOCCF", update(emptySK, "SET n = :v", vx, func(u *dynamodb.UpdateItemInput) { u.ReturnValuesOnConditionCheckFailure = rvoccf })},
		{"UpdateMissingRangeKeyUndefinedValue", update(noSK, "SET n = :v", nil, nil)},
		{"UpdateMissingRangeKeyInvalidRVOCCF", update(noSK, "SET n = :v", vx, func(u *dynamodb.UpdateItemInput) { u.ReturnValuesOnConditionCheckFailure = rvoccf })},
		{"UpdateEmptyKeyTargetsKeyAndUndefinedValue", update(emptySK, "SET pk = :v", nil, nil)},
		{"UpdateValidKeyInvalidRVOCCF", update(okKey, "SET n = :v", vx, func(u *dynamodb.UpdateItemInput) { u.ReturnValuesOnConditionCheckFailure = rvoccf })},
		{"UpdateValidKeyInvalidReturnValues", update(okKey, "SET n = :v", vx, badRV)},
		{"UpdateEmptyKeyInvalidReturnValues", update(emptySK, "SET n = :v", vx, badRV)},
		{"UpdateMissingRangeKeyInvalidReturnValues", update(noSK, "SET n = :v", vx, badRV)},
		{"UpdateInvalidReturnValuesUndefinedValue", update(okKey, "SET n = :v", nil, badRV)},
		{"UpdateInvalidReturnValuesUndeclaredName", update(okKey, "SET #n = :v", vx, badRV)},
		{"UpdateInvalidReturnValuesSyntaxError", update(okKey, "SET", nil, badRV)},
		{"UpdateInvalidReturnValuesInvalidRVOCCF", update(okKey, "SET n = :v", vx, badRVAndRVOCCF)},
		{"UpdateInvalidReturnValuesEmptyKeyInvalidRVOCCF", update(emptySK, "SET n = :v", vx, badRVAndRVOCCF)},
		{"UpdateLowercaseReturnValues", update(okKey, "SET n = :v", vx, func(u *dynamodb.UpdateItemInput) { u.ReturnValues = dynamodbtypes.ReturnValue("all_new") })},
		{"UpdateInvalidReturnValuesUnusedValue", update(okKey, "SET n = :v", ekItem{":v": ekS("x"), ":unused": ekS("y")}, badRV)},
		{"UpdateInvalidReturnValuesUnknownTable", unknownTableUpdate(badRV)},
		// DeleteItem: the key, then the placeholders
		{"DeleteEmptyKeyUndefinedValue", del(emptySK, func(d *dynamodb.DeleteItemInput) { d.ConditionExpression = aws.String("n = :x") })},
		{"DeleteEmptyKeyUndeclaredName", del(emptySK, func(d *dynamodb.DeleteItemInput) { d.ConditionExpression = aws.String("attribute_exists(#n)") })},
		{"DeleteEmptyKeySyntaxError", del(emptySK, func(d *dynamodb.DeleteItemInput) { d.ConditionExpression = aws.String("attribute_exists(") })},
		{"DeleteEmptyKeyUnusedValue", del(emptySK, func(d *dynamodb.DeleteItemInput) { d.ExpressionAttributeValues = ekItem{":unused": ekS("y")} })},
		{"DeleteEmptyKeyInvalidRVOCCF", del(emptySK, func(d *dynamodb.DeleteItemInput) { d.ReturnValuesOnConditionCheckFailure = rvoccf })},
		{"DeleteEmptyKeyInvalidReturnValues", del(emptySK, func(d *dynamodb.DeleteItemInput) { d.ReturnValues = dynamodbtypes.ReturnValue("ALL_NEW") })},
		{"DeleteMissingRangeKeyUndefinedValue", del(noSK, func(d *dynamodb.DeleteItemInput) { d.ConditionExpression = aws.String("n = :x") })},
		// PutItem: the key values, then the placeholders
		{"PutEmptyKeyUndefinedValue", put(emptyKeyTable, emptySK, func(p *dynamodb.PutItemInput) { p.ConditionExpression = aws.String("n = :x") })},
		{"PutEmptyKeyUndeclaredName", put(emptyKeyTable, emptySK, func(p *dynamodb.PutItemInput) { p.ConditionExpression = aws.String("attribute_not_exists(#n)") })},
		{"PutEmptyGSIKeyUndefinedValue", put(emptyKeyTable, ekItem{"pk": ekS("a"), "sk": ekS("2"), "g": e}, func(p *dynamodb.PutItemInput) { p.ConditionExpression = aws.String("n = :x") })},
		// the index keys are reported local indexes first
		{"PutEmptyLSIKeyAndEmptyGSIKey", put(emptyKeyTable, ekItem{"pk": ekS("a"), "sk": ekS("2"), "g": e, "l": e}, nil)},
		{"PutEmptyTableKeyAndEmptyGSIKey", put(emptyKeyTable, ekItem{"pk": e, "sk": ekS("1"), "g": e}, nil)},
		{"PutEmptyGSIHashAndEmptyGSIRange", put(emptyKeyTable, ekItem{"pk": ekS("a"), "sk": ekS("2"), "g": e, "gs": e}, nil)},
		{"UpdateSetEmptyLSIKeyAndEmptyGSIKey", func(ctx context.Context, c *dynamodb.Client) string {
			return update(okKey, "SET g = :v, l = :v", ekItem{":v": e}, nil)(ctx, c)
		}},
		// a write an index rejects leaves the item and the index as they were
		{"PutGSIKeyWrongTypeThenState", func(ctx context.Context, c *dynamodb.Client) string {
			_ = put(emptyKeyTable, ekItem{"pk": ekS("a"), "sk": ekS("1"), "g": voN("5"), "n": ekS("changed")}, nil)(ctx, c)

			return voState(ctx, c)
		}},
		{"UpdateGSIKeyWrongTypeThenState", func(ctx context.Context, c *dynamodb.Client) string {
			_ = update(okKey, "SET g = :v, n = :w", ekItem{":v": voN("5"), ":w": ekS("changed")}, nil)(ctx, c)

			return voState(ctx, c)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { RunE2E(t, voRun(tt.fn)) })
	}
}

// A Query with an invalid Select and a problem in its key condition: DynamoDB reports the
// key condition first (empty value, type mismatch, reversed BETWEEN bounds, a structurally
// wrong condition), and the placeholders before both. (A syntax error in the key condition
// is not covered: minidyn reports it only while evaluating items.)
func TestE2E_ValidationOrder_QuerySelect(t *testing.T) {
	type queryFn = func(ctx context.Context, c *dynamodb.Client) string

	query := func(index, cond string, vals ekItem, mod func(*dynamodb.QueryInput)) queryFn {
		return func(ctx context.Context, c *dynamodb.Client) string {
			in := &dynamodb.QueryInput{
				TableName: aws.String(emptyKeyTable), KeyConditionExpression: aws.String(cond), ExpressionAttributeValues: vals,
			}
			if index != "" {
				in.IndexName = aws.String(index)
			}

			mod(in)

			out, err := c.Query(ctx, in)
			if err != nil {
				return fuzzErrString(err)
			}

			return fmt.Sprintf("ok items=%d", len(out.Items))
		}
	}
	sel := func(s dynamodbtypes.Select) func(*dynamodb.QueryInput) {
		return func(in *dynamodb.QueryInput) { in.Select = s }
	}
	between := "pk = :p AND sk BETWEEN :a AND :b"
	// Select values DynamoDB rejects for a Query of the table, one per kind of error.
	selects := []struct {
		name string
		mod  func(*dynamodb.QueryInput)
	}{
		{"AllProjectedOnTable", sel(dynamodbtypes.SelectAllProjectedAttributes)},
		{"SpecificWithoutProjection", sel(dynamodbtypes.SelectSpecificAttributes)},
		{"CountWithProjection", func(in *dynamodb.QueryInput) {
			in.Select = dynamodbtypes.SelectCount
			in.ProjectionExpression = aws.String("pk")
		}},
		{"Unknown", sel(dynamodbtypes.Select("BOGUS"))},
	}
	keyProblems := []struct {
		name  string
		index string
		cond  string
		vals  ekItem
	}{
		{"ValidKey", "", "pk = :p", ekItem{":p": ekS("a")}},
		{"EmptyHash", "", "pk = :p", ekItem{":p": ekS("")}},
		{"EmptyRange", "", "pk = :p AND sk = :s", ekItem{":p": ekS("a"), ":s": ekS("")}},
		{"HashTypeMismatch", "", "pk = :p", ekItem{":p": voN("1")}},
		{"RangeTypeMismatch", "", "pk = :p AND sk = :s", ekItem{":p": ekS("a"), ":s": voN("1")}},
		{"BetweenReversed", "", between, ekItem{":p": ekS("a"), ":a": ekS("z"), ":b": ekS("a")}},
		{"BetweenMixedTypes", "", between, ekItem{":p": ekS("a"), ":a": ekS("z"), ":b": voN("1")}},
		{"NotTheKeyAttribute", "", "g = :p", ekItem{":p": ekS("a")}},
		{"IndexEmptyHash", "by-g", "g = :p", ekItem{":p": ekS("")}},
		{"UndefinedValue", "", "pk = :p", nil},
		{"UndeclaredName", "", "#n = :p", ekItem{":p": ekS("a")}},
	}

	for _, s := range selects {
		for _, k := range keyProblems {
			fn := query(k.index, k.cond, k.vals, s.mod)

			t.Run(k.name+"_"+s.name, func(t *testing.T) { RunE2E(t, voRun(fn)) })
		}
	}
}

func TestE2E_ValidationOrder_BetweenBounds(t *testing.T) {
	e := ekS("")

	query := func(table, index, cond string, vals ekItem) func(ctx context.Context, c *dynamodb.Client) string {
		return func(ctx context.Context, c *dynamodb.Client) string {
			in := &dynamodb.QueryInput{
				TableName: aws.String(table), KeyConditionExpression: aws.String(cond), ExpressionAttributeValues: vals,
			}
			if index != "" {
				in.IndexName = aws.String(index)
			}

			out, err := c.Query(ctx, in)
			if err != nil {
				return fuzzErrString(err)
			}

			return fmt.Sprintf("ok items=%d", len(out.Items))
		}
	}
	between := "pk = :p AND sk BETWEEN :a AND :b"

	tests := []struct {
		name string
		fn   func(ctx context.Context, c *dynamodb.Client) string
	}{
		{"NReversed", query("ekn", "", between, ekItem{":p": ekS("a"), ":a": voN("10"), ":b": voN("9")})},
		{"NReversedDecimals", query("ekn", "", between, ekItem{":p": ekS("a"), ":a": voN("1.5"), ":b": voN("1.25")})},
		{"NReversedTrailingZero", query("ekn", "", between, ekItem{":p": ekS("a"), ":a": voN("10.0"), ":b": voN("9")})},
		{"NReversedExponent", query("ekn", "", between, ekItem{":p": ekS("a"), ":a": voN("1e1"), ":b": voN("9")})},
		{"NReversedNegative", query("ekn", "", between, ekItem{":p": ekS("a"), ":a": voN("-1"), ":b": voN("-2")})},
		{"NInOrderNumerically", query("ekn", "", between, ekItem{":p": ekS("a"), ":a": voN("9"), ":b": voN("10")})},
		{"NEqualSpellings", query("ekn", "", between, ekItem{":p": ekS("a"), ":a": voN("10"), ":b": voN("10.0")})},
		{"BReversed", query(emptyKeyBinTable, "", between, ekItem{":p": ekB(1), ":a": ekB(2), ":b": ekB(1)})},
		{"BEmptyHigh", query(emptyKeyBinTable, "", between, ekItem{":p": ekB(1), ":a": ekB(1), ":b": ekB()})},
		{"MixedSAndN", query(emptyKeyTable, "", between, ekItem{":p": ekS("a"), ":a": ekS("z"), ":b": voN("1")})},
		{"MixedNAndS", query(emptyKeyTable, "", between, ekItem{":p": ekS("a"), ":a": voN("1"), ":b": ekS("a")})},
		{"ReversedBoundsEmptyHash", query(emptyKeyTable, "", between, ekItem{":p": e, ":a": ekS("z"), ":b": ekS("a")})},
		{"ReversedBoundsHashTypeMismatch", query(emptyKeyTable, "", between, ekItem{":p": voN("1"), ":a": ekS("z"), ":b": ekS("a")})},
		{"ReversedWrongTypedBounds", query(emptyKeyTable, "", between, ekItem{":p": ekS("a"), ":a": voN("10"), ":b": voN("9")})},
		{"MixedBoundsEmptyHash", query(emptyKeyTable, "", between, ekItem{":p": e, ":a": ekS("z"), ":b": voN("1")})},
		{"GSIReversedBoundsEmptyHash", query(emptyKeyTable, "by-g", "g = :p AND gs BETWEEN :a AND :b", ekItem{":p": e, ":a": ekS("z"), ":b": ekS("a")})},
		{"GSIReversedBounds", query(emptyKeyTable, "by-g", "g = :p AND gs BETWEEN :a AND :b", ekItem{":p": ekS("g1"), ":a": ekS("b"), ":b": ekS("a")})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { RunE2E(t, voRun(tt.fn)) })
	}
}

// A GetItem checks its ProjectionExpression (the syntax, then the names it uses) before the
// key, and after the table exists.
func TestE2E_ValidationOrder_GetItemProjection(t *testing.T) {
	e := ekS("")
	okKey := ekItem{"pk": ekS("a"), "sk": ekS("1")}
	missingKey := ekItem{"pk": ekS("zz"), "sk": ekS("1")}
	emptySK := ekItem{"pk": ekS("a"), "sk": e}
	noSK := ekItem{"pk": ekS("a")}
	wrongType := ekItem{"pk": ekS("a"), "sk": voN("1")}
	extraAttr := ekItem{"pk": ekS("a"), "sk": ekS("1"), "x": ekS("y")}
	names := map[string]string{"#n": "g"}

	get := func(table string, key ekItem, projection string, names map[string]string) func(ctx context.Context, c *dynamodb.Client) string {
		return func(ctx context.Context, c *dynamodb.Client) string {
			in := &dynamodb.GetItemInput{TableName: aws.String(table), Key: key, ExpressionAttributeNames: names}
			if projection != "" {
				in.ProjectionExpression = aws.String(projection)
			}

			out, err := c.GetItem(ctx, in)
			if err != nil {
				return fuzzErrString(err)
			}

			return fmt.Sprintf("ok item=%s", nmItemString(out.Item))
		}
	}

	// BatchGetItem is the opposite of GetItem: DynamoDB reports a key problem before the
	// ProjectionExpression.
	batchGet := func(projection string, keys ...ekItem) func(ctx context.Context, c *dynamodb.Client) string {
		return func(ctx context.Context, c *dynamodb.Client) string {
			out, err := c.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]dynamodbtypes.KeysAndAttributes{
				emptyKeyTable: {Keys: keys, ProjectionExpression: aws.String(projection)},
			}})
			if err != nil {
				return fuzzErrString(err)
			}

			return fmt.Sprintf("ok items=%d", len(out.Responses[emptyKeyTable]))
		}
	}

	tests := []struct {
		name string
		fn   func(ctx context.Context, c *dynamodb.Client) string
	}{
		{"BatchGetEmptyKeyUndeclaredName", batchGet("#x", emptySK)},
		{"BatchGetEmptyKeySyntaxError", batchGet("g,,", emptySK)},
		{"BatchGetMissingRangeKeyUndeclaredName", batchGet("#x", noSK)},
		// DynamoDB validates every key of the table before the projection.
		{"BatchGetOkThenEmptyKeyUndeclaredName", batchGet("#x", okKey, emptySK)},
		{"BatchGetOkThenEmptyKeySyntaxError", batchGet("g,,", okKey, emptySK)},
		{"BatchGetOkThenMissingRangeKey", batchGet("#x", okKey, noSK)},
		{"EmptyKeyUndeclaredName", get(emptyKeyTable, emptySK, "#x", nil)},
		{"EmptyKeySyntaxError", get(emptyKeyTable, emptySK, "g,,", nil)},
		{"EmptyKeyUndeclaredNameInPath", get(emptyKeyTable, emptySK, "g.#x", nil)},
		{"EmptyKeyDeclaredName", get(emptyKeyTable, emptySK, "#n", names)},
		{"EmptyKeyValidProjection", get(emptyKeyTable, emptySK, "g", nil)},
		{"MissingRangeKeyUndeclaredName", get(emptyKeyTable, noSK, "#x", nil)},
		{"MissingRangeKeySyntaxError", get(emptyKeyTable, noSK, "g,,", nil)},
		{"MissingRangeKeyValidProjection", get(emptyKeyTable, noSK, "g", nil)},
		{"WrongTypeKeyUndeclaredName", get(emptyKeyTable, wrongType, "#x", nil)},
		{"WrongTypeKeySyntaxError", get(emptyKeyTable, wrongType, "g,,", nil)},
		{"ExtraKeyAttributeUndeclaredName", get(emptyKeyTable, extraAttr, "#x", nil)},
		{"EmptyBinaryKeyUndeclaredName", get(emptyKeyBinTable, ekItem{"pk": ekB(), "sk": ekB(1)}, "#x", nil)},
		{"InvalidNumberKeyUndeclaredName", get("ekn", ekItem{"pk": ekS("a"), "sk": voN("abc")}, "#x", nil)},
		{"InvalidNumberKeyValidProjection", get("ekn", ekItem{"pk": ekS("a"), "sk": voN("abc")}, "g", nil)},
		{"ValidKeyUndeclaredName", get(emptyKeyTable, okKey, "#x", nil)},
		{"ValidKeySyntaxError", get(emptyKeyTable, okKey, "g,,", nil)},
		{"MissingItemUndeclaredName", get(emptyKeyTable, missingKey, "#x", nil)},
		{"MissingItemSyntaxError", get(emptyKeyTable, missingKey, "g,,", nil)},
		{"ValidKeyValidProjection", get(emptyKeyTable, okKey, "gs, #n", names)},
		{"UnknownTableUndeclaredName", get("no-such-table", okKey, "#x", nil)},
		{"UnknownTableEmptyKey", get("no-such-table", emptySK, "g", nil)},
		{"UnusedNameEmptyKey", get(emptyKeyTable, emptySK, "g", names)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { RunE2E(t, voRun(tt.fn)) })
	}
}

// A Query or Scan naming an index the table does not have is rejected before the rest of
// the request is looked at or any item is read. These cases pin where that error sits
// among the other problems a request can have.
func TestE2E_ValidationOrder_UnknownIndex(t *testing.T) {
	type searchFn = func(ctx context.Context, c *dynamodb.Client) string

	query := func(index, cond string, vals ekItem, mod func(*dynamodb.QueryInput)) searchFn {
		return func(ctx context.Context, c *dynamodb.Client) string {
			in := &dynamodb.QueryInput{
				TableName: aws.String(emptyKeyTable), IndexName: aws.String(index),
				KeyConditionExpression: aws.String(cond), ExpressionAttributeValues: vals,
			}
			if mod != nil {
				mod(in)
			}

			out, err := c.Query(ctx, in)
			if err != nil {
				return fuzzErrString(err)
			}

			return fmt.Sprintf("ok items=%d", len(out.Items))
		}
	}
	scan := func(index string, mod func(*dynamodb.ScanInput)) searchFn {
		return func(ctx context.Context, c *dynamodb.Client) string {
			in := &dynamodb.ScanInput{TableName: aws.String(emptyKeyTable), IndexName: aws.String(index)}
			if mod != nil {
				mod(in)
			}

			out, err := c.Scan(ctx, in)
			if err != nil {
				return fuzzErrString(err)
			}

			return fmt.Sprintf("ok items=%d", len(out.Items))
		}
	}
	// The key condition is the one a query of the table would accept, as the index is unknown.
	const cond = "pk = :p"

	validVals := ekItem{":p": ekS("a")}
	badSelect := func(in *dynamodb.QueryInput) { in.Select = dynamodbtypes.Select("BOGUS") }
	consistent := func(in *dynamodb.QueryInput) { in.ConsistentRead = aws.Bool(true) }

	tests := []struct {
		name string
		fn   searchFn
	}{
		{"QueryUnknownIndex", query("nope", cond, validVals, nil)},
		{"ScanUnknownIndex", scan("nope", nil)},
		{"QueryUnknownIndexEmptyHash", query("nope", cond, ekItem{":p": ekS("")}, nil)},
		{"QueryUnknownIndexHashTypeMismatch", query("nope", cond, ekItem{":p": voN("1")}, nil)},
		{"QueryUnknownIndexBetweenReversed", query("nope", "pk = :p AND sk BETWEEN :a AND :b",
			ekItem{":p": ekS("a"), ":a": ekS("z"), ":b": ekS("a")}, nil)},
		{"QueryUnknownIndexNotTheKeyAttribute", query("nope", "g = :p", validVals, nil)},
		{"QueryUnknownIndexUndeclaredName", query("nope", "#n = :p", validVals, nil)},
		{"QueryUnknownIndexUndefinedValue", query("nope", cond, nil, nil)},
		{"QueryUnknownIndexInvalidSelect", query("nope", cond, validVals, badSelect)},
		{"QueryUnknownIndexAllProjectedSelect", query("nope", cond, validVals, func(in *dynamodb.QueryInput) {
			in.Select = dynamodbtypes.SelectAllProjectedAttributes
		})},
		{"QueryUnknownIndexConsistentRead", query("nope", cond, validVals, consistent)},
		{"QueryUnknownIndexUnusedName", query("nope", cond, validVals, func(in *dynamodb.QueryInput) {
			in.ExpressionAttributeNames = map[string]string{"#u": "g"}
		})},
		{"QueryUnknownIndexUnusedValue", query("nope", cond, ekItem{":p": ekS("a"), ":u": ekS("x")}, nil)},
		{"QueryUnknownIndexInvalidNameAlias", query("nope", cond, validVals, func(in *dynamodb.QueryInput) {
			in.ExpressionAttributeNames = map[string]string{"bad": "g"}
		})},
		{"QueryUnknownIndexUndeclaredFilterName", query("nope", cond, validVals, func(in *dynamodb.QueryInput) {
			in.FilterExpression = aws.String("#f = :p")
		})},
		{"ScanUnknownIndexUndeclaredName", scan("nope", func(in *dynamodb.ScanInput) {
			in.FilterExpression = aws.String("#n = :v")
			in.ExpressionAttributeValues = ekItem{":v": ekS("x")}
		})},
		{"ScanUnknownIndexUndefinedValue", scan("nope", func(in *dynamodb.ScanInput) {
			in.FilterExpression = aws.String("g = :v")
		})},
		{"ScanUnknownIndexInvalidSelect", scan("nope", func(in *dynamodb.ScanInput) {
			in.Select = dynamodbtypes.Select("BOGUS")
		})},
		{"ScanUnknownIndexConsistentRead", scan("nope", func(in *dynamodb.ScanInput) {
			in.ConsistentRead = aws.Bool(true)
		})},
		{"ScanUnknownIndexUnusedName", scan("nope", func(in *dynamodb.ScanInput) {
			in.ExpressionAttributeNames = map[string]string{"#u": "g"}
		})},
		// A missing table is reported before the unknown index and the pre-check of the names.
		{"QueryMissingTableUnknownIndex", func(ctx context.Context, c *dynamodb.Client) string {
			_, err := c.Query(ctx, &dynamodb.QueryInput{
				TableName: aws.String("missing"), IndexName: aws.String("nope"),
				KeyConditionExpression: aws.String(cond), ExpressionAttributeValues: validVals,
			})

			return fuzzErrString(err)
		}},
		{"QueryMissingTableUnusedName", func(ctx context.Context, c *dynamodb.Client) string {
			_, err := c.Query(ctx, &dynamodb.QueryInput{
				TableName: aws.String("missing"), KeyConditionExpression: aws.String(cond),
				ExpressionAttributeValues: validVals, ExpressionAttributeNames: map[string]string{"#u": "g"},
			})

			return fuzzErrString(err)
		}},
		{"ScanMissingTableUnusedName", func(ctx context.Context, c *dynamodb.Client) string {
			_, err := c.Scan(ctx, &dynamodb.ScanInput{
				TableName: aws.String("missing"), ExpressionAttributeNames: map[string]string{"#u": "g"},
			})

			return fuzzErrString(err)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { RunE2E(t, voRun(tt.fn)) })
	}
}
