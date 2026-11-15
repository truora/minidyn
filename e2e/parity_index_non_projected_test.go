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

// parityIndexReadReq is one Query or Scan against the select table (see paritySelectTable), whose
// item has pk, sk, g, l, a and b. lko/gko project keys only, linc/ginc also project a, so b is
// never projected.
type parityIndexReadReq struct {
	scan       bool
	index      string
	sel        dynamodbtypes.Select
	projection string
	filter     string
	keyCond    string // overrides the default key condition of a Query
	knownIssue string // the issue tracking a divergence of minidyn on this case (see RunE2EKnown)
	names      map[string]string
	values     map[string]dynamodbtypes.AttributeValue
}

// parityIndexReadRun returns the error text, or the sorted attribute names, Count and ScannedCount.
func parityIndexReadRun(ctx context.Context, client *dynamodb.Client, r parityIndexReadReq) string {
	opt := func(s string) *string {
		if s == "" {
			return nil
		}

		return aws.String(s)
	}

	values := map[string]dynamodbtypes.AttributeValue{}
	for k, v := range r.values {
		values[k] = v
	}

	var (
		items          []map[string]dynamodbtypes.AttributeValue
		count, scanned int32
		err            error
	)

	if len(r.names) == 0 {
		r.names = nil
	}

	if r.scan {
		if len(values) == 0 {
			values = nil
		}

		var out *dynamodb.ScanOutput

		out, err = client.Scan(ctx, &dynamodb.ScanInput{
			TableName: aws.String(paritySelectTableName), IndexName: opt(r.index), Select: r.sel,
			ProjectionExpression: opt(r.projection), FilterExpression: opt(r.filter),
			ExpressionAttributeNames: r.names, ExpressionAttributeValues: values,
		})
		if err == nil {
			items, count, scanned = out.Items, out.Count, out.ScannedCount
		}
	} else {
		key, val := "pk", "p"
		if strings.HasPrefix(r.index, "g") {
			key, val = "g", "g"
		}

		values[":v"] = &dynamodbtypes.AttributeValueMemberS{Value: val}

		cond := key + " = :v"
		if r.keyCond != "" {
			cond = r.keyCond
		}

		var out *dynamodb.QueryOutput

		out, err = client.Query(ctx, &dynamodb.QueryInput{
			TableName: aws.String(paritySelectTableName), IndexName: opt(r.index), Select: r.sel,
			KeyConditionExpression: aws.String(cond), ProjectionExpression: opt(r.projection),
			FilterExpression: opt(r.filter), ExpressionAttributeNames: r.names, ExpressionAttributeValues: values,
		})
		if err == nil {
			items, count, scanned = out.Items, out.Count, out.ScannedCount
		}
	}

	if err != nil {
		return normalizeSDKErrorString(err.Error())
	}

	return fmt.Sprintf("%s count=%d scanned=%d", paritySortedAttrNames(items), count, scanned)
}

// parityGSIRejectCases names the cases whose GSI variants wait for #201: minidyn does not yet
// reject a GSI request for attributes the index does not project.
var parityGSIRejectCases = map[string]bool{
	"SpecificB": true, "ProjectionB": true, "ProjectionAB": true, "ProjectionBA": true, "ProjectionA": true,
	"ProjectionKeys": true, "ProjectionNestedB": true, "ProjectionUnknownAttr": true, "FilterBSpecificA": true,
	"FilterAProjectionB": true, "FilterAndProjectionB": true, "ProjectionNamesSorted": true,
	"ProjectionBNestedTwice": true, "ProjectionListB": true, "BadKeyCondProjectionB": true,
	"UndefinedKeyValueProjectionB": true,
}

// parityIndexNonProjectedCases lists the requests, for Query and Scan, on every index.
func parityIndexNonProjectedCases() (names []string, reqs []parityIndexReadReq) {
	bVal := map[string]dynamodbtypes.AttributeValue{":b": &dynamodbtypes.AttributeValueMemberS{Value: "b"}}
	aVal := map[string]dynamodbtypes.AttributeValue{":a": &dynamodbtypes.AttributeValueMemberS{Value: "a"}}
	nb := map[string]string{"#b": "b"}

	for _, scan := range []bool{false, true} {
		op := "Query"
		if scan {
			op = "Scan"
		}

		addTo := func(idx string) func(string, parityIndexReadReq) {
			return func(name string, r parityIndexReadReq) {
				if strings.HasPrefix(idx, "g") && parityGSIRejectCases[name] {
					r.knownIssue = "#201"
				}

				r.scan, r.index = scan, idx
				names = append(names, op+"/"+idx+"/"+name)
				reqs = append(reqs, r)
			}
		}

		for _, idx := range []string{"lko", "linc", "gko", "ginc"} {
			add := addTo(idx)

			add("AllAttributes", parityIndexReadReq{sel: "ALL_ATTRIBUTES"})
			add("AllProjected", parityIndexReadReq{sel: "ALL_PROJECTED_ATTRIBUTES"})
			add("Default", parityIndexReadReq{})
			add("SpecificB", parityIndexReadReq{sel: "SPECIFIC_ATTRIBUTES", projection: "#b", names: nb})
			add("ProjectionB", parityIndexReadReq{projection: "#b", names: nb})
			add("ProjectionAB", parityIndexReadReq{projection: "a, #b", names: nb})
			add("ProjectionBA", parityIndexReadReq{projection: "#b, a", names: nb})
			add("ProjectionA", parityIndexReadReq{projection: "a"})
			add("ProjectionKeys", parityIndexReadReq{projection: "pk, sk, g, l"})
			add("ProjectionNestedB", parityIndexReadReq{projection: "b.x"})
			add("ProjectionUnknownAttr", parityIndexReadReq{projection: "zz"})
			add("FilterB", parityIndexReadReq{filter: "#b = :b", names: nb, values: bVal})
			add("FilterBSpecificA", parityIndexReadReq{sel: "SPECIFIC_ATTRIBUTES", projection: "a", filter: "#b = :b", names: nb, values: bVal})
			add("FilterBAllAttributes", parityIndexReadReq{sel: "ALL_ATTRIBUTES", filter: "#b = :b", names: nb, values: bVal})
			add("FilterBCount", parityIndexReadReq{sel: "COUNT", filter: "#b = :b", names: nb, values: bVal})
			add("FilterBMissing", parityIndexReadReq{filter: "attribute_not_exists(#b)", names: nb})
			add("FilterAProjectionB", parityIndexReadReq{projection: "#b", filter: "a = :a", names: nb, values: aVal})
			add("FilterAAllAttributes", parityIndexReadReq{sel: "ALL_ATTRIBUTES", filter: "a = :a", values: aVal})
			add("FilterAndProjectionB", parityIndexReadReq{projection: "#b", filter: "#b = :b", names: nb, values: bVal})
			add("CountProjectionB", parityIndexReadReq{sel: "COUNT", projection: "#b", names: nb})
			add("AllAttributesProjectionB", parityIndexReadReq{sel: "ALL_ATTRIBUTES", projection: "#b", names: nb})
			add("UndeclaredNameProjection", parityIndexReadReq{projection: "#x"})
			add("UndeclaredNameProjectionB", parityIndexReadReq{projection: "#x, b"})
			add("UnusedNameProjectionB", parityIndexReadReq{projection: "b", names: map[string]string{"#u": "u"}})
			add("UndefinedFilterValueProjectionB", parityIndexReadReq{projection: "b", filter: "a = :zz"})
			add("SyntaxErrorProjectionB", parityIndexReadReq{projection: "b,"})
		}

		// Orders against other request errors, on a GSI and an LSI.
		for _, idx := range []string{"lko", "gko"} {
			add := addTo(idx)

			add("BadSelectProjectionB", parityIndexReadReq{sel: "BOGUS", projection: "b"})
			add("SpecificNoProjectionFilterB", parityIndexReadReq{sel: "SPECIFIC_ATTRIBUTES", filter: "#b = :b", names: nb, values: bVal})
			add("AllProjectedProjectionB", parityIndexReadReq{sel: "ALL_PROJECTED_ATTRIBUTES", projection: "b"})
			add("BadKeyCondProjectionB", parityIndexReadReq{projection: "b", keyCond: "zz = :v"})
			add("BadKeyCondAllAttributes", parityIndexReadReq{sel: "ALL_ATTRIBUTES", keyCond: "zz = :v"})
			add("UndefinedKeyValueProjectionB", parityIndexReadReq{projection: "b", keyCond: "pk = :nope"})
			add("BadKeyCondSyntaxErrorProjection", parityIndexReadReq{projection: "b,", keyCond: "zz = :v"})
			add("BadKeyCondUndeclaredProjection", parityIndexReadReq{projection: "#x", keyCond: "zz = :v"})
			add("BadSelectSyntaxErrorProjection", parityIndexReadReq{sel: "BOGUS", projection: "b,"})
			add("CountSyntaxErrorProjection", parityIndexReadReq{sel: "COUNT", projection: "b,"})
			add("AllAttributesSyntaxErrorProjection", parityIndexReadReq{sel: "ALL_ATTRIBUTES", projection: "b,"})
			add("SyntaxErrorProjectionUndeclaredFilter", parityIndexReadReq{projection: "b,", filter: "#q = :b", values: bVal})
		}

		// A filter naming attributes the index does not project, and where its check sits.
		bz := map[string]dynamodbtypes.AttributeValue{":n": &dynamodbtypes.AttributeValueMemberN{Value: "1"}}
		nbz := map[string]string{"#b": "b", "#z": "zz"}

		for _, idx := range []string{"lko", "linc", "gko", "ginc"} {
			add := addTo(idx)

			add("FilterBThenZ", parityIndexReadReq{filter: "#b = :b AND #z = :b", names: nbz, values: bVal})
			add("FilterZThenB", parityIndexReadReq{filter: "#z = :b AND #b = :b", names: nbz, values: bVal})
			add("FilterBTwice", parityIndexReadReq{filter: "#b = :b OR #b <> :b", names: nb, values: bVal})
			add("FilterNestedB", parityIndexReadReq{filter: "b.x = :b", values: bVal})
			add("FilterListB", parityIndexReadReq{filter: "b[0] = :b", values: bVal})
			add("FilterSizeB", parityIndexReadReq{filter: "size(b) > :n", values: bz})
			add("FilterAttributeExistsB", parityIndexReadReq{filter: "attribute_exists(b)"})
			add("FilterContainsB", parityIndexReadReq{filter: "contains(b, :b)", values: bVal})
			add("FilterBAndA", parityIndexReadReq{filter: "b = :b AND a = :a", values: map[string]dynamodbtypes.AttributeValue{":b": bVal[":b"], ":a": aVal[":a"]}})
			add("FilterBBadKeyCond", parityIndexReadReq{filter: "b = :b", values: bVal, keyCond: "zz = :v"})
			add("FilterBUndefinedKeyValue", parityIndexReadReq{filter: "b = :b", values: bVal, keyCond: "pk = :nope"})
			add("FilterBBadSelect", parityIndexReadReq{sel: "BOGUS", filter: "b = :b", values: bVal})
			add("FilterBCountProjectionA", parityIndexReadReq{sel: "COUNT", projection: "a", filter: "b = :b", values: bVal})
			add("FilterBAllProjectedProjectionA", parityIndexReadReq{sel: "ALL_PROJECTED_ATTRIBUTES", projection: "a", filter: "b = :b", values: bVal})
			add("FilterBUndeclaredProjectionName", parityIndexReadReq{projection: "#x", filter: "b = :b", values: bVal})
			add("FilterBSyntaxErrorProjection", parityIndexReadReq{projection: "a,", filter: "b = :b", values: bVal})
			add("FilterBUndeclaredFilterName", parityIndexReadReq{filter: "#x = :b", values: bVal})
			add("FilterBUnusedValue", parityIndexReadReq{filter: "b = :b", values: map[string]dynamodbtypes.AttributeValue{":b": bVal[":b"], ":u": bVal[":b"]}})
			add("ProjectionNamesSorted", parityIndexReadReq{projection: "a, b, l"})
			add("ProjectionBNestedTwice", parityIndexReadReq{projection: "b.x, b.y"})
			add("ProjectionListB", parityIndexReadReq{projection: "b[0]"})
		}

		add := addTo("nope")
		add("UnknownIndexProjectionB", parityIndexReadReq{projection: "b"})
		add("UnknownIndexUndeclaredName", parityIndexReadReq{projection: "#x"})
	}

	return names, reqs
}

// TestE2E_IndexNonProjectedAttributes pins what a Query or Scan on an index does with attributes
// the index does not project: an LSI fetches them from the table (#202), a GSI rejects the
// request (#201).
func TestE2E_IndexNonProjectedAttributes(t *testing.T) {
	names, reqs := parityIndexNonProjectedCases()

	for _, empty := range []bool{false, true} {
		name := "WithItem"
		if empty {
			// The request checks run before any item is read, so they fail on an empty table too.
			name = "EmptyTable"
		}

		t.Run(name, func(t *testing.T) {
			run := func(t *testing.T, client *dynamodb.Client, known bool) any {
				t.Helper()
				ctx := context.Background()

				paritySelectTable(ctx, t, client)

				if empty {
					_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
						TableName: aws.String(paritySelectTableName),
						Key: map[string]dynamodbtypes.AttributeValue{
							"pk": &dynamodbtypes.AttributeValueMemberS{Value: "p"},
							"sk": &dynamodbtypes.AttributeValueMemberS{Value: "s"},
						},
					})
					require.NoError(t, err)
				}

				out := make([]any, 0, len(reqs))

				for i, r := range reqs {
					if (r.knownIssue != "") == known {
						out = append(out, names[i]+": "+parityIndexReadRun(ctx, client, r))
					}
				}

				return out
			}

			RunE2E(t, func(t *testing.T, client *dynamodb.Client) any { return run(t, client, false) })

			// The GSI cases that wait for #201 run apart, so one fixed case cannot hide behind the rest.
			t.Run("KnownIssue", func(t *testing.T) {
				RunE2EKnown(t, "#201", func(t *testing.T, client *dynamodb.Client) any { return run(t, client, true) })
			})
		})
	}
}
