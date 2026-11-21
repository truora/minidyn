package core

import (
	"maps"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/truora/minidyn/types"
)

// An ExclusiveStartKey is validated before any item is read (#171), in the order DynamoDB
// Local reports it:
//
//  1. its size against the key attributes (the table's key schema for a table or an LSI, and
//     simply "invalid" for a GSI);
//  2. each key attribute in turn (the table's, then the index's), which must be present and
//     of the declared type;
//  3. an empty S or B value, in the same order;
//  4. a Query's key condition, which the start key must satisfy.
//
// The expected texts are DynamoDB Local's, probed against it (see e2e/parity_start_key_validation_test.go).

const (
	skSizeMsg      = "Exclusive Start Key must have same size as table's key schema"
	skInvalidMsg   = "The provided starting key is invalid"
	skTypeMsg      = "Type mismatch for attribute to update"
	skPredicateMsg = "The provided starting key does not match the range key predicate"
	skRestartMsg   = "The query can return at most one row and cannot be restarted. "

	skEmptyKey  = "The provided starting key is invalid: " + emptyStringKeyMsg
	skEmptyHash = skRestartMsg + skEmptyKey
	skEmptyIdx  = "The provided starting key is invalid: " + secondaryKeyMsg + "The AttributeValue for a key attribute cannot contain an empty string value."
)

type skItem = map[string]*types.Item

func skS(v string) *types.Item { return &types.Item{S: &v} }
func skN(v string) *types.Item { return &types.Item{N: &v} }

// skMessage is the error text without minidyn's detail, which follows the last " | ".
func skMessage(err error) string {
	s := strings.TrimPrefix(err.Error(), "ValidationException: ")
	if i := strings.LastIndex(s, types.DetailSeparator); i >= 0 {
		s = s[:i]
	}

	return s
}

func skTable(t *testing.T) *Table {
	t.Helper()

	// pk/sk, with an LSI on l and a GSI on g/gr: a=(l 3, gr 1), b=(2, 2), c=(1, 3), all in
	// partition p and GSI partition g.
	return startKeyTestTable(t, "S", []string{"a", "b", "c"}, []string{"3", "2", "1"})
}

func skTableKey(sk string) skItem { return skItem{"pk": skS("p"), "sk": skS(sk)} }

func skLSIKey(sk, l string) skItem { return skItem{"pk": skS("p"), "sk": skS(sk), "l": skS(l)} }

func skGSIKey(sk, gr string) skItem {
	return skItem{"pk": skS("p"), "sk": skS(sk), "g": skS("g"), "gr": skS(gr)}
}

func skWith(key skItem, attr string, v *types.Item) skItem {
	out := skItem{}
	maps.Copy(out, key)

	if v == nil {
		delete(out, attr)
	} else {
		out[attr] = v
	}

	return out
}

func skQuery(index, cond string, values skItem) QueryInput {
	return QueryInput{Index: index, KeyConditionExpression: cond, ExpressionAttributeValues: values}
}

func skTableQuery(cond string, extra ...skItem) QueryInput {
	values := skItem{":p": skS("p")}
	for _, e := range extra {
		maps.Copy(values, e)
	}

	return skQuery("", cond, values)
}

func skGSIQuery(cond string, extra ...skItem) QueryInput {
	values := skItem{":p": skS("g")}
	for _, e := range extra {
		maps.Copy(values, e)
	}

	return skQuery("by-g", cond, values)
}

func skLSIQuery(cond string, extra ...skItem) QueryInput {
	in := skTableQuery(cond, extra...)
	in.Index = "by-l"

	return in
}

type skCase struct {
	name  string
	in    QueryInput
	start skItem
	want  string // "" when the start key is accepted
}

func runSkCases(t *testing.T, table *Table, cases []skCase) {
	t.Helper()

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in
			in.ExclusiveStartKey = tc.start
			in.ScanIndexForward = true

			_, err := table.SearchData(in)
			if tc.want == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Equal(t, tc.want, skMessage(err))
			require.Contains(t, err.Error(), types.DetailSeparator, "the detail says what was wrong")
		})
	}
}

func TestSearchData_startKeyOnTable(t *testing.T) {
	table := skTable(t)
	q := skTableQuery("pk = :p")

	runSkCases(t, table, []skCase{
		{"existing item", q, skTableKey("a"), ""},
		{"no such item in the partition", q, skTableKey("zz"), ""},
		{"another partition", q, skItem{"pk": skS("q"), "sk": skS("a")}, skPredicateMsg},
		{"missing range key", q, skItem{"pk": skS("p")}, skSizeMsg},
		{"missing hash key", q, skItem{"sk": skS("a")}, skSizeMsg},
		{"placeholder", q, skItem{"lastkey": skS("value")}, skSizeMsg},
		{"extra attribute", q, skWith(skTableKey("a"), "x", skS("1")), skSizeMsg},
		{"index key attribute", q, skWith(skTableKey("a"), "l", skS("1")), skSizeMsg},
		{"same size, range key missing", q, skItem{"pk": skS("p"), "x": skS("1")}, skInvalidMsg},
		{"same size, hash key missing", q, skItem{"x": skS("p"), "sk": skS("a")}, skInvalidMsg},
		{"range key of the wrong type", q, skItem{"pk": skS("p"), "sk": skN("1")}, skTypeMsg},
		{"hash key of the wrong type", q, skItem{"pk": skN("1"), "sk": skS("a")}, skTypeMsg},
		{"binary range key", q, skItem{"pk": skS("p"), "sk": {B: []byte("a")}}, skTypeMsg},
		{"BOOL range key", q, skItem{"pk": skS("p"), "sk": {BOOL: new(true)}}, skTypeMsg},
		{"wrong type in another partition", q, skItem{"pk": skS("q"), "sk": skN("1")}, skTypeMsg},
		{"wrong type, then the size", q, skWith(skItem{"pk": skS("p"), "sk": skN("1")}, "x", skS("1")), skSizeMsg},
		// The attributes are checked in turn: a missing one hides the type of a later one,
		// and a wrong type hides a later missing one.
		{"hash key of the wrong type, range key missing", q, skItem{"pk": skN("1"), "x": skS("1")}, skTypeMsg},
		{"hash key missing, range key of the wrong type", q, skItem{"x": skS("1"), "sk": skN("1")}, skInvalidMsg},
		{"empty range key", q, skItem{"pk": skS("p"), "sk": skS("")}, skEmptyKey + " Key: sk"},
		{"empty hash key", q, skItem{"pk": skS(""), "sk": skS("a")}, skEmptyHash + " Key: pk"},
		{"both empty reports the hash key", q, skItem{"pk": skS(""), "sk": skS("")}, skEmptyHash + " Key: pk"},
		{"empty range key in another partition", q, skItem{"pk": skS("q"), "sk": skS("")}, skEmptyKey + " Key: sk"},
		{"wrong type before empty", q, skItem{"pk": skS(""), "sk": skN("1")}, skTypeMsg},
		{"missing before empty", q, skItem{"pk": skS(""), "x": skS("1")}, skInvalidMsg},
		{"size before empty", q, skWith(skItem{"pk": skS("p"), "sk": skS("")}, "x", skS("1")), skSizeMsg},
	})
}

func TestSearchData_startKeyRangePredicate(t *testing.T) {
	table := skTable(t)
	b := skItem{":s": skS("b"), ":t": skS("d")}

	// Probed against DynamoDB Local in both directions. A start key at the inclusive bound the
	// walk ends at (<= or the upper bound of BETWEEN going forward, >= or its lower bound going
	// backward) is accepted, though Local rejects it (#153): minidyn returns that key as a
	// LastEvaluatedKey when a page ends there, and must take it back, with an empty page.
	cases := []struct {
		name     string
		cond     string
		start    string
		forward  string
		backward string
	}{
		{"> and start below", "pk = :p AND sk > :s", "a", skPredicateMsg, skPredicateMsg},
		{"> and start equal", "pk = :p AND sk > :s", "b", skPredicateMsg, skPredicateMsg},
		{"> and start above", "pk = :p AND sk > :s", "c", "", ""},
		{">= and start equal", "pk = :p AND sk >= :s", "b", "", ""},
		{">= and start below", "pk = :p AND sk >= :s", "a", skPredicateMsg, skPredicateMsg},
		{">= and start above", "pk = :p AND sk >= :s", "c", "", ""},
		{"< and start above", "pk = :p AND sk < :s", "c", skPredicateMsg, skPredicateMsg},
		{"< and start equal", "pk = :p AND sk < :s", "b", skPredicateMsg, skPredicateMsg},
		{"< and start below", "pk = :p AND sk < :s", "a", "", ""},
		{"<= and start above", "pk = :p AND sk <= :s", "c", skPredicateMsg, skPredicateMsg},
		{"<= and start equal", "pk = :p AND sk <= :s", "b", "", ""},
		{"<= and start below", "pk = :p AND sk <= :s", "a", "", ""},
		{"operands reversed, <=", "pk = :p AND :s >= sk", "b", "", ""},
		{"operands reversed, >=", "pk = :p AND :s <= sk", "b", "", ""},
		{"= and another start", "pk = :p AND sk = :s", "a", skPredicateMsg, skPredicateMsg},
		{"= and start equal", "pk = :p AND sk = :s", "b", "", ""},
		{"between and start below", "pk = :p AND sk BETWEEN :s AND :t", "a", skPredicateMsg, skPredicateMsg},
		{"between and start at the lower bound", "pk = :p AND sk BETWEEN :s AND :t", "b", "", ""},
		{"between and start inside", "pk = :p AND sk BETWEEN :s AND :t", "c", "", ""},
		{"between and start at the upper bound", "pk = :p AND sk BETWEEN :s AND :t", "d", "", ""},
		{"between and start above", "pk = :p AND sk BETWEEN :s AND :t", "e", skPredicateMsg, skPredicateMsg},
		{"begins_with and start outside", "pk = :p AND begins_with(sk, :s)", "a", skPredicateMsg, skPredicateMsg},
		{"begins_with and start inside", "pk = :p AND begins_with(sk, :s)", "bb", "", ""},
		{"begins_with and start equal", "pk = :p AND begins_with(sk, :s)", "b", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := skTableQuery(tc.cond, b)

			for _, dir := range []struct {
				forward bool
				want    string
			}{{true, tc.forward}, {false, tc.backward}} {
				in.ExclusiveStartKey = skTableKey(tc.start)
				in.ScanIndexForward = dir.forward

				_, err := table.SearchData(in)
				if dir.want == "" {
					require.NoError(t, err, "forward=%v", dir.forward)

					continue
				}

				require.Error(t, err, "forward=%v", dir.forward)
				require.Equal(t, dir.want, skMessage(err), "forward=%v", dir.forward)
			}
		})
	}

	q := skTableQuery("pk = :p AND sk > :s", b)
	runSkCases(t, table, []skCase{
		{"another partition above the range", q, skItem{"pk": skS("q"), "sk": skS("c")}, skPredicateMsg},
		{"wrong shape comes first", q, skItem{"pk": skS("p")}, skSizeMsg},
		{"wrong type comes first", q, skItem{"pk": skS("p"), "sk": skN("1")}, skTypeMsg},
		{"empty value comes first", q, skItem{"pk": skS("p"), "sk": skS("")}, skEmptyKey + " Key: sk"},
	})

	// Descending walks check the same predicate.
	in := q
	in.ExclusiveStartKey = skTableKey("a")
	_, err := table.SearchData(in)
	require.Error(t, err)
	require.Equal(t, skPredicateMsg, skMessage(err))
}

func TestSearchData_startKeyNumbersCompareByValue(t *testing.T) {
	table := NewTable("nums")
	table.AttributesDef = map[string]string{"pk": "N", "sk": "N"}
	table.KeySchema = keySchema{HashKey: "pk", RangeKey: "sk"}

	q := skQuery("", "pk = :p AND sk > :s", skItem{":p": skN("1"), ":s": skN("9")})

	runSkCases(t, table, []skCase{
		{"hash key spelled differently", q, skItem{"pk": skN("1.0"), "sk": skN("10")}, ""},
		{"range key spelled differently, above", q, skItem{"pk": skN("1"), "sk": skN("1e1")}, ""},
		{"range key below by value, above as text", q, skItem{"pk": skN("1"), "sk": skN("2")}, skPredicateMsg},
		{"another hash key", q, skItem{"pk": skN("2"), "sk": skN("10")}, skPredicateMsg},
		{"a string", q, skItem{"pk": skN("1"), "sk": skS("10")}, skTypeMsg},
	})
}

func TestSearchData_startKeyOnHashOnlyTable(t *testing.T) {
	table := NewTable("hash")
	table.AttributesDef = map[string]string{"pk": "S"}
	table.KeySchema = keySchema{HashKey: "pk"}

	q := skTableQuery("pk = :p")

	runSkCases(t, table, []skCase{
		{"the partition", q, skItem{"pk": skS("p")}, ""},
		{"another partition", q, skItem{"pk": skS("q")}, skPredicateMsg},
		{"range key attribute", q, skItem{"pk": skS("p"), "sk": skS("a")}, skSizeMsg},
		{"missing", q, skItem{"x": skS("p")}, skInvalidMsg},
		{"wrong type", q, skItem{"pk": skN("1")}, skTypeMsg},
		{"empty", q, skItem{"pk": skS("")}, skEmptyHash + " Key: pk"},
		{"scan, extra attribute", QueryInput{Scan: true}, skItem{"pk": skS("p"), "sk": skS("a")}, skSizeMsg},
		{"scan, missing", QueryInput{Scan: true}, skItem{"x": skS("p")}, skInvalidMsg},
	})
}

func TestSearchData_startKeyOnLocalIndex(t *testing.T) {
	table := skTable(t)
	q := skLSIQuery("pk = :p")

	runSkCases(t, table, []skCase{
		{"existing entry", q, skLSIKey("a", "3"), ""},
		{"another partition", q, skItem{"pk": skS("q"), "sk": skS("a"), "l": skS("1")}, skPredicateMsg},
		{"missing index key", q, skTableKey("a"), skSizeMsg},
		{"missing range key", q, skItem{"pk": skS("p"), "l": skS("3")}, skSizeMsg},
		{"only the hash key", q, skItem{"pk": skS("p")}, skSizeMsg},
		{"placeholder", q, skItem{"lastkey": skS("v")}, skSizeMsg},
		{"extra attribute", q, skWith(skLSIKey("a", "3"), "x", skS("1")), skSizeMsg},
		{"same size, index key missing", q, skItem{"pk": skS("p"), "sk": skS("a"), "x": skS("1")}, skInvalidMsg},
		{"same size, range key missing", q, skItem{"pk": skS("p"), "l": skS("3"), "x": skS("1")}, skInvalidMsg},
		{"index key of the wrong type", q, skWith(skLSIKey("a", "3"), "l", skN("3")), skTypeMsg},
		{"table range key of the wrong type", q, skWith(skLSIKey("a", "3"), "sk", skN("1")), skTypeMsg},
		{"wrong type before a missing index key", q, skItem{"pk": skS("p"), "sk": skN("1"), "x": skS("1")}, skTypeMsg},
		{"missing range key before a wrong index key", q, skItem{"pk": skS("p"), "x": skS("1"), "l": skN("1")}, skInvalidMsg},
		{
			"empty index key", q, skWith(skLSIKey("a", "3"), "l", skS("")),
			skEmptyIdx + " IndexName: by-l, IndexKey: l",
		},
		{"empty table range key", q, skWith(skLSIKey("a", "3"), "sk", skS("")), skEmptyKey + " Key: sk"},
		{"empty table range key reported first", q, skItem{"pk": skS("p"), "sk": skS(""), "l": skS("")}, skEmptyKey + " Key: sk"},
		{"empty hash key", q, skWith(skLSIKey("a", "3"), "pk", skS("")), skEmptyHash + " Key: pk"},
		{"wrong type before empty", q, skItem{"pk": skS("p"), "sk": skS(""), "l": skN("1")}, skTypeMsg},
		{"index range predicate, start below", skLSIQuery("pk = :p AND l > :s", skItem{":s": skS("2")}), skLSIKey("c", "1"), skPredicateMsg},
		{"index range predicate, start above", skLSIQuery("pk = :p AND l > :s", skItem{":s": skS("2")}), skLSIKey("a", "3"), ""},
		// The bound its walk ends at is accepted, as on the table (#153).
		{"index range predicate, start at the upper bound going forward", skLSIQuery("pk = :p AND l <= :s", skItem{":s": skS("2")}), skLSIKey("b", "2"), ""},
		{
			"index range predicate, another partition", skLSIQuery("pk = :p AND l > :s", skItem{":s": skS("2")}),
			skItem{"pk": skS("q"), "sk": skS("a"), "l": skS("3")},
			skPredicateMsg,
		},
		{"begins_with on the index key", skLSIQuery("pk = :p AND begins_with(l, :s)", skItem{":s": skS("3")}), skLSIKey("c", "1"), skPredicateMsg},
	})
}

func TestSearchData_startKeyOnGlobalIndex(t *testing.T) {
	table := skTable(t)
	q := skGSIQuery("g = :p")
	full := skGSIKey("a", "1")

	runSkCases(t, table, []skCase{
		{"existing entry", q, full, ""},
		{"table key of another item, same index partition", q, skItem{"pk": skS("q"), "sk": skS("a"), "g": skS("g"), "gr": skS("1")}, ""},
		{"another index partition", q, skWith(full, "g", skS("g2")), skPredicateMsg},
		{"missing table hash key", q, skWith(full, "pk", nil), skInvalidMsg},
		{"missing table range key", q, skWith(full, "sk", nil), skInvalidMsg},
		{"missing table key", q, skItem{"g": skS("g"), "gr": skS("1")}, skInvalidMsg},
		{"missing index hash key", q, skWith(full, "g", nil), skInvalidMsg},
		{"missing index range key", q, skWith(full, "gr", nil), skInvalidMsg},
		{"table key only", q, skTableKey("a"), skInvalidMsg},
		{"placeholder", q, skItem{"lastkey": skS("v")}, skInvalidMsg},
		{"extra attribute", q, skWith(full, "x", skS("1")), skInvalidMsg},
		{"LSI key attribute", q, skWith(full, "l", skS("1")), skInvalidMsg},
		{"size before type", q, skWith(skWith(full, "gr", skN("1")), "x", skS("1")), skInvalidMsg},
		{"size before empty", q, skWith(skWith(full, "gr", skS("")), "x", skS("1")), skInvalidMsg},
		{"size before the partition", q, skWith(skWith(full, "g", skS("g2")), "x", skS("1")), skInvalidMsg},
		{"index hash key of the wrong type", q, skWith(full, "g", skN("1")), skTypeMsg},
		{"index range key of the wrong type", q, skWith(full, "gr", skN("1")), skTypeMsg},
		{"table hash key of the wrong type", q, skWith(full, "pk", skN("1")), skTypeMsg},
		{"table range key of the wrong type", q, skWith(full, "sk", skN("1")), skTypeMsg},
		// The table key is checked before the index key, attribute by attribute.
		{"table hash key wrong type, index hash key missing", q, skItem{"pk": skN("1"), "sk": skS("a"), "x": skS("1"), "gr": skS("1")}, skTypeMsg},
		{"table hash key missing, index hash key wrong type", q, skItem{"sk": skS("a"), "g": skN("1"), "gr": skS("1"), "x": skS("1")}, skInvalidMsg},
		{"table range key wrong type, index range key missing", q, skItem{"pk": skS("p"), "sk": skN("1"), "g": skS("g"), "x": skS("1")}, skTypeMsg},
		{"table range key missing, index range key wrong type", q, skItem{"pk": skS("p"), "g": skS("g"), "gr": skN("1"), "x": skS("1")}, skInvalidMsg},
		{"wrong type in another partition", q, skWith(skWith(full, "g", skS("g2")), "gr", skN("1")), skTypeMsg},
		{
			"empty index range key", q, skWith(full, "gr", skS("")),
			skEmptyIdx + " IndexName: by-g, IndexKey: gr",
		},
		{
			"empty index hash key", q, skWith(full, "g", skS("")),
			skRestartMsg + skEmptyIdx + " IndexName: by-g, IndexKey: g",
		},
		{"empty table hash key", q, skWith(full, "pk", skS("")), skEmptyHash + " Key: pk"},
		{"empty table range key", q, skWith(full, "sk", skS("")), skEmptyKey + " Key: sk"},
		{"empty table range key before an empty index key", q, skWith(skWith(full, "sk", skS("")), "g", skS("")), skEmptyKey + " Key: sk"},
		{"empty index range key in another partition", q, skWith(skWith(full, "gr", skS("")), "g", skS("g2")), skEmptyIdx + " IndexName: by-g, IndexKey: gr"},
		{"wrong type before empty", q, skWith(skWith(full, "sk", skS("")), "gr", skN("1")), skTypeMsg},
		{"range predicate, start at the bound", skGSIQuery("g = :p AND gr > :s", skItem{":s": skS("1")}), full, skPredicateMsg},
		{"range predicate, start above the bound", skGSIQuery("g = :p AND gr > :s", skItem{":s": skS("0")}), full, ""},
		{"range predicate, start at the upper bound going forward", skGSIQuery("g = :p AND gr <= :s", skItem{":s": skS("1")}), full, ""},
		{"range predicate, missing index range key", skGSIQuery("g = :p AND gr > :s", skItem{":s": skS("2")}), skWith(full, "gr", nil), skInvalidMsg},
	})
}

func TestSearchData_startKeyOnScan(t *testing.T) {
	table := skTable(t)
	scan := QueryInput{Scan: true}
	gsiScan := QueryInput{Scan: true, Index: "by-g"}
	lsiScan := QueryInput{Scan: true, Index: "by-l"}
	full := skGSIKey("a", "1")

	runSkCases(t, table, []skCase{
		{"table", scan, skTableKey("a"), ""},
		{"table, no such item", scan, skTableKey("zz"), ""},
		{"table, another partition", scan, skItem{"pk": skS("zz"), "sk": skS("a")}, ""},
		{"table, missing range key", scan, skItem{"pk": skS("p")}, skSizeMsg},
		{"table, missing hash key", scan, skItem{"sk": skS("a")}, skSizeMsg},
		{"table, placeholder", scan, skItem{"lastkey": skS("v")}, skSizeMsg},
		{"table, extra attribute", scan, skWith(skTableKey("a"), "x", skS("1")), skSizeMsg},
		{"table, index key given", scan, full, skSizeMsg},
		{"table, range key of the wrong type", scan, skItem{"pk": skS("p"), "sk": skN("1")}, skTypeMsg},
		{"table, hash key of the wrong type", scan, skItem{"pk": skN("1"), "sk": skS("a")}, skTypeMsg},
		{"table, wrong type and missing", scan, skItem{"pk": skN("1")}, skSizeMsg},
		{"table, empty range key", scan, skItem{"pk": skS("p"), "sk": skS("")}, skEmptyKey + " Key: sk"},
		{"table, empty hash key", scan, skItem{"pk": skS(""), "sk": skS("a")}, skEmptyHash + " Key: pk"},
		{"GSI", gsiScan, full, ""},
		{"GSI, no such item", gsiScan, skGSIKey("zz", "9"), ""},
		{"GSI, table key only", gsiScan, skTableKey("a"), skInvalidMsg},
		{"GSI, index key missing", gsiScan, skItem{"pk": skS("p"), "sk": skS("a")}, skInvalidMsg},
		{"GSI, placeholder", gsiScan, skItem{"lastkey": skS("v")}, skInvalidMsg},
		{"GSI, extra attribute", gsiScan, skWith(full, "x", skS("1")), skInvalidMsg},
		{"GSI, range key of the wrong type", gsiScan, skWith(full, "gr", skN("1")), skTypeMsg},
		{"GSI, empty range key", gsiScan, skWith(full, "gr", skS("")), skEmptyIdx + " IndexName: by-g, IndexKey: gr"},
		{"LSI", lsiScan, skLSIKey("a", "3"), ""},
		{"LSI, missing index key", lsiScan, skTableKey("a"), skSizeMsg},
		{"LSI, extra attribute", lsiScan, skWith(skLSIKey("a", "3"), "x", skS("1")), skSizeMsg},
		{"LSI, index key of the wrong type", lsiScan, skWith(skLSIKey("a", "3"), "l", skN("3")), skTypeMsg},
		{"LSI, empty index key", lsiScan, skWith(skLSIKey("a", "3"), "l", skS("")), skEmptyIdx + " IndexName: by-l, IndexKey: l"},
	})
}

// An empty ExclusiveStartKey is no start key: the front ends cannot tell it from an absent one.
func TestSearchData_emptyStartKeyIsAbsent(t *testing.T) {
	table := skTable(t)

	for _, start := range []skItem{nil, {}} {
		in := skTableQuery("pk = :p")
		in.ExclusiveStartKey = start
		in.ScanIndexForward = true

		res, err := table.SearchData(in)
		require.NoError(t, err)
		require.Len(t, res.Items, 3)
	}
}

// The start key is validated before any item is read: on an empty table, and in front of the
// Select and index projection checks, but after the key condition and expression attributes.
func TestSearchData_startKeyValidationOrder(t *testing.T) {
	table := skTable(t)

	keysOnly := "KEYS_ONLY"
	require.NoError(t, table.AddIndexes(
		[]*types.GlobalSecondaryIndex{{
			IndexName: new("by-k"), Projection: &types.Projection{ProjectionType: &keysOnly},
			ProvisionedThroughput: &types.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			KeySchema:             []*types.KeySchemaElement{{AttributeName: "g", KeyType: "HASH"}, {AttributeName: "gr", KeyType: "RANGE"}},
		}}, nil,
	))

	bad := skItem{"lastkey": skS("v")}
	badGSI := skItem{"lastkey": skS("v")}
	keysOnlyQuery := func(mod func(*QueryInput)) QueryInput {
		in := skGSIQuery("g = :p")
		in.Index = "by-k"
		mod(&in)

		return in
	}
	tableQuery := func(mod func(*QueryInput)) QueryInput {
		in := skTableQuery("pk = :p")
		mod(&in)

		return in
	}

	cases := []struct {
		name        string
		in          QueryInput
		start       skItem
		wantMessage string
	}{
		{"Select is checked after the start key", tableQuery(func(in *QueryInput) { in.Select = "ALL_PROJECTED_ATTRIBUTES" }), bad, skSizeMsg},
		{"Select COUNT with a projection comes after too", tableQuery(func(in *QueryInput) {
			in.Select = "COUNT"
			in.ProjectionExpression = "l"
		}), bad, skSizeMsg},
		{"filter attributes the index does not project come after", keysOnlyQuery(func(in *QueryInput) { in.FilterExpression = "l = :p" }), badGSI, skInvalidMsg},
		{"projection the index does not project comes after", keysOnlyQuery(func(in *QueryInput) { in.ProjectionExpression = "l" }), badGSI, skInvalidMsg},
		{"Select the index does not support comes after", keysOnlyQuery(func(in *QueryInput) { in.Select = "ALL_ATTRIBUTES" }), badGSI, skInvalidMsg},
		{
			"a partition mismatch comes before the index checks", keysOnlyQuery(func(in *QueryInput) { in.Select = "ALL_ATTRIBUTES" }),
			skItem{"pk": skS("p"), "sk": skS("a"), "g": skS("g2"), "gr": skS("1")},
			skPredicateMsg,
		},
		{
			"a wrong type comes before Select", tableQuery(func(in *QueryInput) { in.Select = "ALL_PROJECTED_ATTRIBUTES" }),
			skItem{"pk": skS("p"), "sk": skN("1")},
			skTypeMsg,
		},
		{"the key condition comes first", tableQuery(func(in *QueryInput) { in.KeyConditionExpression = "l = :p" }), bad, "Query condition missed key schema element"},
		{"an invalid key condition value comes first", tableQuery(func(in *QueryInput) { in.ExpressionAttributeValues[":p"] = skS("") }), bad, emptyStringKeyMsg + " Key: pk"},
		{
			"the filter's names come first", tableQuery(func(in *QueryInput) { in.FilterExpression = "#x = :p" }), bad,
			"Invalid FilterExpression: An expression attribute name used in the document path is not defined; attribute name: #x",
		},
		{
			"the filter's values come first", tableQuery(func(in *QueryInput) { in.FilterExpression = "l = :nope" }), bad,
			"Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: :nope",
		},
		{
			"the projection's names come first", tableQuery(func(in *QueryInput) { in.ProjectionExpression = "#x" }), bad,
			"Invalid ProjectionExpression: An expression attribute name used in the document path is not defined; attribute name: #x",
		},
		{
			"a number DynamoDB cannot store comes first", tableQuery(func(*QueryInput) {}),
			skItem{"pk": skN("1e999")},
			"Number overflow. Attempting to store a number with magnitude larger than supported range",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := tc.in
			in.ExclusiveStartKey = tc.start

			_, err := table.SearchData(in)
			require.Error(t, err)
			require.Equal(t, tc.wantMessage, skMessage(err))
		})
	}

	// Nothing is read: the table has no items and the start key is still rejected.
	empty := NewTable("empty")
	empty.AttributesDef = map[string]string{"pk": "S", "sk": "S"}
	empty.KeySchema = keySchema{HashKey: "pk", RangeKey: "sk"}

	in := skTableQuery("pk = :p")
	in.ExclusiveStartKey = bad

	_, err := empty.SearchData(in)
	require.Error(t, err)
	require.Equal(t, skSizeMsg, skMessage(err))
}

func TestSearchData_startKeyDetailNamesTheIndexOnlyWhenThereIsOne(t *testing.T) {
	table := skTable(t)

	in := skTableQuery("pk = :p")
	in.ExclusiveStartKey = skItem{"pk": skS("q"), "sk": skS("a")}

	_, err := table.SearchData(in)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "index:", "the table is not an index")

	in = skLSIQuery("pk = :p")
	in.ExclusiveStartKey = skItem{"pk": skS("q"), "sk": skS("a"), "l": skS("1")}

	_, err = table.SearchData(in)
	require.Error(t, err)
	require.Contains(t, err.Error(), "index: by-l")
}

// An index whose key swaps the table's (hash sk, range pk) has two key attributes, not four.
func TestSearchData_startKeyOnInvertedGlobalIndex(t *testing.T) {
	table := NewTable("inverted")
	table.AttributesDef = map[string]string{"pk": "S", "sk": "S"}
	table.KeySchema = keySchema{HashKey: "pk", RangeKey: "sk"}

	all := "ALL"
	name := "by-inv"
	require.NoError(t, table.AddIndexes(
		[]*types.GlobalSecondaryIndex{{
			IndexName: &name, Projection: &types.Projection{ProjectionType: &all},
			ProvisionedThroughput: &types.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1},
			KeySchema:             []*types.KeySchemaElement{{AttributeName: "sk", KeyType: "HASH"}, {AttributeName: "pk", KeyType: "RANGE"}},
		}}, nil,
	))

	q := skQuery(name, "sk = :p", skItem{":p": skS("a")})

	runSkCases(t, table, []skCase{
		{"existing entry", q, skItem{"pk": skS("q"), "sk": skS("a")}, ""},
		{"another index partition", q, skItem{"pk": skS("p"), "sk": skS("b")}, skPredicateMsg},
		{"one attribute", q, skItem{"pk": skS("q")}, skInvalidMsg},
		{"three attributes", q, skItem{"pk": skS("q"), "sk": skS("a"), "x": skS("1")}, skInvalidMsg},
		{"wrong type", q, skItem{"pk": skN("1"), "sk": skS("a")}, skTypeMsg},
	})
}
