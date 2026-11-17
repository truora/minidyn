package e2e

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Random operation generator for the differential fuzzer (fuzz_test.go).
//
// Values come from small domains so that operations collide on the same items, conditions
// sometimes pass and sometimes fail, and paths sometimes exist and sometimes don't.

const (
	fuzzTable = "fuzz"
	fuzzGSI   = "by-g"
)

// fuzzOp is one generated API call; run executes it against a client (errors are captured by
// the trace recorder, so they're ignored here).
type fuzzOp struct {
	name string
	run  func(ctx context.Context, client *dynamodb.Client)
}

// fuzzGen generates operations. With known=false it stays away from areas with an open
// parity issue, so a default run is green and any divergence is new. In that mode:
//
//   - an update expression uses each path once and conditions have no redundant
//     parentheses (#164: minidyn accepts invalid expressions).
type fuzzGen struct {
	rng    *rand.Rand
	known  bool
	schema fuzzSchema
	// stored are keys written by an unconditional PutItem and not targeted by a DeleteItem
	// since: items that certainly exist, to resume a Query after.
	stored []map[string]dynamodbtypes.AttributeValue
}

// fuzzSchema is the key layout of the fuzz table. The default fuzz mode rotates through three
// layouts by seed (see fuzzSchemaFor), so that N and B range keys, and an index whose range
// key type differs from the table's, are exercised as well as S keys (#225).
type fuzzSchema struct {
	name string
	// skType is the table's range key (sk) type.
	skType dynamodbtypes.ScalarAttributeType
	// idxRange is the GSI's range key attribute: sk itself for the S layout, a separate
	// attribute r of a different type than sk otherwise.
	idxRange     string
	idxRangeType dynamodbtypes.ScalarAttributeType
}

var (
	fuzzSchemaS = fuzzSchema{"S", dynamodbtypes.ScalarAttributeTypeS, "sk", dynamodbtypes.ScalarAttributeTypeS}
	fuzzSchemaN = fuzzSchema{"N", dynamodbtypes.ScalarAttributeTypeN, "r", dynamodbtypes.ScalarAttributeTypeB}
	fuzzSchemaB = fuzzSchema{"B", dynamodbtypes.ScalarAttributeTypeB, "r", dynamodbtypes.ScalarAttributeTypeN}
)

// fuzzSchemaFor picks the layout of a seed: 0 -> sk S, 1 -> sk N (index range B), 2 -> sk B
// (index range N). A seed always maps to the same layout, so reproduce commands keep working.
func fuzzSchemaFor(seed uint64) fuzzSchema {
	switch seed % 3 {
	case 1:
		return fuzzSchemaN
	case 2:
		return fuzzSchemaB
	default:
		return fuzzSchemaS
	}
}

func (s fuzzSchema) String() string {
	return fmt.Sprintf("pk S, sk %s, GSI %q on g S / %s %s", s.skType, fuzzGSI, s.idxRange, s.idxRangeType)
}

func newFuzzGen(seed uint64, known bool) *fuzzGen {
	return &fuzzGen{
		rng:    rand.New(rand.NewPCG(seed, 0x6d696e6964796e)), //nolint:gosec // deterministic test data
		known:  known,
		schema: fuzzSchemaFor(seed),
	}
}

func (g *fuzzGen) chance(p float64) bool { return g.rng.Float64() < p }

func pick[T any](g *fuzzGen, xs ...T) T { return xs[g.rng.IntN(len(xs))] }

func fzS(s string) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberS{Value: s}
}

func fzN(n string) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberN{Value: n}
}

func (g *fuzzGen) pk() dynamodbtypes.AttributeValue { return fzS(pick(g, "a", "b", "c")) }

// fuzzNumbers stress the numeric key encoding: signs, zero and negative zero, decimals, equal
// spellings of one value, and exponents at the edges of DynamoDB's range (1E-130 .. 9.99E+125,
// at most 38 significant digits).
var fuzzNumbers = []string{
	"-1", "-0", "0", "1", "1.0", "01", "1e0", "1E0", "2", "10", "1E1", "100", "1E2",
	"-1.5", "1.5", "0.5", "0.50", "-0.5", "-10", "-100", "9", "99",
	"1E-130", "-1E-130", "9.99E+125", "-9.99E+125", "1E+125", "1E-129",
	"12345678901234567890123456789012345678", "-12345678901234567890123456789012345678",
	"0.0000000000000000000000000000000000001",
}

// fuzzBinaries stress the binary key encoding: 0x00, 0x01 and 0xFF bytes and values that are
// prefixes of each other. DynamoDB rejects empty key values, so none is empty.
var fuzzBinaries = [][]byte{
	{0x00},
	{0x00, 0x00},
	{0x00, 0x01},
	{0x00, 0xFF},
	{0x01},
	{0x01, 0x00},
	{0x01, 0x01},
	{0x01, 0xFF},
	{0xFF},
	{0xFF, 0x00},
	{0xFF, 0xFF},
	{0xFF, 0xFF, 0x00},
	{0x61},
	{0x61, 0x62},
}

func fzB(b []byte) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberB{Value: b}
}

// typedKey returns a random key value of type t.
func (g *fuzzGen) typedKey(t dynamodbtypes.ScalarAttributeType) dynamodbtypes.AttributeValue {
	switch t {
	case dynamodbtypes.ScalarAttributeTypeN:
		return fzN(pick(g, fuzzNumbers...))
	case dynamodbtypes.ScalarAttributeTypeB:
		return fzB(pick(g, fuzzBinaries...))
	default:
		return g.sk()
	}
}

// skValue returns a random table range key value.
func (g *fuzzGen) skValue() dynamodbtypes.AttributeValue { return g.typedKey(g.schema.skType) }

func (g *fuzzGen) sk() dynamodbtypes.AttributeValue {
	if g.chance(0.2) {
		return fzS(pick(g, "x.y", "x")) // "." in keys must not collide
	}

	return fzS(pick(g, "1", "2", "10", "x", "y"))
}

func (g *fuzzGen) key() map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"pk": g.pk(), "sk": g.skValue()}
}

func (g *fuzzGen) num() string {
	return pick(g, "-1", "0", "1", "2", "3", "10", "1.5")
}

func (g *fuzzGen) str() string { return pick(g, "", "foo", "foobar", "bar") }

func (g *fuzzGen) list() dynamodbtypes.AttributeValue {
	l := make([]dynamodbtypes.AttributeValue, g.rng.IntN(3))
	for i := range l {
		l[i] = fzS(pick(g, "p", "q"))
	}

	return &dynamodbtypes.AttributeValueMemberL{Value: l}
}

func (g *fuzzGen) strSet() dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberSS{Value: g.someOf("x", "y", "z")}
}

// deletableSet is a set for DELETE.
func (g *fuzzGen) deletableSet() dynamodbtypes.AttributeValue { return g.strSet() }

// someOf returns a non-empty random subset of xs, in random order.
func (g *fuzzGen) someOf(xs ...string) []string {
	all := append([]string(nil), xs...)
	g.rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })

	return all[:1+g.rng.IntN(len(all))]
}

func (g *fuzzGen) mapVal() dynamodbtypes.AttributeValue {
	m := map[string]dynamodbtypes.AttributeValue{"k": fzS(g.str())}
	if g.chance(0.5) {
		m["j"] = fzN(g.num())
	}

	if g.chance(0.5) {
		m["ss"] = g.strSet()
	}

	if g.chance(0.5) {
		m["n"] = fzN(g.num())
	}

	return &dynamodbtypes.AttributeValueMemberM{Value: m}
}

// attrValue returns a random value for a named non-key attribute.
func (g *fuzzGen) attrValue(name string) dynamodbtypes.AttributeValue {
	// #213: minidyn accepts a NULL index key, DynamoDB rejects it. Only known mode puts
	// NULL in g.
	if g.chance(0.1) && (g.known || name != "g") {
		return &dynamodbtypes.AttributeValueMemberNULL{Value: true}
	}

	switch name {
	case "n":
		return fzN(g.num())
	case "s":
		return fzS(g.str())
	case "l":
		return g.list()
	case "m":
		return g.mapVal()
	case "ss":
		return g.strSet()
	default: // "g", the GSI hash key
		return fzS(pick(g, "g1", "g2"))
	}
}

var fuzzAttrs = []string{"n", "s", "l", "m", "ss", "g"}

func (g *fuzzGen) item() map[string]dynamodbtypes.AttributeValue {
	it := g.key()

	for _, a := range fuzzAttrs {
		if g.chance(0.6) {
			it[a] = g.attrValue(a)
		}
	}

	if g.schema.idxRange != "sk" && g.chance(0.7) {
		it[g.schema.idxRange] = g.typedKey(g.schema.idxRangeType)
	}

	return it
}

// fuzzExpr accumulates ExpressionAttributeNames / Values for one request.
type fuzzExpr struct {
	names  map[string]string
	values map[string]dynamodbtypes.AttributeValue
}

func newFuzzExpr() *fuzzExpr {
	return &fuzzExpr{names: map[string]string{}, values: map[string]dynamodbtypes.AttributeValue{}}
}

func (e *fuzzExpr) name(attr string) string {
	e.names["#"+attr] = attr

	return "#" + attr
}

func (e *fuzzExpr) val(v dynamodbtypes.AttributeValue) string {
	p := ":v" + strconv.Itoa(len(e.values))
	e.values[p] = v

	return p
}

func (e *fuzzExpr) namesOrNil() map[string]string {
	if len(e.names) == 0 {
		return nil
	}

	return e.names
}

func (e *fuzzExpr) valuesOrNil() map[string]dynamodbtypes.AttributeValue {
	if len(e.values) == 0 {
		return nil
	}

	return e.values
}

// condition returns a random condition or filter expression over item attributes; write
// conditions (write=true) may refer to the key attributes.
func (g *fuzzGen) condition(e *fuzzExpr, write bool) string {
	fns := true
	// #164: minidyn accepts filters on key attributes (g is the index's hash key).
	keys := g.known || write
	c := g.atom(e, fns, keys)

	switch g.rng.IntN(6) {
	case 0:
		return c + " AND " + g.atom(e, fns, keys)
	case 1:
		return c + " OR " + g.atom(e, fns, keys)
	case 2:
		return "NOT (" + c + ")"
	default:
		return c
	}
}

// atom returns one comparison or, if fns, possibly a function call. With keys=false it
// does not refer to the index key attribute g.
func (g *fuzzGen) atom(e *fuzzExpr, fns, keys bool) string {
	attr := pick(g, fuzzAttrs...)
	if !keys && attr == "g" {
		attr = "s"
	}

	n := 5
	if fns {
		n = 10
	}

	switch g.rng.IntN(n) {
	case 0:
		return "attribute_exists(" + e.name(attr) + ")"
	case 1:
		return "attribute_not_exists(" + e.name(attr) + ")"
	case 2:
		return e.name("n") + " " + pick(g, "=", "<>", "<", "<=", ">", ">=") + " " + e.val(fzN(g.num()))
	case 3:
		hi := g.num()
		if !g.known && strings.HasPrefix(hi, "-") {
			hi = "0" // #164: minidyn accepts BETWEEN with lower > upper
		}

		return e.name("n") + " BETWEEN " + e.val(fzN("0")) + " AND " + e.val(fzN(hi))
	case 4:
		return e.name("s") + " " + pick(g, "=", "<>", "<", ">") + " " + e.val(fzS(g.str()))
	case 5:
		return "begins_with(" + e.name("s") + ", " + e.val(fzS(pick(g, "foo", "b", ""))) + ")"
	case 6:
		return "size(" + e.name(pick(g, "l", "s", "ss", "m")) + ") " + pick(g, ">", "=", "<") + " " + e.val(fzN(pick(g, "0", "1", "2")))
	case 7:
		return "contains(" + e.name(pick(g, "ss", "s", "l")) + ", " + e.val(fzS(pick(g, "x", "foo", "p"))) + ")"
	case 8:
		return "attribute_type(" + e.name(attr) + ", " + e.val(fzS(pick(g, "N", "S", "L", "M", "SS", "NULL"))) + ")"
	default:
		return e.name("m") + "." + e.name("k") + " = " + e.val(fzS(g.str()))
	}
}

// next returns a random operation.
func (g *fuzzGen) next() fuzzOp {
	switch g.rng.IntN(10) {
	case 0, 1, 2:
		return g.putItem()
	case 3, 4:
		return g.updateItem()
	case 5:
		return g.deleteItem()
	case 6:
		return g.getItem()
	case 7, 8:
		return g.query()
	default:
		return g.scan()
	}
}

func (g *fuzzGen) putItem() fuzzOp {
	in := &dynamodb.PutItemInput{TableName: aws.String(fuzzTable), Item: g.item()}

	if g.chance(0.3) {
		e := newFuzzExpr()
		in.ConditionExpression = aws.String(g.condition(e, true))
		in.ExpressionAttributeNames = e.namesOrNil()
		in.ExpressionAttributeValues = e.valuesOrNil()
	} else {
		g.forget(in.Item) // replaces an equal spelling of the key, if any
		g.stored = append(g.stored, map[string]dynamodbtypes.AttributeValue{"pk": in.Item["pk"], "sk": in.Item["sk"]})
	}

	if g.chance(0.3) {
		in.ReturnValues = dynamodbtypes.ReturnValueAllOld
	}

	return fuzzOp{"PutItem", func(ctx context.Context, c *dynamodb.Client) { _, _ = c.PutItem(ctx, in) }}
}

// updateAction returns one update action, its clause keyword (SET, ADD, DELETE, REMOVE) and
// the document path it writes.
func (g *fuzzGen) updateAction(e *fuzzExpr) (kw, action, path string) {
	kw, action = g.rawUpdateAction(e)
	path = strings.ReplaceAll(strings.Fields(action)[0], "#", "")

	return kw, action, strings.TrimSuffix(path, "[0]")
}

func (g *fuzzGen) rawUpdateAction(e *fuzzExpr) (string, string) {
	if r := g.schema.idxRange; r != "sk" && g.chance(0.15) {
		// Move the item inside the index, or drop it from it.
		if g.chance(0.7) {
			return "SET", e.name(r) + " = " + e.val(g.typedKey(g.schema.idxRangeType))
		}

		return "REMOVE", e.name(r)
	}

	switch g.rng.IntN(12) {
	case 0:
		return "SET", e.name("n") + " = " + e.name("n") + " " + pick(g, "+", "-") + " " + e.val(fzN(g.num()))
	case 1:
		return "SET", e.name("n") + " = if_not_exists(" + e.name("n") + ", " + e.val(fzN(g.num())) + ")"
	case 2:
		return "SET", e.name("s") + " = " + e.val(fzS(g.str()))
	case 3:
		return "SET", e.name("l") + " = list_append(" + e.name("l") + ", " + e.val(g.list()) + ")"
	case 4:
		return "SET", e.name("m") + "." + e.name("k") + " = " + e.val(fzS(g.str()))
	case 5:
		if g.chance(0.5) {
			return "ADD", e.name("n") + " " + e.val(fzN(g.num()))
		}

		return "ADD", e.name("ss") + " " + e.val(g.strSet())
	case 6:
		return "DELETE", e.name("ss") + " " + e.val(g.deletableSet())
	case 7:
		if g.known {
			return "REMOVE", e.name(pick(g, "n", "s", "l", "m", "ss"))
		}

		if g.chance(0.6) {
			// Not m: REMOVE m would overlap the m.k and m.j paths (#164). Removing g
			// drops the item from the by-g index.
			return "REMOVE", e.name(pick(g, "n", "s", "l", "ss", "g"))
		}

		return "REMOVE", e.name("m") + "." + e.name("j")
	case 8:
		return "REMOVE", e.name("l") + "[0]"
	case 9:
		// Nested DELETE/ADD, which minidyn used to ignore (#184). The map may lack the key.
		if g.chance(0.5) {
			return "DELETE", e.name("m") + "." + e.name("ss") + " " + e.val(g.deletableSet())
		}

		return "ADD", e.name("m") + "." + e.name("ss") + " " + e.val(g.strSet())
	case 10:
		return "ADD", e.name("m") + "." + e.name("n") + " " + e.val(fzN(g.num()))
	default:
		return "SET", e.name("g") + " = " + e.val(fzS(pick(g, "g1", "g2")))
	}
}

// updateExpression returns 1-3 actions grouped by clause. Outside known mode each path is
// written once (#164: minidyn accepts overlapping paths).
func (g *fuzzGen) updateExpression(e *fuzzExpr) string {
	clauses := map[string][]string{}
	paths := map[string]bool{}

	for range 1 + g.rng.IntN(3) {
		kw, action, path := g.updateAction(e)
		if paths[path] && !g.known {
			continue
		}

		paths[path] = true
		clauses[kw] = append(clauses[kw], action)
	}

	var parts []string

	for _, kw := range []string{"SET", "REMOVE", "ADD", "DELETE"} {
		if len(clauses[kw]) > 0 {
			parts = append(parts, kw+" "+strings.Join(clauses[kw], ", "))
		}
	}

	return strings.Join(parts, " ")
}

func (g *fuzzGen) updateItem() fuzzOp {
	e := newFuzzExpr()
	expr := g.updateExpression(e)

	in := &dynamodb.UpdateItemInput{
		TableName:        aws.String(fuzzTable),
		Key:              g.key(),
		UpdateExpression: aws.String(expr),
		ReturnValues:     pick(g, dynamodbtypes.ReturnValueNone, dynamodbtypes.ReturnValueAllNew),
	}

	if g.chance(0.4) {
		in.ReturnValues = pick(g, dynamodbtypes.ReturnValueAllOld, dynamodbtypes.ReturnValueUpdatedNew, dynamodbtypes.ReturnValueUpdatedOld)
	}

	var conds []string

	if g.chance(0.3) {
		conds = append(conds, g.condition(e, true))
	}

	switch len(conds) {
	case 1:
		in.ConditionExpression = aws.String(conds[0])
	case 2:
		in.ConditionExpression = aws.String("(" + conds[0] + ") AND " + conds[1])
	}

	in.ExpressionAttributeNames = e.namesOrNil()
	in.ExpressionAttributeValues = e.valuesOrNil()

	return fuzzOp{"UpdateItem", func(ctx context.Context, c *dynamodb.Client) { _, _ = c.UpdateItem(ctx, in) }}
}

// forget drops a key from g.stored, whatever the spelling of its N or S range key.
func (g *fuzzGen) forget(key map[string]dynamodbtypes.AttributeValue) {
	g.stored = slices.DeleteFunc(g.stored, func(k map[string]dynamodbtypes.AttributeValue) bool {
		return compareKeyValues(k["pk"], key["pk"]) == 0 && compareKeyValues(k["sk"], key["sk"]) == 0
	})
}

func (g *fuzzGen) deleteItem() fuzzOp {
	in := &dynamodb.DeleteItemInput{
		TableName:    aws.String(fuzzTable),
		Key:          g.key(),
		ReturnValues: pick(g, dynamodbtypes.ReturnValueNone, dynamodbtypes.ReturnValueAllOld),
	}
	g.forget(in.Key)

	if g.chance(0.4) {
		e := newFuzzExpr()
		in.ConditionExpression = aws.String(g.condition(e, true))
		in.ExpressionAttributeNames = e.namesOrNil()
		in.ExpressionAttributeValues = e.valuesOrNil()
	}

	return fuzzOp{"DeleteItem", func(ctx context.Context, c *dynamodb.Client) { _, _ = c.DeleteItem(ctx, in) }}
}

func (g *fuzzGen) projection(e *fuzzExpr) *string {
	if !g.chance(0.3) {
		return nil
	}

	parts := []string{e.name("sk")}
	if g.schema.idxRange != "sk" {
		parts = append(parts, e.name(g.schema.idxRange)) // keeps ties on the index order detectable
	}

	paths := []func() string{
		func() string { return e.name("n") },
		func() string { return e.name("m") + "." + e.name("k") },
		func() string { return e.name("ss") },
		func() string { return e.name("l") + "[0]" },
	}

	for _, p := range paths {
		if g.chance(0.5) {
			parts = append(parts, p())
		}
	}

	return aws.String(strings.Join(parts, ", "))
}

func (g *fuzzGen) getItem() fuzzOp {
	e := newFuzzExpr()
	in := &dynamodb.GetItemInput{
		TableName:            aws.String(fuzzTable),
		Key:                  g.key(),
		ProjectionExpression: g.projection(e),
	}
	in.ExpressionAttributeNames = e.namesOrNil()

	return fuzzOp{"GetItem", func(ctx context.Context, c *dynamodb.Client) { _, _ = c.GetItem(ctx, in) }}
}

// skCondition returns an optional range key condition on attr, of key type typ.
func (g *fuzzGen) skCondition(e *fuzzExpr, attr string, typ dynamodbtypes.ScalarAttributeType) string {
	sk := e.name(attr)

	switch g.rng.IntN(5) {
	case 0:
		return ""
	case 1:
		return " AND " + sk + " " + pick(g, "=", "<", "<=", ">", ">=") + " " + e.val(g.typedKey(typ))
	case 2:
		return g.between(e, sk, typ)
	default:
		return g.skPrefixOrCompare(e, sk, typ)
	}
}

// between returns a BETWEEN condition. DynamoDB rejects a lower bound above the upper one,
// which minidyn accepts (#164), so outside known mode the bounds are ordered.
func (g *fuzzGen) between(e *fuzzExpr, sk string, typ dynamodbtypes.ScalarAttributeType) string {
	if typ == dynamodbtypes.ScalarAttributeTypeS {
		return " AND " + sk + " BETWEEN " + e.val(fzS("1")) + " AND " + e.val(g.sk())
	}

	lo, hi := g.typedKey(typ), g.typedKey(typ)
	if !g.known && compareKeyValues(lo, hi) > 0 {
		lo, hi = hi, lo
	}

	return " AND " + sk + " BETWEEN " + e.val(lo) + " AND " + e.val(hi)
}

// skPrefixOrCompare returns begins_with on S and B keys, a comparison on N keys (begins_with
// is not defined for numbers).
func (g *fuzzGen) skPrefixOrCompare(e *fuzzExpr, sk string, typ dynamodbtypes.ScalarAttributeType) string {
	switch typ {
	case dynamodbtypes.ScalarAttributeTypeN:
		return " AND " + sk + " " + pick(g, "<", "<=", ">", ">=") + " " + e.val(g.typedKey(typ))
	case dynamodbtypes.ScalarAttributeTypeB:
		prefix := pick(g, fuzzBinaries...)
		if g.chance(0.5) {
			prefix = prefix[:1+g.rng.IntN(len(prefix))]
		}

		return " AND begins_with(" + sk + ", " + e.val(fzB(prefix)) + ")"
	default:
		prefix := pick(g, "1", "x", "")

		return " AND begins_with(" + sk + ", " + e.val(fzS(prefix)) + ")"
	}
}

// compareKeyValues orders two key values of the same type (N numerically, B bytewise).
func compareKeyValues(a, b dynamodbtypes.AttributeValue) int {
	switch av := a.(type) {
	case *dynamodbtypes.AttributeValueMemberN:
		x, _ := new(big.Rat).SetString(av.Value)
		y, _ := new(big.Rat).SetString(b.(*dynamodbtypes.AttributeValueMemberN).Value)

		return x.Cmp(y)
	case *dynamodbtypes.AttributeValueMemberB:
		return bytes.Compare(av.Value, b.(*dynamodbtypes.AttributeValueMemberB).Value)
	default:
		return strings.Compare(a.(*dynamodbtypes.AttributeValueMemberS).Value, b.(*dynamodbtypes.AttributeValueMemberS).Value)
	}
}

func (g *fuzzGen) query() fuzzOp {
	e := newFuzzExpr()
	in := &dynamodb.QueryInput{
		TableName:        aws.String(fuzzTable),
		ScanIndexForward: aws.Bool(g.chance(0.5)),
	}

	onIndex := g.chance(0.3)
	if onIndex {
		in.IndexName = aws.String(fuzzGSI)
		in.KeyConditionExpression = aws.String(e.name("g") + " = " + e.val(fzS(pick(g, "g1", "g2"))) +
			g.skCondition(e, g.schema.idxRange, g.schema.idxRangeType))
	} else {
		pk := g.pk()
		in.KeyConditionExpression = aws.String(e.name("pk") + " = " + e.val(pk) +
			g.skCondition(e, "sk", g.schema.skType))

		// Resume after a random key of the partition (both directions); with Limit this is
		// paging. Index queries have no Limit (ties) and so no paging.
		g.exclusiveStartKey(in, pk)
	}

	g.readOptions(e, &in.FilterExpression, &in.Limit)

	if onIndex {
		in.Limit = nil // items with equal index keys have no defined order
	}

	if !g.known && in.Limit != nil && strings.Contains(*in.KeyConditionExpression, " AND ") {
		// #153 (intentional divergence, see e2e/README.md): minidyn returns a
		// LastEvaluatedKey when a full page ends exactly at the end of the sort key
		// range, as DynamoDB documents; DynamoDB Local omits it.
		in.KeyConditionExpression = aws.String(strings.SplitN(*in.KeyConditionExpression, " AND ", 2)[0])
	}

	in.ProjectionExpression = g.projection(e)
	in.ExpressionAttributeNames = e.namesOrNil()
	in.ExpressionAttributeValues = e.valuesOrNil()

	return fuzzOp{"Query", func(ctx context.Context, c *dynamodb.Client) { _, _ = c.Query(ctx, in) }}
}

// exclusiveStartKey sometimes resumes a table query after a key, which need not belong to an
// item. Outside known mode, #171: DynamoDB rejects a start key outside the range key
// predicate, minidyn returns an empty page, so there is no range condition with a start key.
func (g *fuzzGen) exclusiveStartKey(in *dynamodb.QueryInput, pk dynamodbtypes.AttributeValue) {
	if !g.chance(0.4) {
		return
	}

	if !g.known && strings.Contains(*in.KeyConditionExpression, " AND ") {
		return
	}

	in.ExclusiveStartKey = map[string]dynamodbtypes.AttributeValue{"pk": pk, "sk": g.skValue()}
}

func (g *fuzzGen) scan() fuzzOp {
	e := newFuzzExpr()
	in := &dynamodb.ScanInput{TableName: aws.String(fuzzTable)}

	if g.chance(0.2) {
		in.IndexName = aws.String(fuzzGSI)
	}

	// No Limit: Scan order is unspecified, so pages can't be compared.
	var noLimit *int32

	g.readOptions(e, &in.FilterExpression, &noLimit)
	in.ProjectionExpression = g.projection(e)
	in.ExpressionAttributeNames = e.namesOrNil()
	in.ExpressionAttributeValues = e.valuesOrNil()

	return fuzzOp{"Scan", func(ctx context.Context, c *dynamodb.Client) { _, _ = c.Scan(ctx, in) }}
}

// readOptions sets an optional filter and an optional limit.
func (g *fuzzGen) readOptions(e *fuzzExpr, filter **string, limit **int32) {
	if g.chance(0.4) {
		*filter = aws.String(g.condition(e, false))
	}

	if g.chance(0.3) {
		*limit = aws.Int32(int32(1 + g.rng.IntN(3))) //nolint:gosec // 1..3
	}
}

// fuzzCreateTable creates the fuzz table with the S layout: pk (S) / sk (S) with GSI by-g on
// g (S) / sk.
func fuzzCreateTable(ctx context.Context, client *dynamodb.Client) error {
	return fuzzCreateTableFor(ctx, client, fuzzSchemaS)
}

// fuzzCreateTableFor creates the fuzz table with a layout: pk (S) / sk (any key type) with GSI
// by-g on g (S) / the layout's index range key.
func fuzzCreateTableFor(ctx context.Context, client *dynamodb.Client, schema fuzzSchema) error {
	attrs := []dynamodbtypes.AttributeDefinition{
		{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		{AttributeName: aws.String("sk"), AttributeType: schema.skType},
		{AttributeName: aws.String("g"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
	}

	if schema.idxRange != "sk" {
		attrs = append(attrs, dynamodbtypes.AttributeDefinition{
			AttributeName: aws.String(schema.idxRange), AttributeType: schema.idxRangeType,
		})
	}

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(fuzzTable),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: dynamodbtypes.KeyTypeRange},
		},
		AttributeDefinitions: attrs,
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName: aws.String(fuzzGSI),
			KeySchema: []dynamodbtypes.KeySchemaElement{
				{AttributeName: aws.String("g"), KeyType: dynamodbtypes.KeyTypeHash},
				{AttributeName: aws.String(schema.idxRange), KeyType: dynamodbtypes.KeyTypeRange},
			},
			Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
		}},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	if err != nil {
		return fmt.Errorf("create fuzz table: %w", err)
	}

	return nil
}
