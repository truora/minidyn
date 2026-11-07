package capacity

import (
	"strings"
	"testing"

	"github.com/truora/minidyn/core"
	"github.com/truora/minidyn/types"
)

// newBillingTable returns a table keyed on id with a by-type GSI (hash type, ALL projection).
func newBillingTable(t *testing.T) *core.Table {
	t.Helper()

	table := core.NewTable("pokemons")
	table.BillingMode = new("PAY_PER_REQUEST")
	table.SetAttributeDefinition([]*types.AttributeDefinition{
		{AttributeName: new("id"), AttributeType: new("S")},
		{AttributeName: new("type"), AttributeType: new("S")},
	})

	if err := table.CreatePrimaryIndex(&types.CreateTableInput{
		KeySchema: []*types.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := table.AddGlobalIndexes([]*types.GlobalSecondaryIndex{{
		IndexName:  new("by-type"),
		KeySchema:  []*types.KeySchemaElement{{AttributeName: "type", KeyType: "HASH"}},
		Projection: &types.Projection{ProjectionType: new("ALL")},
	}}); err != nil {
		t.Fatal(err)
	}

	return table
}

func pokemonItem(id, typ string, pad int) map[string]*types.Item {
	item := map[string]*types.Item{
		"id":   {S: new(id)},
		"name": {S: new(strings.Repeat("x", pad))},
	}
	if typ != "" {
		item["type"] = &types.Item{S: new(typ)}
	}

	return item
}

func lookup(table *core.Table) TableLookup {
	return func(name string) *core.Table {
		if name == table.Name {
			return table
		}

		return nil
	}
}

func TestIndexWritesFollowTheItemThroughTheIndex(t *testing.T) {
	table := newBillingTable(t)
	grass, fire, plain := pokemonItem("1", "grass", 10), pokemonItem("1", "fire", 10), pokemonItem("1", "", 10)

	cases := []struct {
		name          string
		before, after map[string]*types.Item
		want          IndexWrite
		none          bool
	}{
		{name: "enter", before: plain, after: grass, want: IndexWrite{Name: "by-type", Kind: "GSI", Size: Size(grass)}},
		{name: "leave", before: grass, after: plain, want: IndexWrite{Name: "by-type", Kind: "GSI", Size: Size(grass)}},
		{name: "rekey", before: grass, after: fire, want: IndexWrite{Name: "by-type", Kind: "GSI", Size: Size(fire), ReplacedSize: Size(grass)}},
		{name: "unindexed", before: plain, after: plain, none: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := IndexWrites(table, tc.before, tc.after)
			if tc.none {
				if got != nil {
					t.Fatalf("IndexWrites = %#v, want none", got)
				}

				return
			}

			if len(got) != 1 || got[0] != tc.want {
				t.Fatalf("IndexWrites = %#v, want [%#v]", got, tc.want)
			}
		})
	}
}

func TestForItemWrite(t *testing.T) {
	table := newBillingTable(t)
	grass := pokemonItem("1", "grass", 10)

	if ForItemWrite(ModeNone, table, "pokemons", grass, nil, grass) != nil {
		t.Fatal("NONE must report nothing")
	}

	got := ForItemWrite(ModeIndexes, table, "pokemons", grass, nil, grass)
	// base item 1 unit + by-type entry 1 unit
	if got.CapacityUnits != 2 || got.TableUnits != 1 || len(got.Indexes) != 1 || got.Indexes[0].Name != "by-type" {
		t.Fatalf("unexpected consumed capacity: %#v", got)
	}
}

func TestBillWriteWithoutATableChargesTheBaseItemOnly(t *testing.T) {
	grass := pokemonItem("1", "grass", 3000)

	size, indexes := BillWrite(nil, grass, nil, grass)
	if size != Size(grass) || indexes != nil {
		t.Fatalf("BillWrite = %d, %#v", size, indexes)
	}
}

// A write is billed by the item as it was before and as it is after, so the stored item has to
// be measured ahead of the write.
func TestWriteTargetBillsTheItemBeforeAndAfter(t *testing.T) {
	big, small := pokemonItem("1", "grass", 5000), pokemonItem("1", "grass", 10)
	idKey := map[string]*types.Item{"id": {S: new("1")}}

	put := func(t *testing.T, table *core.Table, item map[string]*types.Item) {
		t.Helper()

		if _, err := table.Put(&types.PutItemInput{Item: item}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("delete bills the removed item and its index entry", func(t *testing.T) {
		table := newBillingTable(t)
		put(t, table, big)

		target := WriteTarget{Table: "pokemons", Kind: WriteDelete, Key: idKey}
		target.Snapshot(lookup(table))

		if _, err := table.Delete(&types.DeleteItemInput{Key: idKey}); err != nil {
			t.Fatal(err)
		}

		size, indexes := target.Bill(lookup(table))
		if size != Size(big) || len(indexes) != 1 || indexes[0].Size != Size(big) {
			t.Fatalf("Bill = %d, %#v", size, indexes)
		}
	})

	t.Run("put over a larger item bills the item before", func(t *testing.T) {
		table := newBillingTable(t)
		put(t, table, big)

		target := WriteTarget{Table: "pokemons", Kind: WritePut, Key: small}
		target.Snapshot(lookup(table))
		put(t, table, small)

		if size, _ := target.Bill(lookup(table)); size != Size(big) {
			t.Fatalf("size = %d, want %d", size, Size(big))
		}
	})

	t.Run("update bills the larger of the stored item before and after", func(t *testing.T) {
		table := newBillingTable(t)
		put(t, table, small)

		target := WriteTarget{Table: "pokemons", Kind: WriteUpdate, Key: idKey}
		target.Snapshot(lookup(table))
		put(t, table, big) // the update grew the stored item

		if size, _ := target.Bill(lookup(table)); size != Size(big) {
			t.Fatalf("size = %d, want %d", size, Size(big))
		}
	})

	t.Run("condition check is billed by its key", func(t *testing.T) {
		table := newBillingTable(t)
		put(t, table, big)

		target := WriteTarget{Table: "pokemons", Kind: WriteCheck, Key: idKey}
		target.Snapshot(lookup(table))

		size, indexes := target.Bill(lookup(table))
		if size != Size(idKey) || indexes != nil {
			t.Fatalf("Bill = %d, %#v", size, indexes)
		}
	})

	t.Run("unknown table is billed by size alone", func(t *testing.T) {
		target := WriteTarget{Table: "missing", Kind: WritePut, Key: big}
		target.Snapshot(lookup(newBillingTable(t)))

		size, indexes := target.Bill(lookup(newBillingTable(t)))
		if size != Size(big) || indexes != nil {
			t.Fatalf("Bill = %d, %#v", size, indexes)
		}
	})
}
