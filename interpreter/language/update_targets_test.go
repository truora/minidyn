package language

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func assertPrimaryKeyUpdateError(t *testing.T, err error, wantSubstr string) {
	t.Helper()

	if err == nil {
		t.Fatal("expected error")
	}

	if wantSubstr != "" && !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf("error %q does not contain %q", err.Error(), wantSubstr)
	}

	if !strings.Contains(err.Error(), "part of the key") {
		t.Fatalf("error %q missing key suffix", err.Error())
	}
}

func parseUpdateForTest(t *testing.T, expr string) *UpdateStatement {
	t.Helper()

	p := NewUpdateParser(NewLexer(expr))
	stmt := p.ParseUpdateExpression()

	if len(p.Errors()) != 0 {
		return nil
	}

	return stmt
}

func TestValidateUpdateExpressionDoesNotTargetPrimaryKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		expr       string
		aliases    map[string]string
		hashKey    string
		rangeKey   string
		wantErr    bool
		wantSubstr string
	}{
		{
			name:    "empty expression",
			expr:    "   ",
			hashKey: "id",
			wantErr: false,
		},
		{
			name:       "SET partition key literal",
			expr:       "SET id = :new",
			hashKey:    "id",
			wantErr:    true,
			wantSubstr: "Cannot update attribute id",
		},
		{
			name:       "SET partition key via name alias",
			expr:       "SET #pk = :new",
			aliases:    map[string]string{"#pk": "id"},
			hashKey:    "id",
			wantErr:    true,
			wantSubstr: "Cannot update attribute id",
		},
		{
			name:    "SET non-key attribute",
			expr:    "SET type = :t",
			hashKey: "id",
			wantErr: false,
		},
		{
			name:       "REMOVE partition key",
			expr:       "REMOVE id",
			hashKey:    "id",
			wantErr:    true,
			wantSubstr: "Cannot update attribute id",
		},
		{
			name:       "SET sort key when table has range",
			expr:       "SET sk = :s",
			hashKey:    "pk",
			rangeKey:   "sk",
			wantErr:    true,
			wantSubstr: "Cannot update attribute sk",
		},
		{
			name:     "SET attribute same name as range but no range on table",
			expr:     "SET sk = :s",
			hashKey:  "pk",
			rangeKey: "",
			wantErr:  false,
		},
		{
			name:       "nested path root is key",
			expr:       "SET id[0] = :x",
			hashKey:    "id",
			wantErr:    true,
			wantSubstr: "Cannot update attribute id",
		},
		{
			name:    "parse error defers to interpreter",
			expr:    "SET",
			hashKey: "id",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			paths := UpdateTargetPaths(parseUpdateForTest(t, tt.expr), tt.aliases)
			err := ValidatePathsDoNotTargetPrimaryKey(paths, tt.hashKey, tt.rangeKey)
			if tt.wantErr {
				assertPrimaryKeyUpdateError(t, err, tt.wantSubstr)

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestUpdateTargetPaths(t *testing.T) {
	key := func(k string) PathElement { return PathElement{Kind: PathKindMapKey, Key: k} }
	idx := func(i int64) PathElement { return PathElement{Kind: PathKindListIndex, Index: i} }

	tests := []struct {
		name    string
		expr    string
		aliases map[string]string
		want    [][]PathElement
	}{
		{name: "set", expr: "SET a = :v", want: [][]PathElement{{key("a")}}},
		{
			name: "every action", expr: "SET a = :v, b = :v REMOVE c ADD d :v DELETE e :v",
			want: [][]PathElement{{key("a")}, {key("b")}, {key("c")}, {key("d")}, {key("e")}},
		},
		{name: "alias", expr: "SET #a = :v", aliases: map[string]string{"#a": "real"}, want: [][]PathElement{{key("real")}}},
		{name: "nested", expr: "SET m.a.b = :v", want: [][]PathElement{{key("m"), key("a"), key("b")}}},
		{
			name: "nested alias", expr: "REMOVE #m.#a", aliases: map[string]string{"#m": "m", "#a": "x.y"},
			want: [][]PathElement{{key("m"), key("x.y")}},
		},
		{
			name: "list index", expr: "SET l[1] = :v REMOVE m.l[0].k",
			want: [][]PathElement{{key("l"), idx(1)}, {key("m"), key("l"), idx(0), key("k")}},
		},
		// A literal that does not fit an int64 cannot be extracted: the target keeps its prefix.
		{
			name: "out of range index before another target", expr: "SET l[9223372036854775808] = :v, n = :v",
			want: [][]PathElement{{key("l")}, {key("n")}},
		},
		{
			name: "out of range index after another target", expr: "SET n = :v, l[9223372036854775808] = :v",
			want: [][]PathElement{{key("n")}, {key("l")}},
		},
		{
			name: "nested out of range index keeps prefix", expr: "SET m.l[9223372036854775808].k = :v, n = :v",
			want: [][]PathElement{{key("m"), key("l")}, {key("n")}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, UpdateTargetPaths(parseUpdateForTest(t, tt.expr), tt.aliases))
		})
	}

	t.Run("no statement", func(t *testing.T) {
		require.Empty(t, UpdateTargetPaths(nil, nil))
		require.Empty(t, UpdateTargetPaths(parseUpdateForTest(t, ""), nil))
	})
}
