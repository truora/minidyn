package language

import (
	"strings"
	"testing"
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

			err := ValidateUpdateExpressionDoesNotTargetPrimaryKey(tt.expr, tt.aliases, tt.hashKey, tt.rangeKey)
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
