package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateItemAttributeValue_nil(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateItemAttributeValue(nil))
}

func TestValidateItemAttributeValue_stringSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		item    *Item
		wantErr string
	}{
		{
			name: "unique SS",
			item: &Item{SS: []*string{new("a"), new("b")}},
		},
		{
			name: "empty SS",
			item: &Item{SS: []*string{}},
		},
		{
			name: "nil SS slice",
			item: &Item{SS: nil},
		},
		{
			name:    "duplicate SS top-level",
			item:    &Item{SS: []*string{new("x"), new("y"), new("x")}},
			wantErr: "One or more parameter values were invalid: Input collection [x, y, x] contains duplicates.",
		},
		{
			name:    "duplicate SS via nil pointers (empty string)",
			item:    &Item{SS: []*string{nil, nil}},
			wantErr: "One or more parameter values were invalid: Input collection [, ] contains duplicates.",
		},
		{
			name: "nested duplicate SS inside M",
			item: &Item{M: map[string]*Item{
				"inner": {SS: []*string{new("p"), new("p")}},
			}},
			wantErr: "One or more parameter values were invalid: Input collection [p, p] contains duplicates.",
		},
		{
			name: "nested duplicate SS inside L",
			item: &Item{L: []*Item{
				{S: new("skip")},
				{SS: []*string{new("a"), new("b"), new("a")}},
			}},
			wantErr: "One or more parameter values were invalid: Input collection [a, b, a] contains duplicates.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateItemAttributeValue(tt.item)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Equal(t, tt.wantErr, err.Error())
		})
	}
}

func TestValidateItemAttributeValue_numberSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		item    *Item
		wantErr string
	}{
		{
			name: "unique NS wire strings",
			item: &Item{NS: []*string{new("1"), new("1.0")}},
		},
		{
			name:    "duplicate NS compares wire encoding only",
			item:    &Item{NS: []*string{new("42"), new("42")}},
			wantErr: "One or more parameter values were invalid: Input collection [42, 42] contains duplicates.",
		},
		{
			name: "empty NS",
			item: &Item{NS: nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateItemAttributeValue(tt.item)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Equal(t, tt.wantErr, err.Error())
		})
	}
}

func TestValidateItemAttributeValue_binarySet(t *testing.T) {
	t.Parallel()

	dup := []byte{1, 2, 3}

	tests := []struct {
		name    string
		item    *Item
		wantErr string
	}{
		{
			name: "unique BS",
			item: &Item{BS: [][]byte{{1}, {2}}},
		},
		{
			name:    "duplicate BS same bytes",
			item:    &Item{BS: [][]byte{dup, {9}, dup}},
			wantErr: "One or more parameter values were invalid: Input collection [AQID, CQ==, AQID] contains duplicates.",
		},
		{
			name: "empty BS",
			item: &Item{BS: [][]byte{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateItemAttributeValue(tt.item)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Equal(t, tt.wantErr, err.Error())
		})
	}
}

func TestValidateItemMap(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateItemMap(nil))
	require.NoError(t, ValidateItemMap(map[string]*Item{}))
	require.NoError(t, ValidateItemMap(map[string]*Item{
		"k": {SS: []*string{new("only")}},
	}))

	err := ValidateItemMap(map[string]*Item{
		"colors": {SS: []*string{new("red"), new("red")}},
	})
	require.Error(t, err)
	require.Equal(t,
		"One or more parameter values were invalid: Input collection [red, red] contains duplicates.",
		err.Error())
}
