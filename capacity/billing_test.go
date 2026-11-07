package capacity

import (
	"strings"
	"testing"

	"github.com/truora/minidyn/types"
)

func billingItem(id string, pad int) map[string]*types.Item {
	return map[string]*types.Item{
		"id":   {S: &id},
		"name": {S: strPtr(strings.Repeat("x", pad))},
	}
}

func strPtr(s string) *string { return &s }

func TestWriteBilling(t *testing.T) {
	key := billingItem("k", 0)
	big := billingItem("k", 5000)
	small := billingItem("k", 10)

	tests := []struct {
		name        string
		before      map[string]*types.Item
		after       map[string]*types.Item
		wantSize    int
		wantIndexed map[string]*types.Item
	}{
		{"delete bills the deleted item", big, nil, Size(big), big},
		{"update that shrinks bills the item before", big, small, Size(big), small},
		{"update that grows bills the item after", small, big, Size(big), big},
		{"put of a new item bills the item written", nil, big, Size(big), big},
		{"delete of a missing item bills the key", nil, nil, Size(key), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, indexed := WriteBilling(key, tt.before, tt.after)
			if size != tt.wantSize {
				t.Errorf("size = %d, want %d", size, tt.wantSize)
			}

			if len(indexed) != len(tt.wantIndexed) || (len(indexed) > 0 && Size(indexed) != Size(tt.wantIndexed)) {
				t.Errorf("indexed = %v, want %v", indexed, tt.wantIndexed)
			}
		})
	}
}
