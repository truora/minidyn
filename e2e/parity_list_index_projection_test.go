package e2e

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// Parity for a ProjectionExpression that names a list element (#262): a list too short for the
// index, a missing list and an index past the end leave the path out of the result, and an
// element that exists is returned in a list holding only the projected elements.

func lpList(values ...string) dynamodbtypes.AttributeValue {
	list := make([]dynamodbtypes.AttributeValue, 0, len(values))
	for _, v := range values {
		list = append(list, fzS(v))
	}

	return &dynamodbtypes.AttributeValueMemberL{Value: list}
}

// lpSig renders a value as text, so a nil element of a list shows and an absent item and an
// empty one compare equal.
func lpSig(av dynamodbtypes.AttributeValue) string {
	switch v := av.(type) {
	case nil:
		return "<nil>"
	case *dynamodbtypes.AttributeValueMemberS:
		return v.Value
	case *dynamodbtypes.AttributeValueMemberL:
		parts := make([]string, 0, len(v.Value))
		for _, e := range v.Value {
			parts = append(parts, lpSig(e))
		}

		return "[" + strings.Join(parts, ",") + "]"
	case *dynamodbtypes.AttributeValueMemberM:
		keys := make([]string, 0, len(v.Value))
		for k := range v.Value {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+":"+lpSig(v.Value[k]))
		}

		return "{" + strings.Join(parts, ",") + "}"
	default:
		return "OTHER"
	}
}

func lpGet(ctx context.Context, t *testing.T, client *dynamodb.Client, projection string) string {
	t.Helper()

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(fuzzTable),
		Key:                  fuzzKey("a", "1"),
		ProjectionExpression: aws.String(projection),
	})
	require.NoError(t, err)

	return lpSig(&dynamodbtypes.AttributeValueMemberM{Value: out.Item})
}

func TestE2E_ListIndexProjection(t *testing.T) {
	withList := func(l dynamodbtypes.AttributeValue) map[string]dynamodbtypes.AttributeValue {
		item := fuzzKey("a", "1")
		if l != nil {
			item["l"] = l
		}

		return item
	}

	tests := []struct {
		name       string
		item       map[string]dynamodbtypes.AttributeValue
		projection string
	}{
		{"ElementInRange", withList(lpList("x", "y", "z")), "l[1]"},
		{"FirstElement", withList(lpList("x", "y", "z")), "l[0]"},
		{"ListTooShort", withList(lpList("x")), "l[1]"},
		{"EmptyList", withList(lpList()), "l[1]"},
		{"MissingList", withList(nil), "l[1]"},
		{"IndexPastTheEnd", withList(lpList("x", "y")), "l[5]"},
		{"TwoElementsInOrder", withList(lpList("x", "y", "z")), "l[0], l[2]"},
		{"TwoElementsOutOfOrder", withList(lpList("x", "y", "z")), "l[2], l[0]"},
		{"NestedList", withList(&dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{lpList("p"), lpList("q", "r")}}), "l[1][1]"},
		{"ElementOfMap", withList(&dynamodbtypes.AttributeValueMemberL{Value: []dynamodbtypes.AttributeValue{
			fzS("x"),
			&dynamodbtypes.AttributeValueMemberM{Value: map[string]dynamodbtypes.AttributeValue{"k": fzS("v")}},
		}}), "l[1].k"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RunE2E(t, func(t *testing.T, client *dynamodb.Client) string {
				t.Helper()
				ctx := context.Background()

				fuzzSetup(ctx, t, client, tt.item)

				return lpGet(ctx, t, client, tt.projection)
			})
		})
	}
}
