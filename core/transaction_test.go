package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionCanceledMessage(t *testing.T) {
	c := require.New(t)

	codes := ConditionalCheckFailedCancellationCodes(1, 3)
	c.Equal([]string{"None", "ConditionalCheckFailed", "None"}, codes)
	c.Equal(
		"Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed, None]",
		TransactionCanceledMessage(codes),
	)
}

func TestConditionalCheckFailedCancellationCodes(t *testing.T) {
	none, failed := CancellationReasonCodeNone, CancellationReasonCodeConditionalCheckFailed

	tests := []struct {
		name string
		i, n int
		want []string
	}{
		{"single item", 0, 1, []string{failed}},
		{"first of many", 0, 3, []string{failed, none, none}},
		{"last of many", 2, 3, []string{none, none, failed}},
		{"index equals length", 3, 3, []string{none, none, none}},
		{"index past length", 7, 3, []string{none, none, none}},
		{"negative index", -1, 2, []string{none, none}},
		{"empty transaction", 0, 0, []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ConditionalCheckFailedCancellationCodes(tt.i, tt.n))
		})
	}
}
