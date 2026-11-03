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
