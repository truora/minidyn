package core

import "strings"

const (
	// CancellationReasonCodeNone is the per-item code of a transaction item that did not cause the cancellation.
	CancellationReasonCodeNone = "None"
	// CancellationReasonCodeConditionalCheckFailed is the per-item code of an item whose condition failed.
	CancellationReasonCodeConditionalCheckFailed = "ConditionalCheckFailed"
	// ConditionalCheckFailedReasonMessage is the per-item message DynamoDB gives to a failed condition.
	ConditionalCheckFailedReasonMessage = "The conditional request failed."
)

// TransactionCanceledMessage builds the TransactionCanceledException message
// listing one cancellation code per transaction item, in request order.
func TransactionCanceledMessage(codes []string) string {
	return "Transaction cancelled, please refer cancellation reasons for specific reasons [" +
		strings.Join(codes, ", ") + "]"
}

// ConditionalCheckFailedCancellationCodes returns the cancellation code of each
// of the n items of a transaction in which only item i failed its condition.
// When i is outside [0, n) no item is blamed and every code is None.
func ConditionalCheckFailedCancellationCodes(i, n int) []string {
	codes := make([]string, n)
	for j := range codes {
		codes[j] = CancellationReasonCodeNone
	}

	if i >= 0 && i < n {
		codes[i] = CancellationReasonCodeConditionalCheckFailed
	}

	return codes
}
