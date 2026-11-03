package core

import "strings"

const (
	// CancellationReasonNone is the per-item code of a transaction item that did not cause the cancellation.
	CancellationReasonNone = "None"
	// CancellationReasonConditionalCheckFailed is the per-item code of an item whose condition failed.
	CancellationReasonConditionalCheckFailed = "ConditionalCheckFailed"
	// CancellationReasonConditionalMessage is the per-item message DynamoDB gives to a failed condition.
	CancellationReasonConditionalMessage = "The conditional request failed."
)

// TransactionCanceledMessage builds the TransactionCanceledException message
// listing one cancellation code per transaction item, in request order.
func TransactionCanceledMessage(codes []string) string {
	return "Transaction cancelled, please refer cancellation reasons for specific reasons [" +
		strings.Join(codes, ", ") + "]"
}

// ConditionalCheckFailedCancellationCodes returns the cancellation code of each
// of the n items of a transaction in which only item i failed its condition.
func ConditionalCheckFailedCancellationCodes(i, n int) []string {
	codes := make([]string, n)
	for j := range codes {
		codes[j] = CancellationReasonNone
	}

	codes[i] = CancellationReasonConditionalCheckFailed

	return codes
}
