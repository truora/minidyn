package core

import (
	"errors"
	"strings"

	"github.com/truora/minidyn/types"
)

const (
	// CancellationReasonCodeNone is the per-item code of a transaction item that did not cause the cancellation.
	CancellationReasonCodeNone = "None"
	// CancellationReasonCodeConditionalCheckFailed is the per-item code of an item whose condition failed.
	CancellationReasonCodeConditionalCheckFailed = "ConditionalCheckFailed"
	// CancellationReasonCodeValidationError is the per-item code of an item DynamoDB rejects as
	// invalid inside a transaction.
	CancellationReasonCodeValidationError = "ValidationError"
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
	return ItemCancellationCodes(i, n, CancellationReasonCodeConditionalCheckFailed)
}

// ItemCancellationCodes returns the cancellation code of each of the n items of a transaction
// in which only item i failed, with code. When i is outside [0, n) no item is blamed and every
// code is None.
func ItemCancellationCodes(i, n int, code string) []string {
	codes := make([]string, n)
	for j := range codes {
		codes[j] = CancellationReasonCodeNone
	}

	if i >= 0 && i < n {
		codes[i] = code
	}

	return codes
}

// CancellationReasonMessage is the per-item message of a transaction item that failed with err:
// DynamoDB's text, without minidyn's detail.
func CancellationReasonMessage(err error) string {
	msg := err.Error()
	if verr, ok := errors.AsType[types.Error](err); ok {
		msg = verr.Message()
	}

	if i := strings.LastIndex(msg, types.DetailSeparator); i >= 0 {
		msg = msg[:i]
	}

	return msg
}

// cancellationCoder is implemented by the validation errors DynamoDB reports, inside a
// transaction, as the cancellation reason of the item instead of a ValidationException.
type cancellationCoder interface {
	error
	CancellationCode() string
}

// cancellingError is a validation error with the cancellation code of its transaction item.
type cancellingError struct {
	inner types.Error
	code  string
}

func (e cancellingError) Error() string   { return e.inner.Error() }
func (e cancellingError) Code() string    { return e.inner.Code() }
func (e cancellingError) Message() string { return e.inner.Message() }
func (e cancellingError) OrigErr() error  { return e.inner.OrigErr() }

func (e cancellingError) CancellationCode() string { return e.code }

// CancellationCodeOf returns the cancellation reason code DynamoDB gives the transaction item
// that failed with err, or "" when err is reported as the error of the whole request.
func CancellationCodeOf(err error) string {
	if c, ok := errors.AsType[cancellationCoder](err); ok {
		return c.CancellationCode()
	}

	return ""
}

// validationFailure is err as a ValidationException. A type mismatch of an index key carries
// the ValidationError code of a transaction item.
func validationFailure(err error) error {
	verr := types.NewError("ValidationException", err.Error(), nil)

	if errors.Is(err, ErrIndexKeyTypeMismatch) {
		return cancellingError{inner: verr, code: CancellationReasonCodeValidationError}
	}

	return verr
}
