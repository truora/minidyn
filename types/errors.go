package types

import (
	"encoding/hex"
	"fmt"
)

// An Error wraps lower level errors with code, message and an original error.
// The underlying concrete error type may also satisfy other interfaces which
// can be to used to obtain more specific information about the error.
type Error interface {
	error

	Code() string
	Message() string
	OrigErr() error
}

// BatchedErrors is a batch of errors which also wraps lower level errors with
// code, message, and original errors. Calling Error() will include all errors
// that occurred in the batch.
type BatchedErrors interface {
	Error
	OrigErrs() []error
}

// NewError returns an Error object described by the code, message, and origErr.
// If origErr satisfies the Error interface it will not be wrapped within a new
// Error object and will instead be returned.
func NewError(code, message string, origErr error) Error {
	var errs []error
	if origErr != nil {
		errs = append(errs, origErr)
	}

	return newBaseError(code, message, errs)
}

// NewBatchError returns an BatchedErrors with a collection of errors as an
// array of errors.
func NewBatchError(code, message string, errs []error) BatchedErrors {
	return newBaseError(code, message, errs)
}

// A RequestFailure is an interface to extract request failure information from
// an Error such as the request ID of the failed request returned by a service.
// RequestFailures may not always have a requestID value if the request failed
// prior to reaching the service such as a connection error.
type RequestFailure interface {
	Error
	StatusCode() int
	RequestID() string
}

// NewRequestFailure returns a wrapped error with additional information for
// request status code, and service requestID.
func NewRequestFailure(err Error, statusCode int, reqID string) RequestFailure {
	return newRequestError(err, statusCode, reqID)
}

// UnmarshalError provides the interface for the SDK failing to unmarshal data.
type UnmarshalError interface {
	awsError
	Bytes() []byte
}

// SprintError returns a string of the formatted error code.
func SprintError(code, message, extra string, origErr error) string {
	msg := fmt.Sprintf("%s: %s", code, message)
	if extra != "" {
		msg = fmt.Sprintf("%s\n\t%s", msg, extra)
	}

	if origErr != nil {
		msg = fmt.Sprintf("%s\ncaused by: %s", msg, origErr.Error())
	}

	return msg
}

// A baseError wraps the code and message which defines an error. It also
// can be used to wrap an original error object.
type baseError struct {
	code    string
	message string
	errs    []error
}

// newBaseError returns an error object for the code, message, and errors.
func newBaseError(code, message string, origErrs []error) *baseError {
	b := &baseError{
		code:    code,
		message: message,
		errs:    origErrs,
	}

	return b
}

// Error returns the string representation of the error.
func (b baseError) Error() string {
	size := len(b.errs)
	if size > 0 {
		return SprintError(b.code, b.message, "", errorList(b.errs))
	}

	return SprintError(b.code, b.message, "", nil)
}

// String returns the string representation of the error.
// Alias for Error to satisfy the stringer interface.
func (b baseError) String() string {
	return b.Error()
}

// Code returns the short phrase depicting the classification of the error.
func (b baseError) Code() string {
	return b.code
}

// Message returns the error details message.
func (b baseError) Message() string {
	return b.message
}

// OrigErr returns the original error if one was set. Nil is returned if no
// error was set. This only returns the first element in the list. If the full
// list is needed, use BatchedErrors.
func (b baseError) OrigErr() error {
	switch len(b.errs) {
	case 0:
		return nil
	case 1:
		return b.errs[0]
	default:
		if err, ok := b.errs[0].(Error); ok {
			return NewBatchError(err.Code(), err.Message(), b.errs[1:])
		}

		return NewBatchError("BatchedErrors",
			"multiple errors occurred", b.errs)
	}
}

// OrigErrs returns the original errors if one was set. An empty slice is
// returned if no error was set.
func (b baseError) OrigErrs() []error {
	return b.errs
}

// So that the Error interface type can be included as an anonymous field
// in the requestError struct and not conflict with the error.Error() method.
type awsError Error

// A requestError wraps a request or service error.
type requestError struct {
	awsError
	statusCode int
	requestID  string
	bytes      []byte
}

// newRequestError returns a wrapped error with additional information for
// request status code, and service requestID.
func newRequestError(err Error, statusCode int, requestID string) *requestError {
	return &requestError{
		awsError:   err,
		statusCode: statusCode,
		requestID:  requestID,
	}
}

// Error returns the string representation of the error.
// Satisfies the error interface.
func (r requestError) Error() string {
	extra := fmt.Sprintf("status code: %d, request id: %s",
		r.statusCode, r.requestID)
	return SprintError(r.Code(), r.Message(), extra, r.OrigErr())
}

// String returns the string representation of the error.
// Alias for Error to satisfy the stringer interface.
func (r requestError) String() string {
	return r.Error()
}

// StatusCode returns the wrapped status code for the error
func (r requestError) StatusCode() int {
	return r.statusCode
}

// RequestID returns the wrapped requestID
func (r requestError) RequestID() string {
	return r.requestID
}

// OrigErrs returns the original errors if one was set. An empty slice is
// returned if no error was set.
func (r requestError) OrigErrs() []error {
	if b, ok := r.awsError.(BatchedErrors); ok {
		return b.OrigErrs()
	}

	return []error{r.OrigErr()}
}

type unmarshalError struct {
	awsError
	bytes []byte
}

// Error returns the string representation of the error.
// Satisfies the error interface.
func (e unmarshalError) Error() string {
	extra := hex.Dump(e.bytes)
	return SprintError(e.Code(), e.Message(), extra, e.OrigErr())
}

// String returns the string representation of the error.
// Alias for Error to satisfy the stringer interface.
func (e unmarshalError) String() string {
	return e.Error()
}

// Bytes returns the bytes that failed to unmarshal.
func (e unmarshalError) Bytes() []byte {
	return e.bytes
}

// An error list that satisfies the golang interface
type errorList []error

// Error returns the string representation of the error.
//
// Satisfies the error interface.
func (e errorList) Error() string {
	msg := ""
	// How do we want to handle the array size being zero
	if size := len(e); size > 0 {
		for i := 0; i < size; i++ {
			msg += e[i].Error()
			// We check the next index to see if it is within the slice.
			// If it is, then we append a newline. We do this, because unit tests
			// could be broken with the additional '\n'
			if i+1 < size {
				msg += "\n"
			}
		}
	}

	return msg
}
