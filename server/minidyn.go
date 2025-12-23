package server

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// FailureCondition describe the failure condition to emulate.
type FailureCondition string

const (
	// FailureConditionNone emulates the system working normally.
	FailureConditionNone FailureCondition = "none"
	// FailureConditionInternalServerError emulates DynamoDB internal error.
	FailureConditionInternalServerError FailureCondition = "internal_server"
	// FailureConditionDeprecated keeps compatibility with previous forced failure.
	FailureConditionDeprecated FailureCondition = "deprecated"
)

var (
	emulatedInternalServerError = &ddbtypes.InternalServerError{Message: aws.String("emulated error")}
	// ErrForcedFailure when the error is forced (deprecated).
	ErrForcedFailure = errors.New("forced failure response")

	emulatingErrors = map[FailureCondition]error{
		FailureConditionNone:                nil,
		FailureConditionInternalServerError: emulatedInternalServerError,
		FailureConditionDeprecated:          ErrForcedFailure,
	}
)

// EmulateFailure forces the HTTP server to fail on subsequent operations.
func (s *Server) EmulateFailure(condition FailureCondition) {
	if s == nil || s.client == nil {
		return
	}

	s.client.setFailureCondition(emulatingErrors[condition])
}
