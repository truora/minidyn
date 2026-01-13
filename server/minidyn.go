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

	// ErrServerNotInitialized when the server is not initialized
	ErrServerNotInitialized = errors.New("server not initialized")

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

// ClearTable removes all data from a table and its indexes using the in-memory client.
func (s *Server) ClearTable(tableName string) error {
	if s == nil || s.client == nil {
		return ErrServerNotInitialized
	}

	return s.client.ClearTable(tableName)
}

// ClearAllTables removes all data from every table and its indexes using the in-memory client.
func (s *Server) ClearAllTables() error {
	if s == nil || s.client == nil {
		return ErrServerNotInitialized
	}

	s.client.ClearAllTables()

	return nil
}

// Reset removes all tables and indexes from the in-memory client.
func (s *Server) Reset() error {
	if s == nil || s.client == nil {
		return ErrServerNotInitialized
	}

	s.client.Reset()

	return nil
}
