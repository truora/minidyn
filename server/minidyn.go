package server

import (
	"errors"
	"time"

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

// EmulateFailure configures failure injection on the server's in-memory client.
// Every API hard-fails the call with the emulated error, including BatchWriteItem and
// BatchGetItem (the whole batch errors; it does not return partial results). Use
// FailureConditionNone to clear emulation. To leave individual batch sub-requests
// unprocessed instead of failing the whole call, use EmulateUnprocessedItems.
func (s *Server) EmulateFailure(condition FailureCondition) {
	if s == nil || s.client == nil {
		return
	}

	s.client.setFailureCondition(emulatingErrors[condition])
}

// EmulateFailureForTable scopes failure injection to a single table, or to a
// specific index of that table when indexName is provided. Operations targeting
// other tables (or, for an index-scoped failure, other access paths on the same
// table) keep working. A batch (BatchWriteItem/BatchGetItem) that touches the scoped
// table hard-fails the whole call. Passing FailureConditionNone clears the failure for
// that exact table/index scope. The global EmulateFailure still overrides everything.
func (s *Server) EmulateFailureForTable(tableName string, condition FailureCondition, indexName ...string) {
	if s == nil || s.client == nil {
		return
	}

	index := ""
	if len(indexName) > 0 {
		index = indexName[0]
	}

	s.client.setTableFailureCondition(tableName, index, emulatingErrors[condition])
}

// EmulateUnprocessedItems makes BatchWriteItem and BatchGetItem leave selected
// sub-requests of tableName unprocessed (returned in UnprocessedItems/UnprocessedKeys)
// instead of executing them, while the rest of the batch is applied normally. The
// match predicate receives the zero-based index of the sub-request within that table's
// request slice and its raw payload: a PutRequest's full item, or a DeleteRequest/get
// key map. It is sticky until cleared with EmulateUnprocessedItems(tableName, nil) or
// ClearUnprocessedItems. Single-item operations are unaffected. A global or
// table-scoped EmulateFailure overrides this and hard-fails the whole batch.
func (s *Server) EmulateUnprocessedItems(tableName string, match func(n int, raw map[string]*AttributeValue) bool) {
	if s == nil || s.client == nil {
		return
	}

	s.client.setUnprocessedMatcher(tableName, match)
}

// ClearUnprocessedItems removes every batch partial-failure predicate set with
// EmulateUnprocessedItems.
func (s *Server) ClearUnprocessedItems() {
	if s == nil || s.client == nil {
		return
	}

	s.client.clearUnprocessedMatchers()
}

// SetIndexActivationDelay configures how long newly created GSIs report CREATING before ACTIVE.
func (s *Server) SetIndexActivationDelay(delay time.Duration) {
	if s == nil || s.client == nil {
		return
	}

	s.client.setIndexActivationDelay(delay)
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
