//revive:disable-next-line var-naming // keep same package for white-box testing
package types

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewErrorWrapsCodeMessageAndOrig(t *testing.T) {
	orig := errors.New("boom")
	err := NewError("BadRequest", "something failed", orig)

	if err.Code() != "BadRequest" {
		t.Fatalf("expected code BadRequest, got %s", err.Code())
	}
	if err.Message() != "something failed" {
		t.Fatalf("expected message, got %s", err.Message())
	}
	if !errors.Is(err.OrigErr(), orig) {
		t.Fatalf("expected orig error")
	}

	// Error string should include code/message and orig cause
	if got := err.Error(); got == "" || !containsAll(got, []string{"BadRequest", "something failed", "boom"}) {
		t.Fatalf("error string missing parts: %s", got)
	}
}

func TestNewBatchErrorWrapsMultiple(t *testing.T) {
	errs := []error{errors.New("a"), errors.New("b")}
	be := NewBatchError("BatchedErrors", "multiple", errs)

	if len(be.OrigErrs()) != 2 {
		t.Fatalf("expected 2 orig errs, got %d", len(be.OrigErrs()))
	}
}

func TestNewRequestFailure(t *testing.T) {
	base := NewError("BadRequest", "bad", nil)
	rf := NewRequestFailure(base, 400, "req-1")

	if rf.StatusCode() != 400 {
		t.Fatalf("expected status 400, got %d", rf.StatusCode())
	}
	if rf.RequestID() != "req-1" {
		t.Fatalf("expected req id, got %s", rf.RequestID())
	}
	if got := rf.Error(); got == "" || !containsAll(got, []string{"BadRequest", "bad", "status code", "req-1"}) {
		t.Fatalf("error string missing parts: %s", got)
	}
}

func TestErrorListFormatting(t *testing.T) {
	el := errorList{errors.New("one"), errors.New("two")}
	s := el.Error()
	if s != "one\ntwo" {
		t.Fatalf("unexpected format: %q", s)
	}
}

func containsAll(s string, parts []string) bool {
	for _, p := range parts {
		if !strings.Contains(s, p) {
			return false
		}
	}
	return true
}

func TestNewErrorWithoutOrigUsesSprintErrorWithoutCause(t *testing.T) {
	err := NewError("OnlyCode", "only message", nil)
	if got := err.Error(); got == "" || !containsAll(got, []string{"OnlyCode", "only message"}) {
		t.Fatalf("unexpected error string: %q", got)
	}
}

func TestBaseErrorStringMatchesError(t *testing.T) {
	t.Parallel()

	e := NewError("C", "m", errors.New("x"))
	var b *baseError
	require.True(t, errors.As(e, &b))
	require.Equal(t, b.Error(), b.String())
}

func TestBaseErrorOrigErrMultiplePlainErrors(t *testing.T) {
	t.Parallel()

	be := NewBatchError("BatchedErrors", "multi", []error{errors.New("a"), errors.New("b")})
	orig := be.OrigErr()
	var batch *baseError
	require.True(t, errors.As(orig, &batch))
	require.Equal(t, "BatchedErrors", batch.Code())
	require.Equal(t, "multiple errors occurred", batch.Message())
	require.Len(t, batch.OrigErrs(), 2)
}

func TestBaseErrorOrigErrMultipleFirstIsTypesError(t *testing.T) {
	t.Parallel()

	first := NewError("FirstCode", "first msg", nil)
	be := NewBatchError("ignored", "ignored", []error{first, errors.New("second")})
	orig := be.OrigErr()
	var nested *baseError
	require.True(t, errors.As(orig, &nested))
	require.Equal(t, "FirstCode", nested.Code())
	require.Equal(t, "first msg", nested.Message())
	require.Len(t, nested.OrigErrs(), 1)
}

func TestRequestFailureStringMatchesError(t *testing.T) {
	t.Parallel()

	rf := NewRequestFailure(NewError("E", "msg", nil), 418, "rid")
	var rq *requestError
	require.True(t, errors.As(rf, &rq))
	require.Equal(t, rq.Error(), rq.String())
}

func TestRequestFailureOrigErrsBatchedUnderlying(t *testing.T) {
	t.Parallel()

	underlying := NewBatchError("Batch", "batch-msg", []error{errors.New("a"), errors.New("b")})
	rf := NewRequestFailure(underlying, 500, "req-1")
	var rq *requestError
	require.True(t, errors.As(rf, &rq))
	require.Equal(t, underlying.OrigErrs(), rq.OrigErrs())
}

func TestRequestFailureOrigErrsSingleUnderlying(t *testing.T) {
	t.Parallel()

	underlying := NewError("Single", "m", errors.New("orig"))
	rf := NewRequestFailure(underlying, 400, "req-2")
	var rq *requestError
	require.True(t, errors.As(rf, &rq))
	require.Len(t, rq.OrigErrs(), 1)
	require.Equal(t, underlying.OrigErr(), rq.OrigErrs()[0])
}

// nonBatchError implements types.Error but not BatchedErrors, so requestError.OrigErrs
// uses the fallback path ([]error{r.OrigErr()}) instead of delegating to OrigErrs().
type nonBatchError struct {
	code, msg string
	orig      error
}

func (n *nonBatchError) Error() string   { return n.msg }
func (n *nonBatchError) Code() string    { return n.code }
func (n *nonBatchError) Message() string { return n.msg }
func (n *nonBatchError) OrigErr() error  { return n.orig }

func TestRequestFailureOrigErrsNonBatchedUnderlying(t *testing.T) {
	t.Parallel()

	orig := errors.New("leaf")
	underlying := &nonBatchError{code: "X", msg: "y", orig: orig}
	rf := NewRequestFailure(underlying, 503, "req-3")
	var rq *requestError
	require.True(t, errors.As(rf, &rq))
	require.Len(t, rq.OrigErrs(), 1)
	require.Equal(t, orig, rq.OrigErrs()[0])
}

func TestErrorListEmptyString(t *testing.T) {
	t.Parallel()

	var el errorList
	require.Equal(t, "", el.Error())
}

func TestErrorListStringerViaFmt(t *testing.T) {
	t.Parallel()

	el := errorList{errors.New("only")}
	require.Equal(t, "only", fmt.Sprintf("%v", el))
}
