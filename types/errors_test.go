//revive:disable-next-line var-naming // keep same package for white-box testing
package types

import (
	"errors"
	"strings"
	"testing"
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
