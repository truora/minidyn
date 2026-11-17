package language

import (
	"strings"
	"testing"

	"github.com/truora/minidyn/types"
)

const (
	numberDigits38 = "12345678901234567890123456789012345678"
	precisionError = "DynamoDB only supports precision up to 38 digits"
)

func TestNewNumber(t *testing.T) {
	tests := []struct{ in, want string }{
		{numberDigits38, numberDigits38},
		{"1.0", "1"},
		{"1e2", "100"},
	}

	for _, tt := range tests {
		n, ok := NewNumber(tt.in)
		if !ok || n.Text != tt.want {
			t.Errorf("NewNumber(%q) = %v, %v; want %q", tt.in, n, ok, tt.want)
		}
	}

	if n, ok := NewNumber("abc"); ok || n != nil {
		t.Errorf("NewNumber(abc) = %v, %v; want nil, false", n, ok)
	}
}

func TestNumberAddKeepsPrecision(t *testing.T) {
	n := Number{Text: numberDigits38}

	if obj := n.Add(&Number{Text: "1"}); obj != UNDEFINED {
		t.Fatalf("unexpected result %q", obj.Inspect())
	}

	if n.Text != "12345678901234567890123456789012345679" {
		t.Fatalf("got %s", n.Text)
	}

	errObj, ok := n.Add(&Number{Text: "0.5"}).(*Error)
	if !ok || errObj.Message != precisionError {
		t.Fatalf("expected the precision error, got %v", errObj)
	}

	if n.Text != "12345678901234567890123456789012345679" {
		t.Fatalf("a failed add must not change the number, got %s", n.Text)
	}
}

func TestMapToObjectRejectsInvalidNumbers(t *testing.T) {
	for _, item := range []*types.Item{{N: new("abc")}, {NS: []*string{new("1"), new("")}}} {
		if _, err := MapToObject(item); err == nil {
			t.Errorf("expected an error for %v", item)
		}
	}
}

func TestEvalUpdateArithmeticPrecisionError(t *testing.T) {
	env := NewEnvironment()

	err := env.AddAttributes(map[string]*types.Item{
		":a": {N: new(numberDigits38)},
		":b": {N: new("0.5")},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, input := range []string{"SET :c = :a + :b", "SET :c = :a - :b"} {
		errObj, ok := testEvalUpdate(t, input, env).(*Error)
		if !ok || errObj.Message != precisionError {
			t.Errorf("%s: expected the precision error, got %v", input, errObj)
		}
	}
}

func TestEvalListIndexRejectsFractions(t *testing.T) {
	env := startEvalUpdateEnv(t)
	env.Set(":idx", &Number{Text: "1.5"})

	if result := testEvalUpdate(t, "SET :list[:idx] = :one", env); !isError(result) {
		t.Fatalf("expected an error, got %v", result.Inspect())
	}
}

const (
	overflowError  = "Number overflow. Attempting to store a number with magnitude larger than supported range"
	underflowError = "Number underflow. Attempting to store a number with magnitude smaller than supported range"
)

func TestEvalUpdateArithmeticRangeErrors(t *testing.T) {
	tests := []struct {
		input, a, b, want string
	}{
		{"SET :c = :a + :b", "9e125", "9e125", overflowError},
		{"SET :c = :a - :b", "9e125", "-9e125", overflowError},
		{"SET :c = :a + :b", "9." + strings.Repeat("9", 37) + "e125", "1e88", overflowError},
		{"SET :c = :a - :b", "1.5e-130", "1e-130", underflowError},
		{"SET :c = :a + :b", "-1.5e-130", "1e-130", underflowError},
	}

	for _, tt := range tests {
		env := NewEnvironment()

		if err := env.AddAttributes(map[string]*types.Item{":a": {N: new(tt.a)}, ":b": {N: new(tt.b)}}); err != nil {
			t.Fatal(err)
		}

		errObj, ok := testEvalUpdate(t, tt.input, env).(*Error)
		if !ok || errObj.Message != tt.want {
			t.Errorf("%s with %s, %s: want %q, got %v", tt.input, tt.a, tt.b, tt.want, errObj)
		}
	}
}

func TestNumberAddRangeLimits(t *testing.T) {
	n := Number{Text: "5e125"}

	if obj := n.Add(&Number{Text: "4e125"}); obj != UNDEFINED || n.Text != "9"+strings.Repeat("0", 125) {
		t.Fatalf("the largest magnitudes must add, got %q %v", n.Text, obj)
	}

	errObj, ok := n.Add(&Number{Text: "1e125"}).(*Error)
	if !ok || errObj.Message != overflowError {
		t.Fatalf("expected the overflow error, got %v", errObj)
	}
}
