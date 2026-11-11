package main

import (
	"reflect"
	"testing"
)

// The full parity suite runs longer than go test's default 10m timeout, so run-e2e passes its
// own unless the caller sets one.
func TestWithDefaultTimeout(t *testing.T) {
	tests := map[string]struct {
		extra []string
		want  []string
	}{
		"none":           {extra: nil, want: []string{"-timeout", defaultTimeout}},
		"other flags":    {extra: []string{"-count=1"}, want: []string{"-timeout", defaultTimeout, "-count=1"}},
		"timeout=":       {extra: []string{"-count=1", "-timeout=5m"}, want: []string{"-count=1", "-timeout=5m"}},
		"timeout spaced": {extra: []string{"-timeout", "5m"}, want: []string{"-timeout", "5m"}},
		"double dash":    {extra: []string{"--timeout=5m"}, want: []string{"--timeout=5m"}},
		"test.timeout":   {extra: []string{"-test.timeout=5m"}, want: []string{"-test.timeout=5m"}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := withDefaultTimeout(tt.extra); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("withDefaultTimeout(%v) = %v, want %v", tt.extra, got, tt.want)
			}
		})
	}
}
