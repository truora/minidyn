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

// run-e2e is an explicit parity run, so it tells the e2e tests to fail rather than skip when
// Docker is missing, unless the caller already chose a value.
func TestGoTestEnv(t *testing.T) {
	tests := map[string]struct {
		environ []string
		want    string
	}{
		"unset":           {environ: []string{"HOME=/h"}, want: requireDockerEnv + "=1"},
		"caller opts out": {environ: []string{requireDockerEnv + "=0"}, want: requireDockerEnv + "=0"},
		"caller sets 1":   {environ: []string{requireDockerEnv + "=1"}, want: requireDockerEnv + "=1"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := goTestEnv(tt.environ)

			count := 0

			for _, kv := range got {
				if len(kv) > len(requireDockerEnv) && kv[:len(requireDockerEnv)+1] == requireDockerEnv+"=" {
					count++

					if kv != tt.want {
						t.Fatalf("goTestEnv(%v) has %q, want %q", tt.environ, kv, tt.want)
					}
				}
			}

			if count != 1 {
				t.Fatalf("goTestEnv(%v) sets %s %d times, want once", tt.environ, requireDockerEnv, count)
			}
		})
	}
}
