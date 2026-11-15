package e2e

import (
	"testing"
)

// recordingTB records whether skipIfDockerUnavailable skipped or failed, without ending the
// test that runs it.
type recordingTB struct {
	testing.TB
	skipped, failed bool
}

func (r *recordingTB) Helper()               {}
func (r *recordingTB) Skipf(string, ...any)  { r.skipped = true }
func (r *recordingTB) Fatalf(string, ...any) { r.failed = true }

// Without Docker, the parity tests skip in a plain go test run, but fail when run-e2e asks for
// Docker (MINIDYN_E2E_REQUIRE_DOCKER=1), so a parity run that tested nothing cannot pass.
func TestSkipIfDockerUnavailable(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // no docker binary: docker info fails

	tests := map[string]struct {
		require     string
		wantSkipped bool
		wantFailed  bool
	}{
		"plain go test":    {require: "", wantSkipped: true},
		"run-e2e":          {require: "1", wantFailed: true},
		"explicit opt-out": {require: "0", wantSkipped: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv(requireDockerEnv, tt.require)

			tb := &recordingTB{TB: t}
			skipIfDockerUnavailable(tb)

			if tb.skipped != tt.wantSkipped || tb.failed != tt.wantFailed {
				t.Fatalf("skipped=%t failed=%t, want skipped=%t failed=%t", tb.skipped, tb.failed, tt.wantSkipped, tt.wantFailed)
			}
		})
	}
}
