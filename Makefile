# Minidyn — common test entrypoints from the repository root.

.PHONY: test e2e test-all

# Unit tests: all packages except parity E2E (same scope as qa-engineer).
test:
	go test $$(go list ./... | grep -v '/e2e$$')

# Parity E2E tests (DynamoDB Local via testcontainers); JSON harness prints failures only.
e2e:
	go run ./tools/run-e2e

# Run unit tests then E2E parity tests.
test-all: test e2e
