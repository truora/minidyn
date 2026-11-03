package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/logging"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/truora/minidyn/server"
)

const dynamoDBLocalImage = "amazon/dynamodb-local:latest"

// setupMinidynEndpoint starts an httptest.Server with server.NewServer and returns its URL.
func setupMinidynEndpoint(t *testing.T) string {
	t.Helper()

	srv := server.NewServer()
	srv.SetIndexActivationDelay(5 * time.Millisecond)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	return ts.URL
}

// localEndpointEnv lets a child test process (see runKnownIssue) reuse the parent's
// DynamoDB Local container instead of starting another one.
const localEndpointEnv = "MINIDYN_E2E_LOCAL_ENDPOINT"

// childEnv marks the child process that executes a known-issue case for real (see
// RunE2EKnown in known_issue_test.go). It lives here because setup.go is not a test file.
const childEnv = "MINIDYN_E2E_KNOWN_CHILD"

var (
	localOnce      sync.Once
	localEndpoint  string
	localStartErr  error
	localContainer testcontainers.Container
)

// sharedDynamoDBLocalEndpoint returns the endpoint of a DynamoDB Local container that is
// started once per test process and reused by every parity test. Starting a container per
// subtest dominated the suite's runtime. The container is stopped by TestMain through
// terminateSharedDynamoDBLocal (testcontainers' reaper is the fallback if the process dies).
func sharedDynamoDBLocalEndpoint(t *testing.T) string {
	t.Helper()

	// Only a known-issue child process reuses its parent's container; a stale value exported
	// in a developer's shell is ignored.
	if ep := os.Getenv(localEndpointEnv); ep != "" && os.Getenv(childEnv) != "" {
		return ep
	}

	skipIfDockerUnavailable(t)

	localOnce.Do(func() {
		ctx := context.Background()

		ctr, err := testcontainers.Run(ctx, dynamoDBLocalImage,
			testcontainers.WithExposedPorts("8000/tcp"),
			testcontainers.WithWaitStrategy(
				wait.ForListeningPort("8000/tcp").WithStartupTimeout(2*time.Minute),
			),
		)
		if err != nil {
			localStartErr = err

			return
		}

		localContainer = ctr

		host, err := ctr.Host(ctx)
		if err != nil {
			localStartErr = err

			return
		}

		mapped, err := ctr.MappedPort(ctx, "8000")
		if err != nil {
			localStartErr = err

			return
		}

		localEndpoint = fmt.Sprintf("http://%s:%s", host, mapped.Port())

		// Child processes spawned for known-issue checks inherit this.
		_ = os.Setenv(localEndpointEnv, localEndpoint)
	})

	require.NoError(t, localStartErr, "start dynamodb-local")

	return localEndpoint
}

// terminateSharedDynamoDBLocal stops the shared container, if this process started one.
func terminateSharedDynamoDBLocal() {
	if localContainer == nil {
		return
	}

	if err := testcontainers.TerminateContainer(localContainer); err != nil {
		fmt.Fprintf(os.Stderr, "terminate dynamodb-local: %v\n", err)
	}
}

// setupDynamoDBLocalEndpoint returns the shared DynamoDB Local endpoint with all tables
// removed, so each test starts from an empty database like a fresh container.
func setupDynamoDBLocalEndpoint(t *testing.T) string {
	t.Helper()

	endpoint := sharedDynamoDBLocalEndpoint(t)
	client := newSDKClient(t, endpoint)

	dropAllTables(t, client)
	t.Cleanup(func() { dropAllTables(t, client) })

	return endpoint
}

// dropAllTables deletes every table in the database behind client.
func dropAllTables(t *testing.T, client *dynamodb.Client) {
	t.Helper()

	ctx := context.Background()

	for {
		out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
		require.NoError(t, err)

		if len(out.TableNames) == 0 {
			return
		}

		for _, name := range out.TableNames {
			_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(name)})
			require.NoError(t, err, "delete table %q", name)
		}
	}
}

func newSDKClient(t *testing.T, baseURL string) *dynamodb.Client {
	t.Helper()

	return newWrappedSDKClient(t, baseURL, nil)
}

// newWrappedSDKClient is newSDKClient with the HTTP transport wrapped by wrap (if non-nil),
// which lets parity tests record raw responses.
func newWrappedSDKClient(t *testing.T, baseURL string, wrap func(http.RoundTripper) http.RoundTripper) *dynamodb.Client {
	t.Helper()

	ctx := context.Background()

	var transport http.RoundTripper = &http.Transport{
		DisableKeepAlives:   true,
		DisableCompression:  true,
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 1,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 5 * time.Second,
		}).DialContext,
	}

	if wrap != nil {
		transport = wrap(transport)
	}

	httpClient := &http.Client{Transport: transport}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithHTTPClient(httpClient),
		config.WithLogger(logging.Nop{}),
		config.WithClientLogMode(0),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				SessionToken:    "test",
				Source:          "test",
			},
		}),
		config.WithRetryer(func() aws.Retryer { return aws.NopRetryer{} }),
	)
	require.NoError(t, err)

	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(baseURL)
	})
}

func skipIfDockerUnavailable(t *testing.T) {
	t.Helper()

	cmd := exec.Command("docker", "info")
	cmd.Stdout = nil

	cmd.Stderr = nil
	if err := cmd.Run(); err != nil {
		t.Skipf("docker unavailable (needed for DynamoDB Local): %v", err)
	}
}
