package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
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

// setupMinidynClient starts an httptest.Server with server.NewServer and returns
// a DynamoDB API client pointed at it.
func setupMinidynClient(t *testing.T) *dynamodb.Client {
	t.Helper()

	srv := httptest.NewServer(server.NewServer())
	t.Cleanup(srv.Close)

	return newSDKClient(t, srv.URL)
}

// setupDynamoDBLocalClient starts amazon/dynamodb-local in Docker and returns
// a DynamoDB API client pointed at the mapped host port.
func setupDynamoDBLocalClient(t *testing.T) *dynamodb.Client {
	t.Helper()

	skipIfDockerUnavailable(t)

	ctx := context.Background()
	ctr, err := testcontainers.Run(ctx, dynamoDBLocalImage,
		testcontainers.WithExposedPorts("8000/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("8000/tcp").WithStartupTimeout(2*time.Minute),
		),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(ctr); termErr != nil {
			t.Logf("terminate dynamodb-local: %v", termErr)
		}
	})

	host, err := ctr.Host(ctx)
	require.NoError(t, err)

	mapped, err := ctr.MappedPort(ctx, "8000")
	require.NoError(t, err)

	endpoint := fmt.Sprintf("http://%s:%s", host, mapped.Port())

	return newSDKClient(t, endpoint)
}

func newSDKClient(t *testing.T, baseURL string) *dynamodb.Client {
	t.Helper()

	ctx := context.Background()
	httpClient := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:   true,
			DisableCompression:  true,
			MaxIdleConns:        1,
			MaxIdleConnsPerHost: 1,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 5 * time.Second,
			}).DialContext,
		},
	}

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
