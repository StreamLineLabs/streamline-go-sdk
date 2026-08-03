package streamline_test

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	streamline "github.com/streamlinelabs/streamline-go-sdk/testcontainers"
)

func TestStreamlineContainer(t *testing.T) {
	if os.Getenv("STREAMLINE_TESTCONTAINERS_INTEGRATION") != "1" {
		t.Skip("set STREAMLINE_TESTCONTAINERS_INTEGRATION=1 to run Docker integration")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	options := []streamline.ContainerOption{streamline.WithDebugLogging()}
	if image := os.Getenv("STREAMLINE_TEST_IMAGE"); image != "" {
		options = append(options, streamline.WithImage(image))
	}
	container, err := streamline.RunContainer(ctx, options...)
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}
	defer func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}()

	t.Run("BootstrapServers", func(t *testing.T) {
		servers, err := container.BootstrapServers(ctx)
		if err != nil {
			t.Fatalf("failed to get bootstrap servers: %v", err)
		}
		if servers == "" {
			t.Error("bootstrap servers should not be empty")
		}
		t.Logf("Bootstrap servers: %s", servers)
	})

	t.Run("HTTPEndpoint", func(t *testing.T) {
		endpoint, err := container.HTTPEndpoint(ctx)
		if err != nil {
			t.Fatalf("failed to get HTTP endpoint: %v", err)
		}
		if endpoint == "" {
			t.Error("HTTP endpoint should not be empty")
		}
		t.Logf("HTTP endpoint: %s", endpoint)
	})

	t.Run("HealthCheck", func(t *testing.T) {
		healthURL, err := container.HealthEndpoint(ctx)
		if err != nil {
			t.Fatalf("failed to get health endpoint: %v", err)
		}

		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(healthURL)
		if err != nil {
			t.Fatalf("health check request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected status 200, got %d", resp.StatusCode)
		}
	})

	t.Run("CreateTopic", func(t *testing.T) {
		err := container.CreateTopic(ctx, "test-topic", 3)
		if err != nil {
			t.Fatalf("failed to create topic: %v", err)
		}
	})
}
