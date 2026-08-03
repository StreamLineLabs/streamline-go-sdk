package streamline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// QueryResult represents the result of a SQL query.
type QueryResult struct {
	Columns  []ColumnInfo  `json:"columns"`
	Rows     [][]any       `json:"rows"`
	Metadata QueryMetadata `json:"metadata"`
}

// ColumnInfo describes a result column.
type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// QueryMetadata provides execution statistics.
type QueryMetadata struct {
	ExecutionTimeMs int64 `json:"execution_time_ms"`
	RowsScanned     int64 `json:"rows_scanned"`
	RowsReturned    int   `json:"rows_returned"`
	Truncated       bool  `json:"truncated"`
}

// QueryOptions configures a query request.
type QueryOptions struct {
	// TimeoutMs is the server-side query timeout in milliseconds (default: 30000).
	TimeoutMs int64
	// MaxRows is the maximum number of rows to return (default: 10000).
	MaxRows int
}

// DefaultQueryOptions returns sensible defaults for query execution.
func DefaultQueryOptions() QueryOptions {
	return QueryOptions{
		TimeoutMs: 30000,
		MaxRows:   10000,
	}
}

// QueryClient executes SQL queries against the Streamline HTTP API.
type QueryClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewQueryClient creates a new query client pointing at the Streamline HTTP API.
func NewQueryClient(baseURL string) *QueryClient {
	return &QueryClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}
}

// Query executes a SQL query with default options.
func (c *QueryClient) Query(ctx context.Context, sql string) (*QueryResult, error) {
	return c.QueryWithOptions(ctx, sql, DefaultQueryOptions())
}

// QueryWithOptions executes a SQL query with custom timeout and row limit.
func (c *QueryClient) QueryWithOptions(ctx context.Context, sql string, opts QueryOptions) (_ *QueryResult, err error) {
	if sql == "" {
		return nil, NewConfigurationError("SQL query cannot be empty")
	}

	payload := map[string]any{
		"sql":        sql,
		"timeout_ms": opts.TimeoutMs,
		"max_rows":   opts.MaxRows,
		"format":     "json",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, &StreamlineError{
			Code:    ErrSerialization,
			Message: fmt.Sprintf("marshal query request: %s", err),
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/query", bytes.NewReader(body))
	if err != nil {
		return nil, NewConnectionError(fmt.Sprintf("create request: %s", err), err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, NewConnectionError(fmt.Sprintf("query request failed: %s", err), err)
	}
	defer func() { err = joinClose(err, resp.Body, "close query response body") }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, NewConnectionError(fmt.Sprintf("read response: %s", err), err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &StreamlineError{
			Code:      ErrInternal,
			Message:   fmt.Sprintf("query failed (HTTP %d): %s", resp.StatusCode, string(respBody)),
			Retryable: resp.StatusCode >= 500,
		}
	}

	var result QueryResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, &StreamlineError{
			Code:    ErrSerialization,
			Message: fmt.Sprintf("unmarshal query response: %s", err),
		}
	}
	return &result, nil
}

// Explain returns the query execution plan without running the query.
func (c *QueryClient) Explain(ctx context.Context, sql string) (_ string, err error) {
	if sql == "" {
		return "", NewConfigurationError("SQL query cannot be empty")
	}

	payload := map[string]any{
		"sql":     sql,
		"explain": true,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", &StreamlineError{
			Code:    ErrSerialization,
			Message: fmt.Sprintf("marshal explain request: %s", err),
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/query", bytes.NewReader(body))
	if err != nil {
		return "", NewConnectionError(fmt.Sprintf("create request: %s", err), err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", NewConnectionError(fmt.Sprintf("explain request failed: %s", err), err)
	}
	defer func() { err = joinClose(err, resp.Body, "close explain response body") }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", NewConnectionError(fmt.Sprintf("read response: %s", err), err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", &StreamlineError{
			Code:      ErrInternal,
			Message:   fmt.Sprintf("explain failed (HTTP %d): %s", resp.StatusCode, string(respBody)),
			Retryable: resp.StatusCode >= 500,
		}
	}

	return string(respBody), nil
}
