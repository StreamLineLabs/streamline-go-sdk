package streamline

import (
	"bytes"
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

// QueryClient executes SQL queries against the Streamline API.
type QueryClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewQueryClient creates a new query client.
func NewQueryClient(baseURL string) *QueryClient {
	return &QueryClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}
}

// Query executes a SQL query and returns results.
func (c *QueryClient) Query(sql string) (*QueryResult, error) {
	return c.QueryWithOptions(sql, 30000, 10000)
}

// QueryWithOptions executes a SQL query with custom timeout and row limit.
func (c *QueryClient) QueryWithOptions(sql string, timeoutMs int64, maxRows int) (*QueryResult, error) {
	payload := map[string]any{
		"sql":        sql,
		"timeout_ms": timeoutMs,
		"max_rows":   maxRows,
		"format":     "json",
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	resp, err := c.httpClient.Post(c.baseURL+"/api/v1/query", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("query request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var result QueryResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return &result, nil
}
