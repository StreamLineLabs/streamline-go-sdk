package streamline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestQueryClient_Query(t *testing.T) {
	expected := QueryResult{
		Columns: []ColumnInfo{
			{Name: "user", Type: "VARCHAR"},
			{Name: "action", Type: "VARCHAR"},
		},
		Rows: [][]any{{"alice", "click"}},
		Metadata: QueryMetadata{
			ExecutionTimeMs: 42,
			RowsScanned:     100,
			RowsReturned:    1,
			Truncated:       false,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/query" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req)
		if req["sql"] == nil || req["sql"] == "" {
			t.Error("expected sql in request body")
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(expected)
	}))
	defer server.Close()

	client := NewQueryClient(server.URL)
	result, err := client.Query(context.Background(), "SELECT * FROM topic('events') LIMIT 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}
	if result.Metadata.ExecutionTimeMs != 42 {
		t.Errorf("expected execution time 42, got %d", result.Metadata.ExecutionTimeMs)
	}
}

func TestQueryClient_QueryWithOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req)

		if req["timeout_ms"].(float64) != 5000 {
			t.Errorf("expected timeout_ms 5000, got %v", req["timeout_ms"])
		}
		if req["max_rows"].(float64) != 3 {
			t.Errorf("expected max_rows 3, got %v", req["max_rows"])
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(QueryResult{
			Columns:  []ColumnInfo{{Name: "id", Type: "INT"}},
			Rows:     [][]any{{1}, {2}, {3}},
			Metadata: QueryMetadata{RowsReturned: 3},
		})
	}))
	defer server.Close()

	client := NewQueryClient(server.URL)
	opts := QueryOptions{TimeoutMs: 5000, MaxRows: 3}
	result, err := client.QueryWithOptions(context.Background(), "SELECT id FROM events", opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}
}

func TestQueryClient_Explain(t *testing.T) {
	expectedPlan := `{"plan": "Scan topic(events) → Filter → Limit 10"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req)

		if req["explain"] != true {
			t.Error("expected explain: true in request")
		}

		w.Write([]byte(expectedPlan))
	}))
	defer server.Close()

	client := NewQueryClient(server.URL)
	plan, err := client.Explain(context.Background(), "SELECT * FROM topic('events') LIMIT 10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan != expectedPlan {
		t.Errorf("unexpected plan: %s", plan)
	}
}

func TestQueryClient_EmptySQL(t *testing.T) {
	client := NewQueryClient("http://localhost:9094")

	_, err := client.Query(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty SQL")
	}
	if !IsStreamlineError(err) {
		t.Errorf("expected StreamlineError, got %T", err)
	}
}

func TestQueryClient_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	client := NewQueryClient(server.URL)
	_, err := client.Query(context.Background(), "SELECT 1")
	if err == nil {
		t.Error("expected error for server error")
	}

	if !IsRetryable(err) {
		t.Error("expected 500 error to be retryable")
	}
}

func TestQueryClient_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	client := NewQueryClient(server.URL)
	_, err := client.Query(context.Background(), "SELECT 1")
	if err == nil {
		t.Error("expected error for invalid JSON response")
	}

	code := GetErrorCode(err)
	if code != ErrSerialization {
		t.Errorf("expected ErrSerialization, got %s", code)
	}
}

func TestQueryClient_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate slow response
		<-r.Context().Done()
	}))
	defer server.Close()

	client := NewQueryClient(server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := client.Query(ctx, "SELECT 1")
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestDefaultQueryOptions(t *testing.T) {
	opts := DefaultQueryOptions()
	if opts.TimeoutMs != 30000 {
		t.Errorf("expected default timeout 30000, got %d", opts.TimeoutMs)
	}
	if opts.MaxRows != 10000 {
		t.Errorf("expected default max rows 10000, got %d", opts.MaxRows)
	}
}
