package moonshot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// stubServer returns an httptest server that responds with the given
// status and JSON body, and records the last received body+path.
type recorded struct {
	method string
	path   string
	body   map[string]any
}

func stubServer(t *testing.T, status int, respBody any) (*httptest.Server, *recorded) {
	t.Helper()
	rec := &recorded{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.method = r.Method
		rec.path = r.URL.Path
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&rec.body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if respBody != nil {
			_ = json.NewEncoder(w).Encode(respBody)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

func TestBranchAdminClient_Create(t *testing.T) {
	srv, rec := stubServer(t, 201, map[string]any{
		"id":            "orders:exp-a",
		"base_topic":    "orders",
		"base_offsets":  []int64{0, 0, 0},
		"created_by":    "tester",
		"created_at_ms": 1000,
		"state":         "Active",
		"write_topic":   "__branch__orders__exp-a",
	})
	c, err := NewBranchAdminClient(Options{HTTPURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Create(context.Background(), "orders", "exp-a", []int64{0, 0, 0}, CreateBranchOptions{CreatedBy: "tester"})
	if err != nil {
		t.Fatal(err)
	}
	if v.ID != "orders:exp-a" || v.BaseTopic != "orders" || v.CreatedBy != "tester" {
		t.Fatalf("unexpected branch: %+v", v)
	}
	if rec.method != "POST" || rec.path != "/api/v1/branches" {
		t.Fatalf("unexpected request: %+v", rec)
	}
	if rec.body["base_topic"] != "orders" || rec.body["name"] != "exp-a" || rec.body["created_by"] != "tester" {
		t.Fatalf("unexpected body: %+v", rec.body)
	}
}

func TestBranchAdminClient_RejectsEmptyTopic(t *testing.T) {
	c, _ := NewBranchAdminClient(Options{HTTPURL: "http://x"})
	_, err := c.Create(context.Background(), "", "x", []int64{0}, CreateBranchOptions{})
	if !errors.Is(err, ErrInvalidArg) {
		t.Fatalf("expected ErrInvalidArg, got %v", err)
	}
}

func TestBranchAdminClient_GetHTTPError(t *testing.T) {
	srv, _ := stubServer(t, 404, map[string]any{"error": "not_found", "message": "missing"})
	c, _ := NewBranchAdminClient(Options{HTTPURL: srv.URL})
	_, err := c.Get(context.Background(), "orders:missing")
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *HTTPError, got %v", err)
	}
	if he.Status != 404 {
		t.Fatalf("expected 404, got %d", he.Status)
	}
}

func TestBranchAdminClient_ListArrayShape(t *testing.T) {
	srv, _ := stubServer(t, 200, []map[string]any{
		{"id": "a:1", "base_topic": "a", "base_offsets": []int64{0}, "created_by": "x", "created_at_ms": 1, "state": "Active", "write_topic": "wt"},
		{"id": "b:1", "base_topic": "b", "base_offsets": []int64{0}, "created_by": "y", "created_at_ms": 2, "state": "Active", "write_topic": "wt2"},
	})
	c, _ := NewBranchAdminClient(Options{HTTPURL: srv.URL})
	out, err := c.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[1].ID != "b:1" {
		t.Fatalf("unexpected list: %+v", out)
	}
}

func TestBranchAdminClient_AppendAndRead(t *testing.T) {
	t.Run("append", func(t *testing.T) {
		srv, rec := stubServer(t, 201, map[string]any{"partition": 0, "offset": 42})
		c, _ := NewBranchAdminClient(Options{HTTPURL: srv.URL})
		r, err := c.Append(context.Background(), "orders:exp-a", 0, "hello")
		if err != nil {
			t.Fatal(err)
		}
		if r.Partition != 0 || r.Offset != 42 {
			t.Fatalf("unexpected: %+v", r)
		}
		if rec.body["partition"] != float64(0) || rec.body["value"] != "hello" {
			t.Fatalf("unexpected body: %+v", rec.body)
		}
		if !strings.HasSuffix(rec.path, "/api/v1/branches/orders:exp-a/messages") {
			t.Fatalf("unexpected path: %s", rec.path)
		}
	})
	t.Run("read branch record", func(t *testing.T) {
		val := "v"
		srv, rec := stubServer(t, 200, map[string]any{"from_base": false, "partition": 0, "offset": 7, "value": val})
		c, _ := NewBranchAdminClient(Options{HTTPURL: srv.URL})
		r, err := c.Read(context.Background(), "orders:exp-a", 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		if r.FromBase || r.Offset != 7 || r.Value == nil || *r.Value != "v" {
			t.Fatalf("unexpected: %+v", r)
		}
		if !strings.Contains(rec.path, "/api/v1/branches/orders:exp-a/messages") {
			t.Fatalf("unexpected path: %s", rec.path)
		}
	})
}

func TestContractsClient_ValidOK(t *testing.T) {
	srv, rec := stubServer(t, 200, map[string]any{"status": "ok", "topic": "orders"})
	c, _ := NewContractsClient(Options{HTTPURL: srv.URL})
	contract := Contract{
		Topic:      "orders",
		Assertions: []ContractAssertion{{Path: "$.id", Expected: "string"}},
	}
	r, err := c.Validate(context.Background(), contract, 0, map[string]any{"id": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Valid || r.Topic != "orders" {
		t.Fatalf("unexpected: %+v", r)
	}
	body, _ := json.Marshal(rec.body["contract"])
	if !strings.Contains(string(body), "\"topic\":\"orders\"") {
		t.Fatalf("contract not in body: %s", body)
	}
}

func TestContractsClient_400Failures(t *testing.T) {
	srv, _ := stubServer(t, 400, map[string]any{
		"status":     "error",
		"topic":      "orders",
		"partition":  0,
		"field_path": "$.id",
		"expected":   "string",
		"actual":     "int",
		"message":    "type mismatch",
	})
	c, _ := NewContractsClient(Options{HTTPURL: srv.URL})
	contract := Contract{Topic: "orders"}
	r, err := c.Validate(context.Background(), contract, 0, map[string]any{"id": 1})
	if err != nil {
		t.Fatal(err)
	}
	if r.Valid || r.FieldPath != "$.id" || r.Expected != "string" || r.Actual != "int" {
		t.Fatalf("unexpected: %+v", r)
	}
}

func TestContractsClient_BytesSentAsString(t *testing.T) {
	srv, rec := stubServer(t, 200, map[string]any{"status": "ok", "topic": "t"})
	c, _ := NewContractsClient(Options{HTTPURL: srv.URL})
	_, _ = c.Validate(context.Background(), Contract{Topic: "t"}, 0, []byte("hi"))
	if rec.body["value"] != "hi" {
		t.Fatalf("expected value=hi, got %+v", rec.body)
	}
}

func TestAttestor_SignVerify(t *testing.T) {
	signSrv, _ := stubServer(t, 200, map[string]any{
		"key_id":         "broker-0",
		"algorithm":      "ed25519",
		"timestamp_ms":   42,
		"payload_sha256": "aa",
		"signature_b64":  "bb",
		"header_name":    "streamline-attest",
		"header_value":   "v1.broker-0.aa.42.bb",
	})
	a, _ := NewAttestor(AttestorOptions{Options: Options{HTTPURL: signSrv.URL}})
	sig, err := a.Sign(context.Background(), SignParams{Topic: "t", Partition: 0, Offset: 1, ValueString: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if sig.SignatureB64 != "bb" || sig.HeaderName != "streamline-attest" {
		t.Fatalf("unexpected sig: %+v", sig)
	}

	verifySrv, _ := stubServer(t, 200, map[string]any{"valid": true})
	a2, _ := NewAttestor(AttestorOptions{Options: Options{HTTPURL: verifySrv.URL}})
	ok, err := a2.Verify(context.Background(), VerifyParams{Topic: "t", SignatureB64: "bb", TimestampMs: 42})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected valid=true")
	}
}

func TestAttestor_RejectsBothValueForms(t *testing.T) {
	a, _ := NewAttestor(AttestorOptions{Options: Options{HTTPURL: "http://x"}})
	_, err := a.Sign(context.Background(), SignParams{Topic: "t", Value: []byte("a"), ValueString: "b"})
	if !errors.Is(err, ErrInvalidArg) {
		t.Fatalf("expected ErrInvalidArg, got %v", err)
	}
}

func TestAttestor_BytesGoToBase64(t *testing.T) {
	srv, rec := stubServer(t, 200, map[string]any{
		"key_id": "k", "algorithm": "ed25519", "timestamp_ms": 1,
		"payload_sha256": "x", "signature_b64": "y",
		"header_name": "streamline-attest", "header_value": "v",
	})
	a, _ := NewAttestor(AttestorOptions{Options: Options{HTTPURL: srv.URL}})
	_, err := a.Sign(context.Background(), SignParams{Topic: "t", Value: []byte("hi")})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := rec.body["value_b64"].(string); got != "aGk=" {
		t.Fatalf("expected value_b64=aGk= got %v", rec.body["value_b64"])
	}
}

func TestSemanticSearch(t *testing.T) {
	srv, rec := stubServer(t, 200, map[string]any{
		"hits":    []map[string]any{{"partition": 1, "offset": 5, "score": 0.9}},
		"took_ms": 12,
	})
	c, _ := NewSemanticSearchClient(Options{HTTPURL: srv.URL})
	r, err := c.Search(context.Background(), "logs", "payment failure", SearchOptions{K: 5})
	if err != nil {
		t.Fatal(err)
	}
	if r.TookMs != 12 || len(r.Hits) != 1 || r.Hits[0].Partition != 1 {
		t.Fatalf("unexpected: %+v", r)
	}
	if !strings.HasSuffix(rec.path, "/topics/logs/search") {
		t.Fatalf("unexpected path: %s", rec.path)
	}
}

func TestSemanticSearch_Validation(t *testing.T) {
	c, _ := NewSemanticSearchClient(Options{HTTPURL: "http://x"})
	_, err := c.Search(context.Background(), "t", "", SearchOptions{})
	if !errors.Is(err, ErrInvalidArg) {
		t.Fatalf("expected ErrInvalidArg, got %v", err)
	}
	_, err = c.Search(context.Background(), "t", "q", SearchOptions{K: 1001})
	if !errors.Is(err, ErrInvalidArg) {
		t.Fatalf("expected ErrInvalidArg, got %v", err)
	}
}

func TestMemoryClient_Remember(t *testing.T) {
	srv, rec := stubServer(t, 200, map[string]any{
		"written": []map[string]any{
			{"topic": "agent-a-episodic", "offset": 1},
			{"topic": "agent-a-semantic", "offset": 2},
		},
	})
	c, _ := NewMemoryClient(Options{HTTPURL: srv.URL})
	out, err := c.Remember(context.Background(), RememberParams{
		AgentID: "a", Kind: MemoryFact, Content: "x", Importance: 0.8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[1].Offset != 2 {
		t.Fatalf("unexpected: %+v", out)
	}
	if rec.body["importance"].(float64) != 0.8 {
		t.Fatalf("unexpected importance: %+v", rec.body)
	}
}

func TestMemoryClient_ProcedureRequiresSkill(t *testing.T) {
	c, _ := NewMemoryClient(Options{HTTPURL: "http://x"})
	_, err := c.Remember(context.Background(), RememberParams{
		AgentID: "a", Kind: MemoryProcedure, Content: "x",
	})
	if !errors.Is(err, ErrInvalidArg) {
		t.Fatalf("expected ErrInvalidArg, got %v", err)
	}
}

func TestMemoryClient_InvalidKind(t *testing.T) {
	c, _ := NewMemoryClient(Options{HTTPURL: "http://x"})
	_, err := c.Remember(context.Background(), RememberParams{
		AgentID: "a", Kind: MemoryKind("nope"), Content: "x",
	})
	if !errors.Is(err, ErrInvalidArg) {
		t.Fatalf("expected ErrInvalidArg, got %v", err)
	}
}

func TestMemoryClient_Recall(t *testing.T) {
	srv, _ := stubServer(t, 200, map[string]any{
		"hits": []map[string]any{
			{"tier": "semantic", "topic": "agent-a-semantic", "offset": 5, "content": "hi", "score": 0.7},
		},
	})
	c, _ := NewMemoryClient(Options{HTTPURL: srv.URL})
	hits, err := c.Recall(context.Background(), RecallParams{AgentID: "a", Query: "q"})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Score != 0.7 || hits[0].Tier != "semantic" {
		t.Fatalf("unexpected: %+v", hits)
	}
}
