// Package moonshot exposes thin HTTP clients for the Streamline broker's
// experimental admin and AI APIs (M1 memory, M2 semantic search, M4
// contracts/attestation, M5 branches).
//
// All clients here are **Experimental** and may change shape until the
// underlying broker features reach Beta. They use the Go standard
// net/http package — no extra deps.
package moonshot

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ErrInvalidArg is returned for client-side validation failures.
var ErrInvalidArg = errors.New("moonshot: invalid argument")

// HTTPError represents a non-2xx response from the broker.
type HTTPError struct {
	Method string
	Path   string
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	body := e.Body
	if len(body) > 512 {
		body = body[:512]
	}
	return fmt.Sprintf("%s %s -> HTTP %d: %s", e.Method, e.Path, e.Status, body)
}

// Options configures a moonshot HTTP client.
type Options struct {
	// HTTPURL is the broker's HTTP base URL (e.g. http://localhost:9094).
	HTTPURL string
	// Timeout is the per-request timeout. Defaults to 30s.
	Timeout time.Duration
	// HTTPClient overrides the http.Client used (for tests / custom transport).
	HTTPClient *http.Client
}

type httpBase struct {
	url    string
	client *http.Client
}

func newBase(opts Options) (*httpBase, error) {
	if opts.HTTPURL == "" {
		return nil, fmt.Errorf("%w: HTTPURL is required", ErrInvalidArg)
	}
	c := opts.HTTPClient
	if c == nil {
		timeout := opts.Timeout
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		c = &http.Client{Timeout: timeout}
	}
	return &httpBase{
		url:    strings.TrimRight(opts.HTTPURL, "/"),
		client: c,
	}, nil
}

func (h *httpBase) do(ctx context.Context, method, path string, body any, out any) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal body: %w", err)
		}
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, h.url+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, raw, fmt.Errorf("decode response: %w", err)
		}
	}
	return resp.StatusCode, raw, nil
}

// ---------------------------------------------------------------------------
// M5 — Branches admin
// ---------------------------------------------------------------------------

// BranchView is a single branch as returned by the broker admin API. Mirrors
// the broker's `BranchView` struct (`streamline/src/server/branches_api.rs`).
type BranchView struct {
	ID          string  `json:"id"`
	BaseTopic   string  `json:"base_topic"`
	BaseOffsets []int64 `json:"base_offsets"`
	CreatedBy   string  `json:"created_by"`
	CreatedAtMs uint64  `json:"created_at_ms"`
	State       string  `json:"state"`
	WriteTopic  string  `json:"write_topic"`
}

// BranchAdminClient wraps /api/v1/branches/*.
type BranchAdminClient struct{ *httpBase }

// NewBranchAdminClient constructs a branch admin client.
func NewBranchAdminClient(opts Options) (*BranchAdminClient, error) {
	b, err := newBase(opts)
	if err != nil {
		return nil, err
	}
	return &BranchAdminClient{httpBase: b}, nil
}

// CreateBranchOptions configures Create. CreatedBy is recommended for audit
// trails; if empty, "unknown" is sent.
type CreateBranchOptions struct {
	CreatedBy string
}

// Create creates a new branch. The branch id returned is "<baseTopic>:<name>".
// baseOffsets must have one entry per partition of the base topic.
func (c *BranchAdminClient) Create(ctx context.Context, baseTopic, name string, baseOffsets []int64, opts CreateBranchOptions) (*BranchView, error) {
	if baseTopic == "" || name == "" {
		return nil, fmt.Errorf("%w: baseTopic and name must not be empty", ErrInvalidArg)
	}
	if baseOffsets == nil {
		baseOffsets = []int64{}
	}
	createdBy := opts.CreatedBy
	if createdBy == "" {
		createdBy = "unknown"
	}
	body := map[string]any{
		"base_topic":   baseTopic,
		"name":         name,
		"base_offsets": baseOffsets,
		"created_by":   createdBy,
	}
	var out BranchView
	status, raw, err := c.do(ctx, http.MethodPost, "/api/v1/branches", body, &out)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, &HTTPError{Method: "POST", Path: "/api/v1/branches", Status: status, Body: string(raw)}
	}
	return &out, nil
}

// List returns all branches. The broker returns a top-level JSON array.
func (c *BranchAdminClient) List(ctx context.Context) ([]BranchView, error) {
	status, raw, err := c.do(ctx, http.MethodGet, "/api/v1/branches", nil, nil)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, &HTTPError{Method: "GET", Path: "/api/v1/branches", Status: status, Body: string(raw)}
	}
	var arr []BranchView
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, fmt.Errorf("decode list: %w", err)
	}
	return arr, nil
}

// branchIDPath path-encodes everything except the `:` separator the broker
// uses between base-topic and name (the broker does NOT decode `%3A`).
func branchIDPath(branchID string) string {
	parts := strings.SplitN(branchID, ":", 2)
	if len(parts) == 2 {
		return url.PathEscape(parts[0]) + ":" + url.PathEscape(parts[1])
	}
	return url.PathEscape(branchID)
}

// Get returns a single branch by id ("<baseTopic>:<name>").
func (c *BranchAdminClient) Get(ctx context.Context, branchID string) (*BranchView, error) {
	if branchID == "" {
		return nil, fmt.Errorf("%w: branchID must not be empty", ErrInvalidArg)
	}
	path := "/api/v1/branches/" + branchIDPath(branchID)
	var out BranchView
	status, raw, err := c.do(ctx, http.MethodGet, path, nil, &out)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, &HTTPError{Method: "GET", Path: path, Status: status, Body: string(raw)}
	}
	return &out, nil
}

// Discard removes a branch. The broker responds with 204 No Content.
func (c *BranchAdminClient) Discard(ctx context.Context, branchID string) error {
	if branchID == "" {
		return fmt.Errorf("%w: branchID must not be empty", ErrInvalidArg)
	}
	path := "/api/v1/branches/" + branchIDPath(branchID)
	status, raw, err := c.do(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return &HTTPError{Method: "DELETE", Path: path, Status: status, Body: string(raw)}
	}
	return nil
}

// AppendResult is the broker's response to a successful append.
type AppendResult struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

// Append posts a single UTF-8 record to a branch on the given partition.
// For arbitrary bytes, base64-wrap them yourself; the wire layer is
// intentionally simple.
func (c *BranchAdminClient) Append(ctx context.Context, branchID string, partition int32, value string) (*AppendResult, error) {
	if branchID == "" {
		return nil, fmt.Errorf("%w: branchID must not be empty", ErrInvalidArg)
	}
	path := "/api/v1/branches/" + branchIDPath(branchID) + "/messages"
	body := map[string]any{"partition": partition, "value": value}
	var out AppendResult
	status, raw, err := c.do(ctx, http.MethodPost, path, body, &out)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, &HTTPError{Method: "POST", Path: path, Status: status, Body: string(raw)}
	}
	return &out, nil
}

// ReadResult is one branch read. FromBase indicates whether the record came
// from the base topic (transparent fall-through) or the branch's write topic.
// Value is populated only for branch reads — base reads carry only metadata.
type ReadResult struct {
	FromBase  bool    `json:"from_base"`
	Partition int32   `json:"partition"`
	Offset    int64   `json:"offset"`
	Value     *string `json:"value,omitempty"`
}

// Read fetches the next record on a branch's partition after the given
// logical offset. Pass after=-1 to read the first available record.
func (c *BranchAdminClient) Read(ctx context.Context, branchID string, partition int32, after int64) (*ReadResult, error) {
	if branchID == "" {
		return nil, fmt.Errorf("%w: branchID must not be empty", ErrInvalidArg)
	}
	path := fmt.Sprintf("/api/v1/branches/%s/messages?partition=%d&after=%d", branchIDPath(branchID), partition, after)
	var out ReadResult
	status, raw, err := c.do(ctx, http.MethodGet, path, nil, &out)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, &HTTPError{Method: "GET", Path: path, Status: status, Body: string(raw)}
	}
	return &out, nil
}

// ---------------------------------------------------------------------------
// M4 — Contracts validate
// ---------------------------------------------------------------------------

// ContractAssertion is one assertion in a contract. Path is a JSON pointer
// (e.g. "$.user.id"); Expected is a type label like "string", "int", etc.
type ContractAssertion struct {
	Path     string `json:"path"`
	Expected string `json:"expected"`
}

// Contract is the inline contract dto the broker validates against. There is
// no contract registry — every Validate call carries the contract in full.
type Contract struct {
	Topic      string              `json:"topic"`
	Version    string              `json:"version,omitempty"`
	SchemaID   *int                `json:"schema_id,omitempty"`
	Assertions []ContractAssertion `json:"assertions"`
}

// ContractValidationResult mirrors the broker's `ValidateOk`/`ValidateErr`
// envelopes. Valid is true on HTTP 200; on HTTP 400 the failure fields are
// populated and Valid is false.
type ContractValidationResult struct {
	Valid     bool
	Topic     string
	Partition int32  // populated only on failure
	FieldPath string // populated only on failure
	Expected  string // populated only on failure
	Actual    string // populated only on failure
	Message   string // populated only on failure
	SchemaID  *int   // optional
}

// ContractsClient wraps POST /api/v1/contracts/validate.
type ContractsClient struct{ *httpBase }

// NewContractsClient constructs a contracts client.
func NewContractsClient(opts Options) (*ContractsClient, error) {
	b, err := newBase(opts)
	if err != nil {
		return nil, err
	}
	return &ContractsClient{httpBase: b}, nil
}

// Validate dry-runs a contract against a value on a given partition. The
// value may be any JSON-serializable value, a string, or a []byte (encoded
// as a UTF-8 string). The broker has NO contract registry endpoint — pass
// the contract inline every time.
func (c *ContractsClient) Validate(ctx context.Context, contract Contract, partition int32, value any) (*ContractValidationResult, error) {
	if contract.Topic == "" {
		return nil, fmt.Errorf("%w: contract.Topic must not be empty", ErrInvalidArg)
	}
	if contract.Assertions == nil {
		contract.Assertions = []ContractAssertion{}
	}
	switch v := value.(type) {
	case []byte:
		value = string(v)
	}
	body := map[string]any{
		"contract":  contract,
		"partition": partition,
		"value":     value,
	}
	status, raw, err := c.do(ctx, http.MethodPost, "/api/v1/contracts/validate", body, nil)
	if err != nil {
		return nil, err
	}
	switch status {
	case 200:
		var w struct {
			Status string `json:"status"`
			Topic  string `json:"topic"`
		}
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &w)
		}
		return &ContractValidationResult{Valid: true, Topic: w.Topic}, nil
	case 400:
		var w struct {
			Status    string `json:"status"`
			Topic     string `json:"topic"`
			Partition int32  `json:"partition"`
			FieldPath string `json:"field_path"`
			Expected  string `json:"expected"`
			Actual    string `json:"actual"`
			Message   string `json:"message"`
			SchemaID  *int   `json:"schema_id,omitempty"`
		}
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &w)
		}
		// `status:"error"` is the canonical failure envelope. Any other 400
		// shape is a transport-level error and we surface it as one.
		if w.Status != "error" {
			return nil, &HTTPError{Method: "POST", Path: "/api/v1/contracts/validate", Status: status, Body: string(raw)}
		}
		return &ContractValidationResult{
			Valid:     false,
			Topic:     w.Topic,
			Partition: w.Partition,
			FieldPath: w.FieldPath,
			Expected:  w.Expected,
			Actual:    w.Actual,
			Message:   w.Message,
			SchemaID:  w.SchemaID,
		}, nil
	default:
		return nil, &HTTPError{Method: "POST", Path: "/api/v1/contracts/validate", Status: status, Body: string(raw)}
	}
}

// ---------------------------------------------------------------------------
// M4 — Attestation sign/verify
// ---------------------------------------------------------------------------

// AttestHeader is the canonical attestation header name.
const AttestHeader = "streamline-attest"

// SignedAttestation is the result of a successful sign call.
type SignedAttestation struct {
	KeyID         string `json:"key_id"`
	Algorithm     string `json:"algorithm"`
	TimestampMs   int64  `json:"timestamp_ms"`
	PayloadSHA256 string `json:"payload_sha256"`
	SignatureB64  string `json:"signature_b64"`
	HeaderName    string `json:"header_name"`
	HeaderValue   string `json:"header_value"`
}

// AttestorOptions configures the Attestor.
type AttestorOptions struct {
	Options
	KeyID     string // default key id (defaults to "broker-0")
	Algorithm string // default algorithm (defaults to "ed25519")
}

// Attestor wraps /api/v1/attest and /api/v1/attest/verify.
type Attestor struct {
	*httpBase
	keyID     string
	algorithm string
}

// NewAttestor constructs an attestor.
func NewAttestor(opts AttestorOptions) (*Attestor, error) {
	b, err := newBase(opts.Options)
	if err != nil {
		return nil, err
	}
	keyID := opts.KeyID
	if keyID == "" {
		keyID = "broker-0"
	}
	algo := opts.Algorithm
	if algo == "" {
		algo = "ed25519"
	}
	return &Attestor{httpBase: b, keyID: keyID, algorithm: algo}, nil
}

// SignParams configures a Sign call.
type SignParams struct {
	Topic       string
	Partition   int32
	Offset      int64
	Value       []byte // sent as value_b64; if you need string semantics, use ValueString
	ValueString string // exclusive with Value; sent as value
	SchemaID    int    // default 0
	TimestampMs int64  // default time.Now()
	KeyID       string // overrides client default
}

// VerifyParams configures a Verify call.
type VerifyParams struct {
	Topic        string
	Partition    int32
	Offset       int64
	Value        []byte
	ValueString  string
	SchemaID     int
	TimestampMs  int64
	SignatureB64 string
	KeyID        string
	Algorithm    string
}

// Sign signs an attestation envelope.
func (a *Attestor) Sign(ctx context.Context, p SignParams) (*SignedAttestation, error) {
	if p.Topic == "" {
		return nil, fmt.Errorf("%w: topic must not be empty", ErrInvalidArg)
	}
	if p.Value != nil && p.ValueString != "" {
		return nil, fmt.Errorf("%w: set Value or ValueString, not both", ErrInvalidArg)
	}
	ts := p.TimestampMs
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	keyID := p.KeyID
	if keyID == "" {
		keyID = a.keyID
	}
	body := map[string]any{
		"topic":        p.Topic,
		"partition":    p.Partition,
		"offset":       p.Offset,
		"schema_id":    p.SchemaID,
		"timestamp_ms": ts,
		"key_id":       keyID,
	}
	if p.Value != nil {
		body["value_b64"] = base64.StdEncoding.EncodeToString(p.Value)
	} else {
		body["value"] = p.ValueString
	}
	var out SignedAttestation
	status, raw, err := a.do(ctx, http.MethodPost, "/api/v1/attest", body, &out)
	if err != nil {
		return nil, err
	}
	if status != 200 {
		return nil, &HTTPError{Method: "POST", Path: "/api/v1/attest", Status: status, Body: string(raw)}
	}
	return &out, nil
}

type verifyResp struct {
	Valid bool `json:"valid"`
}

// Verify verifies a previously-issued attestation.
func (a *Attestor) Verify(ctx context.Context, p VerifyParams) (bool, error) {
	if p.Topic == "" || p.SignatureB64 == "" {
		return false, fmt.Errorf("%w: topic and SignatureB64 must not be empty", ErrInvalidArg)
	}
	keyID := p.KeyID
	if keyID == "" {
		keyID = a.keyID
	}
	algo := p.Algorithm
	if algo == "" {
		algo = a.algorithm
	}
	body := map[string]any{
		"topic":         p.Topic,
		"partition":     p.Partition,
		"offset":        p.Offset,
		"schema_id":     p.SchemaID,
		"timestamp_ms":  p.TimestampMs,
		"key_id":        keyID,
		"signature_b64": p.SignatureB64,
		"algorithm":     algo,
	}
	if p.Value != nil {
		body["value_b64"] = base64.StdEncoding.EncodeToString(p.Value)
	} else {
		body["value"] = p.ValueString
	}
	var out verifyResp
	status, raw, err := a.do(ctx, http.MethodPost, "/api/v1/attest/verify", body, &out)
	if err != nil {
		return false, err
	}
	if status != 200 {
		return false, &HTTPError{Method: "POST", Path: "/api/v1/attest/verify", Status: status, Body: string(raw)}
	}
	return out.Valid, nil
}

// ---------------------------------------------------------------------------
// M2 — Semantic search
// ---------------------------------------------------------------------------

// SemanticSearchHit is a single search hit.
type SemanticSearchHit struct {
	Partition int     `json:"partition"`
	Offset    int64   `json:"offset"`
	Score     float64 `json:"score"`
	Value     *string `json:"value,omitempty"`
}

// SemanticSearchResult is the result of a semantic search call.
type SemanticSearchResult struct {
	Hits   []SemanticSearchHit `json:"hits"`
	TookMs int                 `json:"took_ms"`
}

// SemanticSearchClient wraps POST /api/v1/topics/{topic}/search.
type SemanticSearchClient struct{ *httpBase }

// NewSemanticSearchClient constructs a search client.
func NewSemanticSearchClient(opts Options) (*SemanticSearchClient, error) {
	b, err := newBase(opts)
	if err != nil {
		return nil, err
	}
	return &SemanticSearchClient{httpBase: b}, nil
}

// SearchOptions configures Search.
type SearchOptions struct {
	K      int            // top-k; defaults to 10; must be 1..1000
	Filter map[string]any // optional metadata filter
}

// Search runs a semantic search query against a topic.
func (c *SemanticSearchClient) Search(ctx context.Context, topic, query string, opts SearchOptions) (*SemanticSearchResult, error) {
	if topic == "" || query == "" {
		return nil, fmt.Errorf("%w: topic and query must not be empty", ErrInvalidArg)
	}
	k := opts.K
	if k == 0 {
		k = 10
	}
	if k <= 0 || k > 1000 {
		return nil, fmt.Errorf("%w: k must be in [1, 1000]", ErrInvalidArg)
	}
	body := map[string]any{"query": query, "k": k}
	if opts.Filter != nil {
		body["filter"] = opts.Filter
	}
	path := "/api/v1/topics/" + url.PathEscape(topic) + "/search"
	var out SemanticSearchResult
	status, raw, err := c.do(ctx, http.MethodPost, path, body, &out)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &HTTPError{Method: "POST", Path: path, Status: status, Body: string(raw)}
	}
	return &out, nil
}

// ---------------------------------------------------------------------------
// M1 — Agent Memory
// ---------------------------------------------------------------------------

// MemoryKind is the kind of memory write.
type MemoryKind string

const (
	MemoryObservation MemoryKind = "observation"
	MemoryFact        MemoryKind = "fact"
	MemoryProcedure   MemoryKind = "procedure"
)

func (k MemoryKind) valid() bool {
	switch k {
	case MemoryObservation, MemoryFact, MemoryProcedure:
		return true
	}
	return false
}

// WrittenEntry is one tier-write outcome.
type WrittenEntry struct {
	Topic  string `json:"topic"`
	Offset int64  `json:"offset"`
}

// RecalledMemory is one recalled memory.
type RecalledMemory struct {
	Tier    string  `json:"tier"`
	Topic   string  `json:"topic"`
	Offset  int64   `json:"offset"`
	Content string  `json:"content"`
	Score   float64 `json:"score"`
}

// MemoryClient wraps /api/v1/memory/{remember,recall}.
type MemoryClient struct{ *httpBase }

// NewMemoryClient constructs a memory client.
func NewMemoryClient(opts Options) (*MemoryClient, error) {
	b, err := newBase(opts)
	if err != nil {
		return nil, err
	}
	return &MemoryClient{httpBase: b}, nil
}

// RememberParams configures a Remember call.
type RememberParams struct {
	AgentID    string
	Kind       MemoryKind
	Content    string
	Importance float64  // default 0.5; must be in [0, 1]
	Tags       []string // optional
	Skill      string   // required when Kind == MemoryProcedure
}

// RecallParams configures a Recall call.
type RecallParams struct {
	AgentID string
	Query   string
	K       int // default 10
	MinHits int // default 0
}

// Remember writes a memory for an agent. Returns one entry per tier.
func (c *MemoryClient) Remember(ctx context.Context, p RememberParams) ([]WrittenEntry, error) {
	if p.AgentID == "" {
		return nil, fmt.Errorf("%w: AgentID must not be empty", ErrInvalidArg)
	}
	if p.Content == "" {
		return nil, fmt.Errorf("%w: Content must not be empty", ErrInvalidArg)
	}
	if !p.Kind.valid() {
		return nil, fmt.Errorf("%w: invalid Kind %q", ErrInvalidArg, p.Kind)
	}
	importance := p.Importance
	if importance == 0 {
		importance = 0.5
	}
	if importance < 0 || importance > 1 {
		return nil, fmt.Errorf("%w: Importance must be in [0, 1]", ErrInvalidArg)
	}
	if p.Kind == MemoryProcedure && p.Skill == "" {
		return nil, fmt.Errorf("%w: Skill is required for kind=procedure", ErrInvalidArg)
	}
	tags := p.Tags
	if tags == nil {
		tags = []string{}
	}
	body := map[string]any{
		"agent_id":   p.AgentID,
		"kind":       string(p.Kind),
		"content":    p.Content,
		"importance": importance,
		"tags":       tags,
	}
	if p.Kind == MemoryProcedure {
		body["skill"] = p.Skill
	}
	var out struct {
		Written []WrittenEntry `json:"written"`
	}
	status, raw, err := c.do(ctx, http.MethodPost, "/api/v1/memory/remember", body, &out)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &HTTPError{Method: "POST", Path: "/api/v1/memory/remember", Status: status, Body: string(raw)}
	}
	return out.Written, nil
}

// Recall returns top-k memories for an agent matching query.
func (c *MemoryClient) Recall(ctx context.Context, p RecallParams) ([]RecalledMemory, error) {
	if p.AgentID == "" || p.Query == "" {
		return nil, fmt.Errorf("%w: AgentID and Query must not be empty", ErrInvalidArg)
	}
	k := p.K
	if k == 0 {
		k = 10
	}
	if k <= 0 || k > 1000 {
		return nil, fmt.Errorf("%w: K must be in [1, 1000]", ErrInvalidArg)
	}
	body := map[string]any{
		"agent_id": p.AgentID,
		"query":    p.Query,
		"k":        k,
		"min_hits": p.MinHits,
	}
	var out struct {
		Hits []RecalledMemory `json:"hits"`
	}
	status, raw, err := c.do(ctx, http.MethodPost, "/api/v1/memory/recall", body, &out)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &HTTPError{Method: "POST", Path: "/api/v1/memory/recall", Status: status, Body: string(raw)}
	}
	return out.Hits, nil
}
