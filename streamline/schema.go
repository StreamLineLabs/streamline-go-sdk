package streamline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SchemaType represents the type of a schema.
type SchemaType string

const (
	SchemaTypeAvro     SchemaType = "AVRO"
	SchemaTypeProtobuf SchemaType = "PROTOBUF"
	SchemaTypeJSON     SchemaType = "JSON"
)

// SchemaInfo describes a registered schema.
type SchemaInfo struct {
	ID      int        `json:"id"`
	Subject string     `json:"subject"`
	Version int        `json:"version"`
	Type    SchemaType `json:"schemaType"`
	Schema  string     `json:"schema"`
}

// SchemaRegistryClient interacts with the Streamline Schema Registry HTTP API.
type SchemaRegistryClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewSchemaRegistryClient creates a new schema registry client.
// The baseURL should point to the Streamline HTTP API (e.g., "http://localhost:9094").
func NewSchemaRegistryClient(baseURL string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// RegisterSchema registers a schema under the given subject and returns the schema ID.
func (c *SchemaRegistryClient) RegisterSchema(subject, schema string, schemaType SchemaType) (int, error) {
	payload := map[string]string{
		"schema":     schema,
		"schemaType": string(schemaType),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", c.baseURL, subject)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("register schema: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("register schema failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return 0, fmt.Errorf("unmarshal response: %w", err)
	}
	return result.ID, nil
}

// GetSchema retrieves a schema by its global ID.
func (c *SchemaRegistryClient) GetSchema(id int) (*SchemaInfo, error) {
	url := fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("schema not found: %d", id)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get schema failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var info SchemaInfo
	if err := json.Unmarshal(respBody, &info); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return &info, nil
}

// GetVersions returns all version numbers registered under a subject.
func (c *SchemaRegistryClient) GetVersions(subject string) ([]int, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", c.baseURL, subject)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("get versions: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get versions failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var versions []int
	if err := json.Unmarshal(respBody, &versions); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return versions, nil
}

// CheckCompatibility checks if a schema is compatible with the latest version under a subject.
func (c *SchemaRegistryClient) CheckCompatibility(subject, schema string, schemaType SchemaType) (bool, error) {
	payload := map[string]string{
		"schema":     schema,
		"schemaType": string(schemaType),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/compatibility/subjects/%s/versions/latest", c.baseURL, subject)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("compatibility check: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return true, nil // No existing schema = compatible
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("compatibility check failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		IsCompatible bool `json:"is_compatible"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return false, fmt.Errorf("unmarshal response: %w", err)
	}
	return result.IsCompatible, nil
}

// GetSubjects returns all registered subjects.
func (c *SchemaRegistryClient) GetSubjects() ([]string, error) {
	url := fmt.Sprintf("%s/subjects", c.baseURL)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("get subjects: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get subjects failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var subjects []string
	if err := json.Unmarshal(respBody, &subjects); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return subjects, nil
}

// DeleteSubject deletes a subject and all its schema versions.
func (c *SchemaRegistryClient) DeleteSubject(subject string) ([]int, error) {
	url := fmt.Sprintf("%s/subjects/%s", c.baseURL, subject)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("delete subject: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("delete subject failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var versions []int
	if err := json.Unmarshal(respBody, &versions); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return versions, nil
}
