package streamline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/IBM/sarama"
)

// TopicConfig holds configuration for creating a topic.
type TopicConfig struct {
	// Name is the topic name.
	Name string

	// NumPartitions is the number of partitions.
	NumPartitions int32

	// ReplicationFactor is the number of replicas.
	ReplicationFactor int16

	// Config is the topic configuration (e.g., retention.ms).
	Config map[string]string
}

// TopicInfo holds information about a topic.
type TopicInfo struct {
	// Name is the topic name.
	Name string

	// Partitions is the number of partitions.
	Partitions int32

	// ReplicationFactor is the replication factor.
	ReplicationFactor int16

	// Config is the topic configuration.
	Config map[string]string

	// Internal indicates if this is an internal topic.
	Internal bool
}

// PartitionInfo holds information about a partition.
type PartitionInfo struct {
	// ID is the partition ID.
	ID int32

	// Leader is the leader broker ID.
	Leader int32

	// Replicas is the list of replica broker IDs.
	Replicas []int32

	// ISR is the list of in-sync replica broker IDs.
	ISR []int32
}

// BrokerInfo holds information about a broker.
type BrokerInfo struct {
	// ID is the broker ID.
	ID int32

	// Host is the broker hostname.
	Host string

	// Port is the broker port.
	Port int32

	// Rack is the broker rack (optional).
	Rack string
}

// Admin provides administrative operations.
type Admin struct {
	client sarama.Client
	admin  sarama.ClusterAdmin
}

func newAdmin(client sarama.Client) (*Admin, error) {
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, err
	}
	return &Admin{
		client: client,
		admin:  admin,
	}, nil
}

// CreateTopic creates a new topic.
func (a *Admin) CreateTopic(ctx context.Context, config TopicConfig) error {
	if err := validateTopicName(config.Name); err != nil {
		return fmt.Errorf("streamline: %w", err)
	}

	detail := &sarama.TopicDetail{
		NumPartitions:     config.NumPartitions,
		ReplicationFactor: config.ReplicationFactor,
	}

	if len(config.Config) > 0 {
		detail.ConfigEntries = make(map[string]*string)
		for k, v := range config.Config {
			val := v
			detail.ConfigEntries[k] = &val
		}
	}

	err := a.admin.CreateTopic(config.Name, detail, false)
	if err != nil {
		return fmt.Errorf("streamline: failed to create topic: %w", err)
	}
	return nil
}

// DeleteTopic deletes a topic.
func (a *Admin) DeleteTopic(ctx context.Context, name string) error {
	if err := validateTopicName(name); err != nil {
		return fmt.Errorf("streamline: %w", err)
	}

	err := a.admin.DeleteTopic(name)
	if err != nil {
		return fmt.Errorf("streamline: failed to delete topic: %w", err)
	}
	return nil
}

// ListTopics returns all topics.
func (a *Admin) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	topics, err := a.admin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("streamline: failed to list topics: %w", err)
	}

	result := make([]TopicInfo, 0, len(topics))
	for name, detail := range topics {
		config := make(map[string]string)
		for k, v := range detail.ConfigEntries {
			if v != nil {
				config[k] = *v
			}
		}

		result = append(result, TopicInfo{
			Name:              name,
			Partitions:        detail.NumPartitions,
			ReplicationFactor: detail.ReplicationFactor,
			Config:            config,
		})
	}

	return result, nil
}

// DescribeTopic returns details about a topic.
func (a *Admin) DescribeTopic(ctx context.Context, name string) (*TopicInfo, []PartitionInfo, error) {
	metadata, err := a.admin.DescribeTopics([]string{name})
	if err != nil {
		return nil, nil, fmt.Errorf("streamline: failed to describe topic: %w", err)
	}

	if len(metadata) == 0 {
		return nil, nil, fmt.Errorf("streamline: topic not found: %s", name)
	}

	topicMeta := metadata[0]
	if topicMeta.Err != sarama.ErrNoError {
		return nil, nil, fmt.Errorf("streamline: error describing topic: %v", topicMeta.Err)
	}

	partitions := make([]PartitionInfo, len(topicMeta.Partitions))
	for i, p := range topicMeta.Partitions {
		partitions[i] = PartitionInfo{
			ID:       p.ID,
			Leader:   p.Leader,
			Replicas: p.Replicas,
			ISR:      p.Isr,
		}
	}

	// Get replication factor from first partition
	var replicationFactor int16
	if len(partitions) > 0 {
		replicationFactor = int16(len(partitions[0].Replicas))
	}

	topicInfo := &TopicInfo{
		Name:              name,
		Partitions:        int32(len(partitions)),
		ReplicationFactor: replicationFactor,
		Internal:          topicMeta.IsInternal,
	}

	return topicInfo, partitions, nil
}

// GetTopicConfig returns the configuration for a topic.
func (a *Admin) GetTopicConfig(ctx context.Context, name string) (map[string]string, error) {
	resource := sarama.ConfigResource{
		Type: sarama.TopicResource,
		Name: name,
	}

	entries, err := a.admin.DescribeConfig(resource)
	if err != nil {
		return nil, fmt.Errorf("streamline: failed to get topic config: %w", err)
	}

	config := make(map[string]string)
	for _, entry := range entries {
		config[entry.Name] = entry.Value
	}

	return config, nil
}

// AlterTopicConfig updates the configuration for a topic.
func (a *Admin) AlterTopicConfig(ctx context.Context, name string, config map[string]string) error {
	entries := make(map[string]*string)
	for k, v := range config {
		val := v
		entries[k] = &val
	}

	err := a.admin.AlterConfig(sarama.TopicResource, name, entries, false)
	if err != nil {
		return fmt.Errorf("streamline: failed to alter topic config: %w", err)
	}
	return nil
}

// AddPartitions increases the partition count for a topic.
func (a *Admin) AddPartitions(ctx context.Context, name string, count int32) error {
	err := a.admin.CreatePartitions(name, count, nil, false)
	if err != nil {
		return fmt.Errorf("streamline: failed to add partitions: %w", err)
	}
	return nil
}

// ListBrokers returns all brokers in the cluster.
func (a *Admin) ListBrokers(ctx context.Context) ([]BrokerInfo, error) {
	brokers := a.client.Brokers()

	result := make([]BrokerInfo, len(brokers))
	for i, b := range brokers {
		result[i] = BrokerInfo{
			ID:   b.ID(),
			Host: b.Addr(),
			Port: 0, // Sarama doesn't expose port separately
		}
	}

	return result, nil
}

// ListConsumerGroups returns all consumer groups.
func (a *Admin) ListConsumerGroups(ctx context.Context) ([]string, error) {
	groups, err := a.admin.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("streamline: failed to list consumer groups: %w", err)
	}

	result := make([]string, 0, len(groups))
	for name := range groups {
		result = append(result, name)
	}

	return result, nil
}

// DescribeConsumerGroup returns details about a consumer group.
func (a *Admin) DescribeConsumerGroup(ctx context.Context, groupID string) (*ConsumerGroupInfo, error) {
	groups, err := a.admin.DescribeConsumerGroups([]string{groupID})
	if err != nil {
		return nil, fmt.Errorf("streamline: failed to describe consumer group: %w", err)
	}

	if len(groups) == 0 {
		return nil, fmt.Errorf("streamline: consumer group not found: %s", groupID)
	}

	g := groups[0]
	members := make([]GroupMember, len(g.Members))
	i := 0
	for memberID, m := range g.Members {
		members[i] = GroupMember{
			ID:       memberID,
			ClientID: m.ClientId,
			Host:     m.ClientHost,
		}
		i++
	}

	return &ConsumerGroupInfo{
		GroupID:  g.GroupId,
		State:    g.State,
		Protocol: g.Protocol,
		Members:  members,
	}, nil
}

// ConsumerGroupInfo holds information about a consumer group.
type ConsumerGroupInfo struct {
	GroupID  string
	State    string
	Protocol string
	Members  []GroupMember
}

// GroupMember holds information about a consumer group member.
type GroupMember struct {
	ID       string
	ClientID string
	Host     string
}

// DeleteConsumerGroup deletes a consumer group.
func (a *Admin) DeleteConsumerGroup(ctx context.Context, groupID string) error {
	err := a.admin.DeleteConsumerGroup(groupID)
	if err != nil {
		return fmt.Errorf("streamline: failed to delete consumer group: %w", err)
	}
	return nil
}

// GetConsumerGroupOffsets returns the offsets for a consumer group.
func (a *Admin) GetConsumerGroupOffsets(ctx context.Context, groupID string, topic string) (map[int32]int64, error) {
	offsetMgr, err := sarama.NewOffsetManagerFromClient(groupID, a.client)
	if err != nil {
		return nil, fmt.Errorf("streamline: failed to create offset manager: %w", err)
	}
	defer offsetMgr.Close()

	partitions, err := a.client.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("streamline: failed to get partitions: %w", err)
	}

	offsets := make(map[int32]int64)
	for _, p := range partitions {
		pom, err := offsetMgr.ManagePartition(topic, p)
		if err != nil {
			continue
		}
		offset, _ := pom.NextOffset()
		offsets[p] = offset
		pom.Close()
	}

	return offsets, nil
}

// ResetConsumerGroupOffsets resets offsets for a consumer group.
func (a *Admin) ResetConsumerGroupOffsets(ctx context.Context, groupID string, topic string, offset int64) error {
	partitions, err := a.client.Partitions(topic)
	if err != nil {
		return fmt.Errorf("streamline: failed to get partitions: %w", err)
	}

	offsetMgr, err := sarama.NewOffsetManagerFromClient(groupID, a.client)
	if err != nil {
		return fmt.Errorf("streamline: failed to create offset manager: %w", err)
	}
	defer offsetMgr.Close()

	for _, p := range partitions {
		pom, err := offsetMgr.ManagePartition(topic, p)
		if err != nil {
			return fmt.Errorf("streamline: failed to manage partition %d: %w", p, err)
		}

		targetOffset := offset
		if offset == -1 { // Latest
			latest, err := a.client.GetOffset(topic, p, sarama.OffsetNewest)
			if err != nil {
				pom.Close()
				return fmt.Errorf("streamline: failed to get latest offset: %w", err)
			}
			targetOffset = latest
		} else if offset == -2 { // Earliest
			earliest, err := a.client.GetOffset(topic, p, sarama.OffsetOldest)
			if err != nil {
				pom.Close()
				return fmt.Errorf("streamline: failed to get earliest offset: %w", err)
			}
			targetOffset = earliest
		}

		pom.MarkOffset(targetOffset, "")
		pom.Close()
	}

	// Wait for commits
	time.Sleep(100 * time.Millisecond)

	return nil
}

// Close closes the admin client.
func (a *Admin) Close() error {
	if a.admin != nil {
		return a.admin.Close()
	}
	return nil
}

// ── HTTP-based Admin Operations ─────────────────────────────────────────────
// These methods communicate with the Streamline HTTP REST API (port 9094)
// for operations not available via the Kafka wire protocol.

// ClusterInfo holds cluster overview information.
type ClusterInfo struct {
	ClusterID  string       `json:"cluster_id"`
	BrokerID   int32        `json:"broker_id"`
	Brokers    []BrokerInfo `json:"brokers"`
	Controller int32        `json:"controller"`
}

// ConsumerLag holds lag information for a single partition.
type ConsumerLag struct {
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	CurrentOffset int64  `json:"current_offset"`
	EndOffset     int64  `json:"end_offset"`
	Lag           int64  `json:"lag"`
}

// ConsumerGroupLag holds aggregated lag for a consumer group.
type ConsumerGroupLag struct {
	GroupID    string        `json:"group_id"`
	Partitions []ConsumerLag `json:"partitions"`
	TotalLag   int64         `json:"total_lag"`
}

// InspectedMessage holds a message returned by the inspection API.
type InspectedMessage struct {
	Offset    int64             `json:"offset"`
	Key       *string           `json:"key,omitempty"`
	Value     string            `json:"value"`
	Timestamp int64             `json:"timestamp"`
	Partition int32             `json:"partition"`
	Headers   map[string]string `json:"headers"`
}

// MetricPoint holds a single metric data point.
type MetricPoint struct {
	Name      string            `json:"name"`
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels"`
	Timestamp int64             `json:"timestamp"`
}

// BranchInfo holds information about a copy-on-write topic branch (M5).
type BranchInfo struct {
	// Name is the branch name.
	Name string `json:"name"`

	// BaseTopic is the base topic this branch forks from.
	BaseTopic string `json:"base_topic"`

	// State is the branch state (active, discarded, merged).
	State string `json:"state"`

	// CreatedAt is the creation timestamp (epoch milliseconds).
	CreatedAt int64 `json:"created_at"`
}

// HTTPAdmin provides administrative operations via the Streamline HTTP REST API.
type HTTPAdmin struct {
	baseURL    string
	httpClient *http.Client
}

// NewHTTPAdmin creates a new HTTP-based admin client.
func NewHTTPAdmin(baseURL string) *HTTPAdmin {
	return &HTTPAdmin{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// ClusterInfo returns cluster overview including broker list.
func (h *HTTPAdmin) ClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	var result ClusterInfo
	if err := h.get(ctx, "/v1/cluster", &result); err != nil {
		return nil, fmt.Errorf("streamline: failed to get cluster info: %w", err)
	}
	return &result, nil
}

// ConsumerGroupLag returns lag details for a consumer group.
func (h *HTTPAdmin) ConsumerGroupLag(ctx context.Context, groupID string) (*ConsumerGroupLag, error) {
	var result ConsumerGroupLag
	if err := h.get(ctx, fmt.Sprintf("/v1/consumer-groups/%s/lag", groupID), &result); err != nil {
		return nil, fmt.Errorf("streamline: failed to get consumer group lag: %w", err)
	}
	return &result, nil
}

// ConsumerGroupTopicLag returns lag details for a specific topic within a group.
func (h *HTTPAdmin) ConsumerGroupTopicLag(ctx context.Context, groupID, topic string) (*ConsumerGroupLag, error) {
	var result ConsumerGroupLag
	if err := h.get(ctx, fmt.Sprintf("/v1/consumer-groups/%s/lag/%s", groupID, topic), &result); err != nil {
		return nil, fmt.Errorf("streamline: failed to get consumer group topic lag: %w", err)
	}
	return &result, nil
}

// InspectMessages browses messages from a topic partition.
func (h *HTTPAdmin) InspectMessages(ctx context.Context, topic string, partition int32, offset *int64, limit int) ([]InspectedMessage, error) {
	path := fmt.Sprintf("/v1/inspect/%s?partition=%d&limit=%d", topic, partition, limit)
	if offset != nil {
		path += fmt.Sprintf("&offset=%d", *offset)
	}
	var result []InspectedMessage
	if err := h.get(ctx, path, &result); err != nil {
		return nil, fmt.Errorf("streamline: failed to inspect messages: %w", err)
	}
	return result, nil
}

// LatestMessages returns the most recent messages from a topic.
func (h *HTTPAdmin) LatestMessages(ctx context.Context, topic string, count int) ([]InspectedMessage, error) {
	var result []InspectedMessage
	if err := h.get(ctx, fmt.Sprintf("/v1/inspect/%s/latest?count=%d", topic, count), &result); err != nil {
		return nil, fmt.Errorf("streamline: failed to get latest messages: %w", err)
	}
	return result, nil
}

// MetricsHistory returns metrics history from the server.
func (h *HTTPAdmin) MetricsHistory(ctx context.Context) ([]MetricPoint, error) {
	var result []MetricPoint
	if err := h.get(ctx, "/v1/metrics/history", &result); err != nil {
		return nil, fmt.Errorf("streamline: failed to get metrics history: %w", err)
	}
	return result, nil
}

// CreateBranch creates a copy-on-write branch of a topic (M5).
func (h *HTTPAdmin) CreateBranch(ctx context.Context, name, baseTopic string, baseOffsets map[int32]int64) (*BranchInfo, error) {
	body := map[string]any{
		"name":       name,
		"base_topic": baseTopic,
	}
	if len(baseOffsets) > 0 {
		body["base_offsets"] = baseOffsets
	}
	var result BranchInfo
	if err := h.post(ctx, "/v1/branches", body, &result); err != nil {
		return nil, fmt.Errorf("streamline: failed to create branch: %w", err)
	}
	return &result, nil
}

// ListBranches lists copy-on-write topic branches (M5).
func (h *HTTPAdmin) ListBranches(ctx context.Context, topic string) ([]BranchInfo, error) {
	path := "/v1/branches"
	if topic != "" {
		path += "?topic=" + url.QueryEscape(topic)
	}
	var result []BranchInfo
	if err := h.get(ctx, path, &result); err != nil {
		return nil, fmt.Errorf("streamline: failed to list branches: %w", err)
	}
	return result, nil
}

// DiscardBranch discards (deletes) a copy-on-write topic branch (M5).
func (h *HTTPAdmin) DiscardBranch(ctx context.Context, branchID string) error {
	path := "/v1/branches/" + url.PathEscape(branchID)
	if err := h.deleteReq(ctx, path); err != nil {
		return fmt.Errorf("streamline: failed to discard branch: %w", err)
	}
	return nil
}

// get performs an HTTP GET and decodes the JSON response into target.
func (h *HTTPAdmin) get(ctx context.Context, path string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(target)
}

// post performs an HTTP POST with a JSON body and decodes the JSON response.
func (h *HTTPAdmin) post(ctx context.Context, path string, body any, target any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	if target != nil {
		return json.NewDecoder(resp.Body).Decode(target)
	}
	return nil
}

// deleteReq performs an HTTP DELETE request.
func (h *HTTPAdmin) deleteReq(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, h.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}
