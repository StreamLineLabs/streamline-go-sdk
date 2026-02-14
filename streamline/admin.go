package streamline

import (
	"context"
	"fmt"
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
