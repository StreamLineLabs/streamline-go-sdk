package streamline

import (
	"testing"
)

func TestTopicConfig(t *testing.T) {
	tests := []struct {
		name              string
		config            TopicConfig
		wantPartitions    int32
		wantReplication   int16
		wantConfigEntries int
	}{
		{
			name: "basic topic",
			config: TopicConfig{
				Name:              "events",
				NumPartitions:     3,
				ReplicationFactor: 1,
			},
			wantPartitions:    3,
			wantReplication:   1,
			wantConfigEntries: 0,
		},
		{
			name: "topic with config",
			config: TopicConfig{
				Name:              "logs",
				NumPartitions:     12,
				ReplicationFactor: 3,
				Config: map[string]string{
					"retention.ms":    "86400000",
					"cleanup.policy":  "delete",
					"compression.type": "lz4",
				},
			},
			wantPartitions:    12,
			wantReplication:   3,
			wantConfigEntries: 3,
		},
		{
			name: "single partition topic",
			config: TopicConfig{
				Name:              "single",
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
			wantPartitions:    1,
			wantReplication:   1,
			wantConfigEntries: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.config.NumPartitions != tt.wantPartitions {
				t.Errorf("NumPartitions = %d, want %d", tt.config.NumPartitions, tt.wantPartitions)
			}
			if tt.config.ReplicationFactor != tt.wantReplication {
				t.Errorf("ReplicationFactor = %d, want %d", tt.config.ReplicationFactor, tt.wantReplication)
			}
			if len(tt.config.Config) != tt.wantConfigEntries {
				t.Errorf("Config entries = %d, want %d", len(tt.config.Config), tt.wantConfigEntries)
			}
		})
	}
}

func TestTopicConfigValues(t *testing.T) {
	cfg := TopicConfig{
		Name:              "my-topic",
		NumPartitions:     6,
		ReplicationFactor: 3,
		Config: map[string]string{
			"retention.ms":   "3600000",
			"cleanup.policy": "compact",
		},
	}

	if cfg.Name != "my-topic" {
		t.Errorf("Name = %q, want 'my-topic'", cfg.Name)
	}
	if v, ok := cfg.Config["retention.ms"]; !ok || v != "3600000" {
		t.Errorf("Config[retention.ms] = %q, want '3600000'", v)
	}
	if v, ok := cfg.Config["cleanup.policy"]; !ok || v != "compact" {
		t.Errorf("Config[cleanup.policy] = %q, want 'compact'", v)
	}
}

func TestTopicInfo(t *testing.T) {
	tests := []struct {
		name     string
		info     TopicInfo
		internal bool
	}{
		{
			name: "user topic",
			info: TopicInfo{
				Name:              "user-events",
				Partitions:        6,
				ReplicationFactor: 3,
				Config:            map[string]string{"retention.ms": "86400000"},
				Internal:          false,
			},
			internal: false,
		},
		{
			name: "internal topic",
			info: TopicInfo{
				Name:              "__consumer_offsets",
				Partitions:        50,
				ReplicationFactor: 3,
				Config:            map[string]string{},
				Internal:          true,
			},
			internal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.info.Internal != tt.internal {
				t.Errorf("Internal = %v, want %v", tt.info.Internal, tt.internal)
			}
			if tt.info.Partitions <= 0 {
				t.Errorf("Partitions should be > 0, got %d", tt.info.Partitions)
			}
			if tt.info.ReplicationFactor <= 0 {
				t.Errorf("ReplicationFactor should be > 0, got %d", tt.info.ReplicationFactor)
			}
		})
	}
}

func TestTopicInfoFields(t *testing.T) {
	info := TopicInfo{
		Name:              "orders",
		Partitions:        12,
		ReplicationFactor: 3,
		Config: map[string]string{
			"retention.ms": "604800000",
		},
		Internal: false,
	}

	if info.Name != "orders" {
		t.Errorf("Name = %q, want 'orders'", info.Name)
	}
	if info.Partitions != 12 {
		t.Errorf("Partitions = %d, want 12", info.Partitions)
	}
	if info.ReplicationFactor != 3 {
		t.Errorf("ReplicationFactor = %d, want 3", info.ReplicationFactor)
	}
}

func TestPartitionInfo(t *testing.T) {
	tests := []struct {
		name         string
		partition    PartitionInfo
		wantReplicas int
		wantISR      int
	}{
		{
			name: "fully replicated",
			partition: PartitionInfo{
				ID:       0,
				Leader:   1,
				Replicas: []int32{1, 2, 3},
				ISR:      []int32{1, 2, 3},
			},
			wantReplicas: 3,
			wantISR:      3,
		},
		{
			name: "under-replicated",
			partition: PartitionInfo{
				ID:       1,
				Leader:   2,
				Replicas: []int32{1, 2, 3},
				ISR:      []int32{2, 3},
			},
			wantReplicas: 3,
			wantISR:      2,
		},
		{
			name: "single replica",
			partition: PartitionInfo{
				ID:       0,
				Leader:   0,
				Replicas: []int32{0},
				ISR:      []int32{0},
			},
			wantReplicas: 1,
			wantISR:      1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.partition.Replicas) != tt.wantReplicas {
				t.Errorf("Replicas count = %d, want %d", len(tt.partition.Replicas), tt.wantReplicas)
			}
			if len(tt.partition.ISR) != tt.wantISR {
				t.Errorf("ISR count = %d, want %d", len(tt.partition.ISR), tt.wantISR)
			}
			if tt.partition.Leader < 0 {
				t.Errorf("Leader should be >= 0, got %d", tt.partition.Leader)
			}
		})
	}
}

func TestPartitionInfoFields(t *testing.T) {
	p := PartitionInfo{
		ID:       5,
		Leader:   2,
		Replicas: []int32{1, 2, 3},
		ISR:      []int32{2, 3},
	}

	if p.ID != 5 {
		t.Errorf("ID = %d, want 5", p.ID)
	}
	if p.Leader != 2 {
		t.Errorf("Leader = %d, want 2", p.Leader)
	}
	// Verify leader is in replicas
	found := false
	for _, r := range p.Replicas {
		if r == p.Leader {
			found = true
			break
		}
	}
	if !found {
		t.Error("Leader should be in Replicas list")
	}
}

func TestBrokerInfo(t *testing.T) {
	tests := []struct {
		name   string
		broker BrokerInfo
	}{
		{
			name: "broker with rack",
			broker: BrokerInfo{
				ID:   1,
				Host: "broker-1.example.com",
				Port: 9092,
				Rack: "us-east-1a",
			},
		},
		{
			name: "broker without rack",
			broker: BrokerInfo{
				ID:   2,
				Host: "broker-2.example.com",
				Port: 9092,
				Rack: "",
			},
		},
		{
			name: "localhost broker",
			broker: BrokerInfo{
				ID:   0,
				Host: "localhost",
				Port: 9092,
				Rack: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.broker.ID < 0 {
				t.Errorf("ID should be >= 0, got %d", tt.broker.ID)
			}
			if tt.broker.Host == "" {
				t.Error("Host should not be empty")
			}
		})
	}
}

func TestBrokerInfoFields(t *testing.T) {
	b := BrokerInfo{
		ID:   3,
		Host: "kafka-3.prod.internal",
		Port: 9093,
		Rack: "us-west-2b",
	}

	if b.ID != 3 {
		t.Errorf("ID = %d, want 3", b.ID)
	}
	if b.Host != "kafka-3.prod.internal" {
		t.Errorf("Host = %q, want 'kafka-3.prod.internal'", b.Host)
	}
	if b.Port != 9093 {
		t.Errorf("Port = %d, want 9093", b.Port)
	}
	if b.Rack != "us-west-2b" {
		t.Errorf("Rack = %q, want 'us-west-2b'", b.Rack)
	}
}

func TestConsumerGroupInfo(t *testing.T) {
	info := &ConsumerGroupInfo{
		GroupID:  "my-consumer-group",
		State:    "Stable",
		Protocol: "range",
		Members: []GroupMember{
			{ID: "member-1", ClientID: "client-1", Host: "/10.0.0.1"},
			{ID: "member-2", ClientID: "client-2", Host: "/10.0.0.2"},
		},
	}

	if info.GroupID != "my-consumer-group" {
		t.Errorf("GroupID = %q, want 'my-consumer-group'", info.GroupID)
	}
	if info.State != "Stable" {
		t.Errorf("State = %q, want 'Stable'", info.State)
	}
	if info.Protocol != "range" {
		t.Errorf("Protocol = %q, want 'range'", info.Protocol)
	}
	if len(info.Members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(info.Members))
	}
}

func TestConsumerGroupInfoStates(t *testing.T) {
	states := []string{"Stable", "PreparingRebalance", "CompletingRebalance", "Empty", "Dead"}

	for _, state := range states {
		info := &ConsumerGroupInfo{
			GroupID: "group-" + state,
			State:   state,
		}
		if info.State != state {
			t.Errorf("State = %q, want %q", info.State, state)
		}
	}
}

func TestGroupMember(t *testing.T) {
	tests := []struct {
		name   string
		member GroupMember
	}{
		{
			name: "standard member",
			member: GroupMember{
				ID:       "consumer-1-abc123",
				ClientID: "my-service",
				Host:     "/192.168.1.10",
			},
		},
		{
			name: "member with empty host",
			member: GroupMember{
				ID:       "consumer-2-def456",
				ClientID: "another-service",
				Host:     "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.member.ID == "" {
				t.Error("ID should not be empty")
			}
			if tt.member.ClientID == "" {
				t.Error("ClientID should not be empty")
			}
		})
	}
}

func TestGroupMemberFields(t *testing.T) {
	m := GroupMember{
		ID:       "member-abc-123",
		ClientID: "order-processor",
		Host:     "/10.0.1.5",
	}

	if m.ID != "member-abc-123" {
		t.Errorf("ID = %q, want 'member-abc-123'", m.ID)
	}
	if m.ClientID != "order-processor" {
		t.Errorf("ClientID = %q, want 'order-processor'", m.ClientID)
	}
	if m.Host != "/10.0.1.5" {
		t.Errorf("Host = %q, want '/10.0.1.5'", m.Host)
	}
}

func TestAdminCloseNilAdmin(t *testing.T) {
	a := &Admin{}
	if err := a.Close(); err != nil {
		t.Errorf("Close with nil admin should return nil, got %v", err)
	}
}

func TestTopicConfigNilConfig(t *testing.T) {
	cfg := TopicConfig{
		Name:              "simple-topic",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	if cfg.Config != nil {
		t.Error("expected nil Config map by default")
	}
}

func TestConsumerGroupInfoNoMembers(t *testing.T) {
	info := &ConsumerGroupInfo{
		GroupID:  "empty-group",
		State:    "Empty",
		Protocol: "",
		Members:  []GroupMember{},
	}

	if len(info.Members) != 0 {
		t.Errorf("expected 0 members, got %d", len(info.Members))
	}
	if info.State != "Empty" {
		t.Errorf("State = %q, want 'Empty'", info.State)
	}
}
