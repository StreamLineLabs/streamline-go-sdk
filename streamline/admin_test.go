package streamline

import (
	"context"
	"net/http"
	"net/http/httptest"
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

// ── HTTP Admin Tests ────────────────────────────────────────────────────────

func TestHTTPAdmin_ClusterInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/cluster" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"cluster_id":"test-cluster","broker_id":1,"brokers":[{"id":1,"host":"broker-1","port":9092,"rack":"us-east-1a"},{"id":2,"host":"broker-2","port":9092}],"controller":1}`))
	}))
	defer srv.Close()

	admin := NewHTTPAdmin(srv.URL)
	info, err := admin.ClusterInfo(context.Background())
	if err != nil {
		t.Fatalf("ClusterInfo() error = %v", err)
	}
	if info.ClusterID != "test-cluster" {
		t.Errorf("ClusterID = %q, want 'test-cluster'", info.ClusterID)
	}
	if len(info.Brokers) != 2 {
		t.Errorf("len(Brokers) = %d, want 2", len(info.Brokers))
	}
	if info.Brokers[0].Host != "broker-1" {
		t.Errorf("Brokers[0].Host = %q, want 'broker-1'", info.Brokers[0].Host)
	}
	if info.Brokers[0].Rack != "us-east-1a" {
		t.Errorf("Brokers[0].Rack = %q, want 'us-east-1a'", info.Brokers[0].Rack)
	}
	if info.Controller != 1 {
		t.Errorf("Controller = %d, want 1", info.Controller)
	}
}

func TestHTTPAdmin_ConsumerGroupLag(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/consumer-groups/my-group/lag" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"group_id":"my-group","partitions":[{"topic":"events","partition":0,"current_offset":50,"end_offset":100,"lag":50}],"total_lag":50}`))
	}))
	defer srv.Close()

	admin := NewHTTPAdmin(srv.URL)
	lag, err := admin.ConsumerGroupLag(context.Background(), "my-group")
	if err != nil {
		t.Fatalf("ConsumerGroupLag() error = %v", err)
	}
	if lag.GroupID != "my-group" {
		t.Errorf("GroupID = %q, want 'my-group'", lag.GroupID)
	}
	if len(lag.Partitions) != 1 {
		t.Fatalf("len(Partitions) = %d, want 1", len(lag.Partitions))
	}
	if lag.Partitions[0].Lag != 50 {
		t.Errorf("Partitions[0].Lag = %d, want 50", lag.Partitions[0].Lag)
	}
	if lag.TotalLag != 50 {
		t.Errorf("TotalLag = %d, want 50", lag.TotalLag)
	}
}

func TestHTTPAdmin_InspectMessages(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("partition") != "0" {
			t.Errorf("expected partition=0, got %s", r.URL.Query().Get("partition"))
		}
		if r.URL.Query().Get("limit") != "5" {
			t.Errorf("expected limit=5, got %s", r.URL.Query().Get("limit"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"offset":0,"key":"k1","value":"v1","timestamp":1000,"partition":0,"headers":{"source":"test"}},{"offset":1,"value":"v2","timestamp":1001,"partition":0,"headers":{}}]`))
	}))
	defer srv.Close()

	admin := NewHTTPAdmin(srv.URL)
	msgs, err := admin.InspectMessages(context.Background(), "events", 0, nil, 5)
	if err != nil {
		t.Fatalf("InspectMessages() error = %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("len(messages) = %d, want 2", len(msgs))
	}
	if msgs[0].Value != "v1" {
		t.Errorf("msgs[0].Value = %q, want 'v1'", msgs[0].Value)
	}
	if msgs[0].Headers["source"] != "test" {
		t.Errorf("msgs[0].Headers['source'] = %q, want 'test'", msgs[0].Headers["source"])
	}
}

func TestHTTPAdmin_LatestMessages(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("count") != "3" {
			t.Errorf("expected count=3, got %s", r.URL.Query().Get("count"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"offset":97,"value":"msg1","timestamp":3000,"partition":0,"headers":{}},{"offset":98,"value":"msg2","timestamp":3001,"partition":0,"headers":{}}]`))
	}))
	defer srv.Close()

	admin := NewHTTPAdmin(srv.URL)
	msgs, err := admin.LatestMessages(context.Background(), "events", 3)
	if err != nil {
		t.Fatalf("LatestMessages() error = %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("len(messages) = %d, want 2", len(msgs))
	}
	if msgs[0].Offset != 97 {
		t.Errorf("msgs[0].Offset = %d, want 97", msgs[0].Offset)
	}
}

func TestHTTPAdmin_MetricsHistory(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/metrics/history" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"name":"bytes_in","value":1024.5,"labels":{"topic":"events"},"timestamp":1000}]`))
	}))
	defer srv.Close()

	admin := NewHTTPAdmin(srv.URL)
	metrics, err := admin.MetricsHistory(context.Background())
	if err != nil {
		t.Fatalf("MetricsHistory() error = %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("len(metrics) = %d, want 1", len(metrics))
	}
	if metrics[0].Name != "bytes_in" {
		t.Errorf("metrics[0].Name = %q, want 'bytes_in'", metrics[0].Name)
	}
	if metrics[0].Value != 1024.5 {
		t.Errorf("metrics[0].Value = %f, want 1024.5", metrics[0].Value)
	}
}

func TestHTTPAdmin_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal"}`))
	}))
	defer srv.Close()

	admin := NewHTTPAdmin(srv.URL)
	_, err := admin.ClusterInfo(context.Background())
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}
