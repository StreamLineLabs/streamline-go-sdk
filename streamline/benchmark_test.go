package streamline

import (
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// BenchmarkNewClient benchmarks client construction (validation path only,
// since connecting to a real broker is not possible in unit benchmarks).
func BenchmarkNewClient(b *testing.B) {
	b.Run("EmptyBrokers", func(b *testing.B) {
		cfg := Config{Brokers: []string{}}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = NewClient(cfg)
		}
	})

	b.Run("UnsupportedSASL", func(b *testing.B) {
		cfg := DefaultConfig()
		cfg.SASL = &SASLConfig{
			Mechanism: "UNSUPPORTED",
			Username:  "user",
			Password:  "pass",
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = NewClient(cfg)
		}
	})

	b.Run("DefaultConfig", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = DefaultConfig()
		}
	})

	b.Run("DefaultConfig_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = DefaultConfig()
			}
		})
	})

	b.Run("ConfigWithSASLAndTLS", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cfg := DefaultConfig()
			cfg.SASL = &SASLConfig{
				Mechanism: "SCRAM-SHA-256",
				Username:  "admin",
				Password:  "secret",
			}
			cfg.TLS = &TLSConfig{
				Enable:   true,
				CertFile: "/path/cert.pem",
				KeyFile:  "/path/key.pem",
				CAFile:   "/path/ca.pem",
			}
		}
	})

	brokerCounts := []int{1, 3, 5, 10}
	for _, n := range brokerCounts {
		b.Run(fmt.Sprintf("ConfigWith%dBrokers", n), func(b *testing.B) {
			brokers := make([]string, n)
			for i := range brokers {
				brokers[i] = fmt.Sprintf("broker%d:9092", i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = Config{
					Brokers:  brokers,
					ClientID: "bench-client",
					Version:  sarama.V2_8_0_0,
				}
			}
		})
	}
}

// BenchmarkProducerConfig benchmarks ProducerConfig construction with various options.
func BenchmarkProducerConfig(b *testing.B) {
	b.Run("ZeroValue", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ProducerConfig{}
		}
	})

	b.Run("FullConfig", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ProducerConfig{
				MaxMessageBytes: 2097152,
				RequiredAcks:    -1,
				Compression:     4,
				BatchSize:       32768,
				BatchTimeout:    50 * time.Millisecond,
				Idempotent:      true,
				Retries:         5,
			}
		}
	})

	b.Run("FromDefault", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cfg := DefaultConfig()
			cfg.Producer.MaxMessageBytes = 2097152
			cfg.Producer.Compression = 4
			cfg.Producer.Idempotent = true
		}
	})

	b.Run("FullConfig_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = ProducerConfig{
					MaxMessageBytes: 2097152,
					RequiredAcks:    -1,
					Compression:     4,
					BatchSize:       32768,
					BatchTimeout:    50 * time.Millisecond,
					Idempotent:      true,
					Retries:         5,
				}
			}
		})
	})
}

// BenchmarkConsumerConfig benchmarks ConsumerConfig construction with options.
func BenchmarkConsumerConfig(b *testing.B) {
	b.Run("ZeroValue", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ConsumerConfig{}
		}
	})

	b.Run("FullConfig", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ConsumerConfig{
				GroupID:           "benchmark-group",
				AutoOffsetReset:   "earliest",
				SessionTimeout:    45 * time.Second,
				HeartbeatInterval: 5 * time.Second,
				MaxPollRecords:    1000,
				IsolationLevel:    1,
			}
		}
	})

	b.Run("FromDefault", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cfg := DefaultConfig()
			cfg.Consumer.GroupID = "bench-group"
			cfg.Consumer.AutoOffsetReset = "earliest"
			cfg.Consumer.MaxPollRecords = 1000
		}
	})

	b.Run("FullConfig_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = ConsumerConfig{
					GroupID:           "benchmark-group",
					AutoOffsetReset:   "earliest",
					SessionTimeout:    45 * time.Second,
					HeartbeatInterval: 5 * time.Second,
					MaxPollRecords:    1000,
					IsolationLevel:    1,
				}
			}
		})
	})
}

// BenchmarkAdminConfig benchmarks TopicConfig and related admin struct construction.
func BenchmarkAdminConfig(b *testing.B) {
	b.Run("TopicConfig_Simple", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = TopicConfig{
				Name:              "bench-topic",
				NumPartitions:     12,
				ReplicationFactor: 3,
			}
		}
	})

	b.Run("TopicConfig_WithConfig", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = TopicConfig{
				Name:              "bench-topic",
				NumPartitions:     12,
				ReplicationFactor: 3,
				Config: map[string]string{
					"retention.ms":          "86400000",
					"cleanup.policy":        "compact",
					"min.insync.replicas":   "2",
					"max.message.bytes":     "1048576",
					"segment.bytes":         "1073741824",
					"compression.type":      "zstd",
				},
			}
		}
	})

	b.Run("TopicInfo", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = TopicInfo{
				Name:              "bench-topic",
				Partitions:        12,
				ReplicationFactor: 3,
				Config:            map[string]string{"retention.ms": "86400000"},
				Internal:          false,
			}
		}
	})

	b.Run("PartitionInfo", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = PartitionInfo{
				ID:       0,
				Leader:   1,
				Replicas: []int32{1, 2, 3},
				ISR:      []int32{1, 2, 3},
			}
		}
	})

	b.Run("BrokerInfo", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = BrokerInfo{
				ID:   1,
				Host: "broker1.example.com",
				Port: 9092,
				Rack: "us-east-1a",
			}
		}
	})

	b.Run("ConsumerGroupInfo", func(b *testing.B) {
		members := []GroupMember{
			{ID: "m1", ClientID: "client-1", Host: "/10.0.0.1"},
			{ID: "m2", ClientID: "client-2", Host: "/10.0.0.2"},
			{ID: "m3", ClientID: "client-3", Host: "/10.0.0.3"},
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ConsumerGroupInfo{
				GroupID:  "bench-group",
				State:    "Stable",
				Protocol: "range",
				Members:  members,
			}
		}
	})
}

// BenchmarkErrorCreation benchmarks StreamlineError creation and Error() string formatting.
func BenchmarkErrorCreation(b *testing.B) {
	b.Run("NewError", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = NewError(ErrConnection, "connection refused")
		}
	})

	b.Run("NewConnectionError", func(b *testing.B) {
		cause := fmt.Errorf("dial tcp: connection refused")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = NewConnectionError("failed to connect", cause)
		}
	})

	b.Run("NewAuthenticationError", func(b *testing.B) {
		cause := fmt.Errorf("bad credentials")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = NewAuthenticationError("auth failed", cause)
		}
	})

	b.Run("NewTopicNotFoundError", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = NewTopicNotFoundError("events")
		}
	})

	b.Run("NewTimeoutError", func(b *testing.B) {
		cause := fmt.Errorf("context deadline exceeded")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = NewTimeoutError("produce", cause)
		}
	})

	b.Run("NewConfigurationError", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = NewConfigurationError("invalid broker list")
		}
	})

	b.Run("ErrorString_Simple", func(b *testing.B) {
		err := NewError(ErrConnection, "connection refused")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = err.Error()
		}
	})

	b.Run("ErrorString_WithHint", func(b *testing.B) {
		err := NewConnectionError("failed to connect", nil)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = err.Error()
		}
	})

	b.Run("ErrorCodeString", func(b *testing.B) {
		codes := []ErrorCode{
			ErrConnection, ErrAuthentication, ErrAuthorization,
			ErrTopicNotFound, ErrTimeout, ErrProducer,
			ErrConsumer, ErrSerialization, ErrConfiguration, ErrInternal,
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = codes[i%len(codes)].String()
		}
	})

	b.Run("IsStreamlineError", func(b *testing.B) {
		err := fmt.Errorf("wrapper: %w", NewError(ErrInternal, "oops"))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = IsStreamlineError(err)
		}
	})

	b.Run("GetErrorCode", func(b *testing.B) {
		err := fmt.Errorf("wrapper: %w", NewTimeoutError("fetch", nil))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = GetErrorCode(err)
		}
	})

	b.Run("IsRetryable", func(b *testing.B) {
		err := fmt.Errorf("wrapper: %w", NewConnectionError("conn refused", nil))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = IsRetryable(err)
		}
	})

	b.Run("NewError_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = NewError(ErrConnection, "connection refused")
			}
		})
	})
}

// BenchmarkMessageSerialization benchmarks Message struct creation and field access
// patterns used during produce operations.
func BenchmarkMessageSerialization(b *testing.B) {
	b.Run("SimpleMessage", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = &Message{
				Topic: "bench-topic",
				Key:   []byte("key"),
				Value: []byte("value"),
			}
		}
	})

	b.Run("MessageWithHeaders", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = &Message{
				Topic:     "bench-topic",
				Key:       []byte("key"),
				Value:     []byte("Hello, Streamline!"),
				Partition: -1,
				Timestamp: time.Now(),
				Headers: map[string][]byte{
					"content-type": []byte("application/json"),
					"trace-id":     []byte("abc-123-def-456"),
					"source":       []byte("benchmark"),
				},
			}
		}
	})

	b.Run("LargeValue", func(b *testing.B) {
		value := make([]byte, 1024)
		for i := range value {
			value[i] = byte(i % 256)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = &Message{
				Topic: "bench-topic",
				Key:   []byte("key"),
				Value: value,
			}
		}
	})

	b.Run("ConsumerMessage", func(b *testing.B) {
		headers := map[string][]byte{
			"content-type": []byte("application/json"),
			"trace-id":     []byte("abc-123"),
		}
		ts := time.Now()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = &ConsumerMessage{
				Topic:     "bench-topic",
				Partition: 3,
				Offset:    int64(i),
				Key:       []byte("key"),
				Value:     []byte(`{"event":"click","user":"bench"}`),
				Headers:   headers,
				Timestamp: ts,
			}
		}
	})

	b.Run("ProducerResult", func(b *testing.B) {
		ts := time.Now()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = &ProducerResult{
				Topic:     "bench-topic",
				Partition: 3,
				Offset:    int64(i),
				Timestamp: ts,
			}
		}
	})

	b.Run("AsyncProducerResult", func(b *testing.B) {
		ts := time.Now()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = &AsyncProducerResult{
				Topic:     "bench-topic",
				Partition: 3,
				Offset:    int64(i),
				Timestamp: ts,
			}
		}
	})

	// Benchmark the header map construction pattern used in ConsumeClaim
	b.Run("HeaderMapConstruction", func(b *testing.B) {
		rawHeaders := []struct {
			Key   string
			Value []byte
		}{
			{"content-type", []byte("application/json")},
			{"trace-id", []byte("abc-123-def-456")},
			{"source", []byte("benchmark")},
			{"correlation-id", []byte("corr-789")},
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			headers := make(map[string][]byte, len(rawHeaders))
			for _, h := range rawHeaders {
				headers[h.Key] = h.Value
			}
		}
	})

	b.Run("MessageWithHeaders_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = &Message{
					Topic: "bench-topic",
					Key:   []byte("key"),
					Value: []byte("value"),
					Headers: map[string][]byte{
						"content-type": []byte("application/json"),
					},
				}
			}
		})
	})
}

// BenchmarkScramClient benchmarks SCRAM authentication client credential handling.
func BenchmarkScramClient(b *testing.B) {
	b.Run("SHA256_Begin", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			client := &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			_ = client.Begin("user", "password", "")
		}
	})

	b.Run("SHA512_Begin", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			client := &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			_ = client.Begin("user", "password", "")
		}
	})

	b.Run("SHA256_BeginAndStep", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			client := &XDGSCRAMClient{HashGeneratorFcn: SHA256}
			_ = client.Begin("user", "password", "")
			_, _ = client.Step("")
		}
	})

	b.Run("SHA512_BeginAndStep", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			client := &XDGSCRAMClient{HashGeneratorFcn: SHA512}
			_ = client.Begin("user", "password", "")
			_, _ = client.Step("")
		}
	})

	b.Run("ClientConstruction", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = &XDGSCRAMClient{HashGeneratorFcn: SHA256}
		}
	})

	b.Run("SHA256_Begin_Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				client := &XDGSCRAMClient{HashGeneratorFcn: SHA256}
				_ = client.Begin("user", "password", "")
			}
		})
	})
}
// TODO: add TestMain setup for integration tests
