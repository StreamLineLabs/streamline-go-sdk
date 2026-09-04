package streamline

import (
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// TestWithDefaultsZeroConfig is a regression test for a zero-valued Config
// producing a Sarama configuration that fails validation (for example
// "Net.DialTimeout must be > 0").
func TestWithDefaultsZeroConfig(t *testing.T) {
	got := Config{}.withDefaults()
	want := DefaultConfig()

	// RequiredAcks=0 is a valid explicit Kafka mode and cannot be inferred as
	// unset from a value field, so only DefaultConfig supplies -1.
	want.Producer.RequiredAcks = 0
	want.Producer.BatchSize = 0
	want.Producer.BatchTimeout = 0
	want.Producer.Retries = 0

	if got.ClientID != want.ClientID {
		t.Errorf("ClientID = %q, want %q", got.ClientID, want.ClientID)
	}

	if got.Version != want.Version {
		t.Errorf("Version = %v, want %v", got.Version, want.Version)
	}
	if got.ConnectionTimeout != want.ConnectionTimeout {
		t.Errorf("ConnectionTimeout = %v, want %v", got.ConnectionTimeout, want.ConnectionTimeout)
	}
	if got.MetadataRefreshInterval != want.MetadataRefreshInterval {
		t.Errorf("MetadataRefreshInterval = %v, want %v", got.MetadataRefreshInterval, want.MetadataRefreshInterval)
	}
	if got.HTTPEndpoint != want.HTTPEndpoint {
		t.Errorf("HTTPEndpoint = %q, want %q", got.HTTPEndpoint, want.HTTPEndpoint)
	}
	if got.Producer != want.Producer {
		t.Errorf("Producer = %+v, want %+v", got.Producer, want.Producer)
	}
	if got.Consumer != want.Consumer {
		t.Errorf("Consumer = %+v, want %+v", got.Consumer, want.Consumer)
	}
	if got.Brokers != nil {
		t.Errorf("Brokers = %v, want nil (brokers are never defaulted)", got.Brokers)
	}
}

func TestRequiredAcksZeroRemainsNoResponse(t *testing.T) {
	cfg := Config{
		Brokers:  []string{"localhost:9092"},
		Producer: ProducerConfig{RequiredAcks: 0},
	}.withDefaults()

	saramaConfig, err := buildSaramaConfig(cfg)
	if err != nil {
		t.Fatalf("buildSaramaConfig: %v", err)
	}

	if saramaConfig.Producer.RequiredAcks != sarama.NoResponse {
		t.Fatalf("RequiredAcks = %d, want NoResponse", saramaConfig.Producer.RequiredAcks)
	}
}

func TestMeaningfulProducerZerosArePreserved(t *testing.T) {
	cfg := Config{
		Brokers: []string{"localhost:9092"},
		Producer: ProducerConfig{
			BatchSize:    0,
			BatchTimeout: 0,
			Retries:      0,
		},
	}.withDefaults()

	if cfg.Producer.BatchSize != 0 {
		t.Fatalf("BatchSize = %d, want 0", cfg.Producer.BatchSize)
	}
	if cfg.Producer.BatchTimeout != 0 {
		t.Fatalf("BatchTimeout = %v, want 0", cfg.Producer.BatchTimeout)
	}
	if cfg.Producer.Retries != 0 {
		t.Fatalf("Retries = %d, want 0", cfg.Producer.Retries)
	}
}

func TestTLSConfigIsApplied(t *testing.T) {
	cfg := Config{
		Brokers: []string{"localhost:9093"},
		TLS: &TLSConfig{
			Enable:             true,
			InsecureSkipVerify: true,
		},
	}.withDefaults()

	saramaConfig, err := buildSaramaConfig(cfg)
	if err != nil {
		t.Fatalf("buildSaramaConfig: %v", err)
	}
	if !saramaConfig.Net.TLS.Enable || saramaConfig.Net.TLS.Config == nil {
		t.Fatal("expected TLS to be enabled")
	}
	if !saramaConfig.Net.TLS.Config.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify to be applied")
	}
}

func TestWithDefaultsKeepsExplicitValues(t *testing.T) {
	cfg := Config{
		Brokers:                 []string{"broker-1:9092"},
		ClientID:                "custom-client",
		Version:                 sarama.V3_6_0_0,
		ConnectionTimeout:       2 * time.Second,
		MetadataRefreshInterval: time.Minute,
		HTTPEndpoint:            "http://streamline.internal:9094",
		Producer: ProducerConfig{
			MaxMessageBytes: 2048,
			RequiredAcks:    1,
			Compression:     2,
			BatchSize:       64,
			BatchTimeout:    time.Second,
			Idempotent:      true,
			Retries:         7,
		},
		Consumer: ConsumerConfig{
			GroupID:           "custom-group",
			AutoOffsetReset:   "earliest",
			SessionTimeout:    20 * time.Second,
			HeartbeatInterval: 4 * time.Second,
			MaxPollRecords:    42,
			IsolationLevel:    1,
		},
	}

	got := cfg.withDefaults()

	if got.Brokers[0] != "broker-1:9092" {
		t.Errorf("Brokers = %v, want [broker-1:9092]", got.Brokers)
	}
	if got.ClientID != "custom-client" {
		t.Errorf("ClientID = %q, want custom-client", got.ClientID)
	}
	if got.Version != sarama.V3_6_0_0 {
		t.Errorf("Version = %v, want V3_6_0_0", got.Version)
	}
	if got.ConnectionTimeout != 2*time.Second {
		t.Errorf("ConnectionTimeout = %v, want 2s", got.ConnectionTimeout)
	}
	if got.MetadataRefreshInterval != time.Minute {
		t.Errorf("MetadataRefreshInterval = %v, want 1m", got.MetadataRefreshInterval)
	}
	if got.HTTPEndpoint != "http://streamline.internal:9094" {
		t.Errorf("HTTPEndpoint = %q, want http://streamline.internal:9094", got.HTTPEndpoint)
	}
	if got.Producer != cfg.Producer {
		t.Errorf("Producer = %+v, want %+v", got.Producer, cfg.Producer)
	}
	if got.Consumer != cfg.Consumer {
		t.Errorf("Consumer = %+v, want %+v", got.Consumer, cfg.Consumer)
	}
}

func TestWithDefaultsShortSessionTimeoutShrinksHeartbeat(t *testing.T) {
	tests := []struct {
		name           string
		sessionTimeout time.Duration
		wantHeartbeat  time.Duration
	}{
		{"default session timeout", 0, defaultHeartbeatInterval},
		{"long session timeout", time.Minute, defaultHeartbeatInterval},
		{"short session timeout", 3 * time.Second, time.Second},
		{"tiny session timeout", 2 * time.Millisecond, minHeartbeatInterval},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{Consumer: ConsumerConfig{SessionTimeout: tt.sessionTimeout}}.withDefaults()
			if cfg.Consumer.HeartbeatInterval != tt.wantHeartbeat {
				t.Errorf("HeartbeatInterval = %v, want %v", cfg.Consumer.HeartbeatInterval, tt.wantHeartbeat)
			}
			if cfg.Consumer.HeartbeatInterval >= cfg.Consumer.SessionTimeout {
				t.Errorf("HeartbeatInterval %v must stay below SessionTimeout %v",
					cfg.Consumer.HeartbeatInterval, cfg.Consumer.SessionTimeout)
			}
		})
	}
}

// TestBuildSaramaConfigValidates guards the reported failure where a partial
// Config produced an invalid Sarama config (zero DialTimeout) and NewClient
// failed before ever reaching the network.
func TestBuildSaramaConfigValidates(t *testing.T) {
	tests := []struct {
		name   string
		config Config
	}{
		{"zero config", Config{}},
		{"brokers only", Config{Brokers: []string{"localhost:9092"}}},
		{"default config", DefaultConfig()},
		{
			name: "partial producer config",
			config: Config{
				Brokers:  []string{"localhost:9092"},
				Producer: ProducerConfig{Compression: 1},
			},
		},
		{
			name: "partial consumer config",
			config: Config{
				Brokers:  []string{"localhost:9092"},
				Consumer: ConsumerConfig{GroupID: "group", AutoOffsetReset: "earliest"},
			},
		},
		{
			name: "idempotent producer",
			config: Config{
				Brokers:  []string{"localhost:9092"},
				Producer: ProducerConfig{Idempotent: true},
			},
		},
		{
			name: "sasl plain",
			config: Config{
				Brokers: []string{"localhost:9092"},
				SASL:    &SASLConfig{Mechanism: "PLAIN", Username: "u", Password: "p"},
			},
		},
		{
			name: "sasl scram sha 512",
			config: Config{
				Brokers: []string{"localhost:9092"},
				SASL:    &SASLConfig{Mechanism: "SCRAM-SHA-512", Username: "u", Password: "p"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			saramaConfig, err := buildSaramaConfig(tt.config.withDefaults())
			if err != nil {
				t.Fatalf("buildSaramaConfig: %v", err)
			}
			if err := saramaConfig.Validate(); err != nil {
				t.Fatalf("sarama config invalid: %v", err)
			}
			if saramaConfig.Net.DialTimeout <= 0 {
				t.Errorf("Net.DialTimeout = %v, want > 0", saramaConfig.Net.DialTimeout)
			}
		})
	}
}

func TestBuildSaramaConfigMapsFields(t *testing.T) {
	cfg := Config{
		Brokers:                 []string{"localhost:9092"},
		ClientID:                "mapper",
		ConnectionTimeout:       7 * time.Second,
		MetadataRefreshInterval: 90 * time.Second,
		Producer: ProducerConfig{
			MaxMessageBytes: 2048,
			RequiredAcks:    1,
			Compression:     int8(sarama.CompressionGZIP),
			BatchSize:       512,
			BatchTimeout:    25 * time.Millisecond,
			Retries:         5,
		},
		Consumer: ConsumerConfig{
			AutoOffsetReset:   "earliest",
			SessionTimeout:    15 * time.Second,
			HeartbeatInterval: 2 * time.Second,
			IsolationLevel:    int8(sarama.ReadCommitted),
		},
	}

	saramaConfig, err := buildSaramaConfig(cfg.withDefaults())
	if err != nil {
		t.Fatalf("buildSaramaConfig: %v", err)
	}
	if err := saramaConfig.Validate(); err != nil {
		t.Fatalf("sarama config invalid: %v", err)
	}

	checks := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"ClientID", saramaConfig.ClientID, "mapper"},
		{"Net.DialTimeout", saramaConfig.Net.DialTimeout, 7 * time.Second},
		{"Metadata.RefreshFrequency", saramaConfig.Metadata.RefreshFrequency, 90 * time.Second},
		{"Producer.MaxMessageBytes", saramaConfig.Producer.MaxMessageBytes, 2048},
		{"Producer.RequiredAcks", saramaConfig.Producer.RequiredAcks, sarama.WaitForLocal},
		{"Producer.Compression", saramaConfig.Producer.Compression, sarama.CompressionGZIP},
		{"Producer.Flush.Bytes", saramaConfig.Producer.Flush.Bytes, 512},
		{"Producer.Flush.Frequency", saramaConfig.Producer.Flush.Frequency, 25 * time.Millisecond},
		{"Producer.Retry.Max", saramaConfig.Producer.Retry.Max, 5},
		{"Consumer.Offsets.Initial", saramaConfig.Consumer.Offsets.Initial, sarama.OffsetOldest},
		{"Consumer.Group.Session.Timeout", saramaConfig.Consumer.Group.Session.Timeout, 15 * time.Second},
		{"Consumer.Group.Heartbeat.Interval", saramaConfig.Consumer.Group.Heartbeat.Interval, 2 * time.Second},
		{"Consumer.IsolationLevel", saramaConfig.Consumer.IsolationLevel, sarama.ReadCommitted},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestBuildSaramaConfigUnsupportedSASLMechanism(t *testing.T) {
	cfg := Config{
		Brokers: []string{"localhost:9092"},
		SASL:    &SASLConfig{Mechanism: "OAUTHBEARER", Username: "u", Password: "p"},
	}

	if _, err := buildSaramaConfig(cfg.withDefaults()); err == nil {
		t.Fatal("expected an error for an unsupported SASL mechanism")
	}
}

func TestBuildSaramaConfigIdempotentProducer(t *testing.T) {
	cfg := Config{
		Brokers:  []string{"localhost:9092"},
		Producer: ProducerConfig{Idempotent: true},
	}.withDefaults()

	saramaConfig, err := buildSaramaConfig(cfg)
	if err != nil {
		t.Fatalf("buildSaramaConfig: %v", err)
	}
	if err := saramaConfig.Validate(); err != nil {
		t.Fatalf("sarama config invalid: %v", err)
	}
	if saramaConfig.Net.MaxOpenRequests != 1 {
		t.Errorf("Net.MaxOpenRequests = %d, want 1 for an idempotent producer", saramaConfig.Net.MaxOpenRequests)
	}
	if saramaConfig.Producer.Retry.Max < 1 {
		t.Errorf("Producer.Retry.Max = %d, want >= 1 for an idempotent producer", saramaConfig.Producer.Retry.Max)
	}
}

func TestBuildSaramaConfigIdempotentRejectsPartialAcks(t *testing.T) {
	cfg := Config{
		Brokers:  []string{"localhost:9092"},
		Producer: ProducerConfig{Idempotent: true, RequiredAcks: 1},
	}.withDefaults()

	_, err := buildSaramaConfig(cfg)
	if err == nil {
		t.Fatal("expected an error for an idempotent producer with RequiredAcks=1")
	}
	var sErr *StreamlineError
	if !errors.As(err, &sErr) {
		t.Fatalf("expected a *StreamlineError, got %T", err)
	}
	if sErr.Code != ErrConfiguration {
		t.Errorf("error code = %v, want %v", sErr.Code, ErrConfiguration)
	}
}

// TestNewClientRejectsMissingBrokers documents that Brokers stays required even
// though every other field is optional.
func TestNewClientRejectsMissingBrokers(t *testing.T) {
	if _, err := NewClient(Config{}); err == nil {
		t.Fatal("expected an error when no brokers are configured")
	}
}

// TestRequiredAcksMapping locks the SDK's acknowledgement levels to the Sarama
// constants they stand for: 0 = no response, 1 = leader only, -1 = all replicas.
func TestRequiredAcksMapping(t *testing.T) {
	tests := []struct {
		name string
		acks int16
		want sarama.RequiredAcks
	}{
		{"no response", 0, sarama.NoResponse},
		{"leader only", 1, sarama.WaitForLocal},
		{"all replicas", -1, sarama.WaitForAll},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Brokers:  []string{"localhost:9092"},
				Producer: ProducerConfig{RequiredAcks: tt.acks},
			}.withDefaults()

			saramaConfig, err := buildSaramaConfig(cfg)
			if err != nil {
				t.Fatalf("buildSaramaConfig: %v", err)
			}
			if saramaConfig.Producer.RequiredAcks != tt.want {
				t.Errorf("RequiredAcks = %d, want %d", saramaConfig.Producer.RequiredAcks, tt.want)
			}
			if err := saramaConfig.Validate(); err != nil {
				t.Fatalf("sarama config invalid: %v", err)
			}
		})
	}
}

// TestWithDefaultsPromotesIdempotentAcks covers the one case where a zero
// RequiredAcks is treated as unset: an idempotent producer cannot run with
// acks=0, so the default of -1 is filled in instead of failing later.
func TestWithDefaultsPromotesIdempotentAcks(t *testing.T) {
	cfg := Config{
		Brokers:  []string{"localhost:9092"},
		Producer: ProducerConfig{Idempotent: true},
	}.withDefaults()

	if cfg.Producer.RequiredAcks != defaultRequiredAcks {
		t.Fatalf("RequiredAcks = %d, want %d for an idempotent producer", cfg.Producer.RequiredAcks, defaultRequiredAcks)
	}

	nonIdempotent := Config{
		Brokers:  []string{"localhost:9092"},
		Producer: ProducerConfig{Idempotent: false},
	}.withDefaults()

	if nonIdempotent.Producer.RequiredAcks != 0 {
		t.Errorf("RequiredAcks = %d, want 0 to stay an explicit fire-and-forget setting", nonIdempotent.Producer.RequiredAcks)
	}
}
