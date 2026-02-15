package streamline

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "localhost:9092" {
		t.Errorf("expected default broker localhost:9092, got %v", cfg.Brokers)
	}
	if cfg.ClientID != "streamline-go-client" {
		t.Errorf("expected default client ID 'streamline-go-client', got %q", cfg.ClientID)
	}
	if cfg.Version != sarama.V2_8_0_0 {
		t.Errorf("expected default version V2_8_0_0, got %v", cfg.Version)
	}
	if cfg.ConnectionTimeout != 10*time.Second {
		t.Errorf("expected 10s connection timeout, got %v", cfg.ConnectionTimeout)
	}
	if cfg.MetadataRefreshInterval != 5*time.Minute {
		t.Errorf("expected 5m metadata refresh, got %v", cfg.MetadataRefreshInterval)
	}
	if cfg.SASL != nil {
		t.Error("expected nil SASL config by default")
	}
	if cfg.TLS != nil {
		t.Error("expected nil TLS config by default")
	}
}

func TestDefaultConfigProducerDefaults(t *testing.T) {
	cfg := DefaultConfig()
	p := cfg.Producer

	tests := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"MaxMessageBytes", p.MaxMessageBytes, 1048576},
		{"RequiredAcks", p.RequiredAcks, int16(-1)},
		{"Compression", p.Compression, int8(0)},
		{"BatchSize", p.BatchSize, 16384},
		{"BatchTimeout", p.BatchTimeout, 10 * time.Millisecond},
		{"Idempotent", p.Idempotent, false},
		{"Retries", p.Retries, 3},
	}

	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("Producer.%s = %v, want %v", tt.name, tt.got, tt.want)
		}
	}
}

func TestDefaultConfigConsumerDefaults(t *testing.T) {
	cfg := DefaultConfig()
	c := cfg.Consumer

	if c.AutoOffsetReset != "latest" {
		t.Errorf("expected AutoOffsetReset 'latest', got %q", c.AutoOffsetReset)
	}
	if c.SessionTimeout != 30*time.Second {
		t.Errorf("expected 30s session timeout, got %v", c.SessionTimeout)
	}
	if c.HeartbeatInterval != 3*time.Second {
		t.Errorf("expected 3s heartbeat interval, got %v", c.HeartbeatInterval)
	}
	if c.MaxPollRecords != 500 {
		t.Errorf("expected 500 max poll records, got %d", c.MaxPollRecords)
	}
	if c.IsolationLevel != 0 {
		t.Errorf("expected isolation level 0, got %d", c.IsolationLevel)
	}
}

func TestConfigWithBootstrapServers(t *testing.T) {
	brokers := []string{"broker1:9092", "broker2:9092", "broker3:9092"}
	cfg := Config{
		Brokers:  brokers,
		ClientID: "my-app",
		Version:  sarama.V3_0_0_0,
	}

	if len(cfg.Brokers) != 3 {
		t.Fatalf("expected 3 brokers, got %d", len(cfg.Brokers))
	}
	for i, b := range brokers {
		if cfg.Brokers[i] != b {
			t.Errorf("broker[%d] = %q, want %q", i, cfg.Brokers[i], b)
		}
	}
	if cfg.ClientID != "my-app" {
		t.Errorf("expected client ID 'my-app', got %q", cfg.ClientID)
	}
	if cfg.Version != sarama.V3_0_0_0 {
		t.Errorf("expected version V3_0_0_0, got %v", cfg.Version)
	}
}

func TestNewClientNoBrokers(t *testing.T) {
	_, err := NewClient(Config{Brokers: []string{}})
	if err == nil {
		t.Fatal("expected error with empty brokers")
	}
	expected := "streamline: at least one broker address is required"
	if err.Error() != expected {
		t.Errorf("error = %q, want %q", err.Error(), expected)
	}
}

func TestNewClientNilBrokers(t *testing.T) {
	_, err := NewClient(Config{})
	if err == nil {
		t.Fatal("expected error with nil brokers")
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name:    "empty brokers",
			config:  Config{Brokers: []string{}},
			wantErr: true,
		},
		{
			name:    "nil brokers",
			config:  Config{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewClient(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewClientUnsupportedSASLMechanism(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SASL = &SASLConfig{
		Mechanism: "UNSUPPORTED",
		Username:  "user",
		Password:  "pass",
	}

	_, err := NewClient(cfg)
	if err == nil {
		t.Fatal("expected error with unsupported SASL mechanism")
	}
	if err.Error() != "streamline: unsupported SASL mechanism: UNSUPPORTED" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSASLConfig(t *testing.T) {
	tests := []struct {
		name      string
		mechanism string
		username  string
		password  string
	}{
		{"plain", "PLAIN", "admin", "secret"},
		{"scram256", "SCRAM-SHA-256", "user256", "pass256"},
		{"scram512", "SCRAM-SHA-512", "user512", "pass512"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sasl := &SASLConfig{
				Mechanism: tt.mechanism,
				Username:  tt.username,
				Password:  tt.password,
			}
			if sasl.Mechanism != tt.mechanism {
				t.Errorf("Mechanism = %q, want %q", sasl.Mechanism, tt.mechanism)
			}
			if sasl.Username != tt.username {
				t.Errorf("Username = %q, want %q", sasl.Username, tt.username)
			}
			if sasl.Password != tt.password {
				t.Errorf("Password = %q, want %q", sasl.Password, tt.password)
			}
		})
	}
}

func TestTLSConfig(t *testing.T) {
	tests := []struct {
		name               string
		tls                TLSConfig
		wantEnable         bool
		wantInsecureSkip   bool
	}{
		{
			name: "full TLS",
			tls: TLSConfig{
				Enable:             true,
				CertFile:           "/path/to/cert.pem",
				KeyFile:            "/path/to/key.pem",
				CAFile:             "/path/to/ca.pem",
				InsecureSkipVerify: false,
			},
			wantEnable:       true,
			wantInsecureSkip: false,
		},
		{
			name: "insecure TLS",
			tls: TLSConfig{
				Enable:             true,
				InsecureSkipVerify: true,
			},
			wantEnable:       true,
			wantInsecureSkip: true,
		},
		{
			name:               "disabled TLS",
			tls:                TLSConfig{Enable: false},
			wantEnable:         false,
			wantInsecureSkip:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.tls.Enable != tt.wantEnable {
				t.Errorf("Enable = %v, want %v", tt.tls.Enable, tt.wantEnable)
			}
			if tt.tls.InsecureSkipVerify != tt.wantInsecureSkip {
				t.Errorf("InsecureSkipVerify = %v, want %v", tt.tls.InsecureSkipVerify, tt.wantInsecureSkip)
			}
		})
	}

	// Verify all fields are accessible
	tls := TLSConfig{
		Enable:             true,
		CertFile:           "/cert.pem",
		KeyFile:            "/key.pem",
		CAFile:             "/ca.pem",
		InsecureSkipVerify: true,
	}
	if tls.CertFile != "/cert.pem" {
		t.Errorf("CertFile = %q, want %q", tls.CertFile, "/cert.pem")
	}
	if tls.KeyFile != "/key.pem" {
		t.Errorf("KeyFile = %q, want %q", tls.KeyFile, "/key.pem")
	}
	if tls.CAFile != "/ca.pem" {
		t.Errorf("CAFile = %q, want %q", tls.CAFile, "/ca.pem")
	}
}

func TestProducerConfig(t *testing.T) {
	pc := ProducerConfig{
		MaxMessageBytes: 2097152,
		RequiredAcks:    1,
		Compression:     1,
		BatchSize:       32768,
		BatchTimeout:    50 * time.Millisecond,
		Idempotent:      true,
		Retries:         5,
	}

	if pc.MaxMessageBytes != 2097152 {
		t.Errorf("MaxMessageBytes = %d, want 2097152", pc.MaxMessageBytes)
	}
	if pc.RequiredAcks != 1 {
		t.Errorf("RequiredAcks = %d, want 1", pc.RequiredAcks)
	}
	if pc.Compression != 1 {
		t.Errorf("Compression = %d, want 1", pc.Compression)
	}
	if pc.BatchSize != 32768 {
		t.Errorf("BatchSize = %d, want 32768", pc.BatchSize)
	}
	if pc.BatchTimeout != 50*time.Millisecond {
		t.Errorf("BatchTimeout = %v, want 50ms", pc.BatchTimeout)
	}
	if !pc.Idempotent {
		t.Error("Idempotent = false, want true")
	}
	if pc.Retries != 5 {
		t.Errorf("Retries = %d, want 5", pc.Retries)
	}
}

func TestConsumerConfig(t *testing.T) {
	cc := ConsumerConfig{
		GroupID:           "my-group",
		AutoOffsetReset:   "earliest",
		SessionTimeout:    45 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		MaxPollRecords:    1000,
		IsolationLevel:    1,
	}

	if cc.GroupID != "my-group" {
		t.Errorf("GroupID = %q, want 'my-group'", cc.GroupID)
	}
	if cc.AutoOffsetReset != "earliest" {
		t.Errorf("AutoOffsetReset = %q, want 'earliest'", cc.AutoOffsetReset)
	}
	if cc.SessionTimeout != 45*time.Second {
		t.Errorf("SessionTimeout = %v, want 45s", cc.SessionTimeout)
	}
	if cc.HeartbeatInterval != 5*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 5s", cc.HeartbeatInterval)
	}
	if cc.MaxPollRecords != 1000 {
		t.Errorf("MaxPollRecords = %d, want 1000", cc.MaxPollRecords)
	}
	if cc.IsolationLevel != 1 {
		t.Errorf("IsolationLevel = %d, want 1", cc.IsolationLevel)
	}
}

func TestClientCloseIdempotent(t *testing.T) {
	// Verify that a Client with closed=true returns nil on Close
	c := &Client{closed: true}
	if err := c.Close(); err != nil {
		t.Errorf("Close on already-closed client should return nil, got %v", err)
	}
}

func TestClientIsHealthyWhenClosed(t *testing.T) {
	c := &Client{closed: true}
	_, err := c.NewConsumer(nil, "group", []string{"topic"})
	if err == nil {
		t.Fatal("expected error when client is closed")
	}
	if err.Error() != "streamline: client is closed" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestClientBrokers(t *testing.T) {
	brokers := []string{"broker1:9092", "broker2:9092"}
	c := &Client{
		config: Config{Brokers: brokers},
	}
	got := c.Brokers()
	if len(got) != 2 {
		t.Fatalf("expected 2 brokers, got %d", len(got))
	}
	for i, b := range brokers {
		if got[i] != b {
			t.Errorf("Brokers()[%d] = %q, want %q", i, got[i], b)
		}
	}
}

func TestConfigWithSASLAndTLS(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SASL = &SASLConfig{
		Mechanism: "SCRAM-SHA-256",
		Username:  "user",
		Password:  "pass",
	}
	cfg.TLS = &TLSConfig{
		Enable:   true,
		CertFile: "/cert.pem",
		KeyFile:  "/key.pem",
		CAFile:   "/ca.pem",
	}

	if cfg.SASL == nil {
		t.Fatal("SASL config should not be nil")
	}
	if cfg.TLS == nil {
		t.Fatal("TLS config should not be nil")
	}
	if cfg.SASL.Mechanism != "SCRAM-SHA-256" {
		t.Errorf("SASL Mechanism = %q, want 'SCRAM-SHA-256'", cfg.SASL.Mechanism)
	}
	if !cfg.TLS.Enable {
		t.Error("TLS Enable = false, want true")
	}
}
