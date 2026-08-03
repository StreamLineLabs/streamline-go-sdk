package streamline

import (
	"testing"

	"github.com/IBM/sarama"
)

type recordingPartitionOffsetManager struct {
	current     int64
	marked      []int64
	reset       []int64
	errors      chan *sarama.ConsumerError
	closeCalled bool
}

func (m *recordingPartitionOffsetManager) NextOffset() (int64, string) {
	return m.current, ""
}

func (m *recordingPartitionOffsetManager) MarkOffset(offset int64, _ string) {
	m.marked = append(m.marked, offset)
}

func (m *recordingPartitionOffsetManager) ResetOffset(offset int64, _ string) {
	m.reset = append(m.reset, offset)
}

func (m *recordingPartitionOffsetManager) Errors() <-chan *sarama.ConsumerError {
	return m.errors
}

func (m *recordingPartitionOffsetManager) AsyncClose() {}

func (m *recordingPartitionOffsetManager) Close() error {
	m.closeCalled = true
	return nil
}

func TestApplyPartitionOffsetDirection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		current    int64
		target     int64
		wantMarked []int64
		wantReset  []int64
	}{
		{
			name:      "rewind uses reset",
			current:   100,
			target:    10,
			wantReset: []int64{10},
		},
		{
			name:      "same offset uses reset",
			current:   100,
			target:    100,
			wantReset: []int64{100},
		},
		{
			name:       "advance uses mark",
			current:    100,
			target:     101,
			wantMarked: []int64{101},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			manager := &recordingPartitionOffsetManager{
				current: tt.current,
				errors:  make(chan *sarama.ConsumerError),
			}

			applyPartitionOffset(manager, tt.target)

			if !equalOffsets(manager.marked, tt.wantMarked) {
				t.Fatalf("marked = %v, want %v", manager.marked, tt.wantMarked)
			}
			if !equalOffsets(manager.reset, tt.wantReset) {
				t.Fatalf("reset = %v, want %v", manager.reset, tt.wantReset)
			}
		})
	}
}

func equalOffsets(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
