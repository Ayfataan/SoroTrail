package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIngestLagLedgers(t *testing.T) {
	tests := []struct {
		name         string
		chainHead    int64
		lastIngested int64
		want         int64
	}{
		{
			name:         "chain head ahead of last ingested returns the difference",
			chainHead:    1000,
			lastIngested: 950,
			want:         50,
		},
		{
			name:         "equal values return zero",
			chainHead:    1000,
			lastIngested: 1000,
			want:         0,
		},
		{
			name:         "zero chain head returns zero rather than negative lag",
			chainHead:    0,
			lastIngested: 100,
			want:         0,
		},
		{
			name:         "negative chain head returns zero rather than negative lag",
			chainHead:    -1,
			lastIngested: 100,
			want:         0,
		},
		{
			name:         "zero last ingested returns zero",
			chainHead:    100,
			lastIngested: 0,
			want:         0,
		},
		{
			name:         "negative last ingested returns zero",
			chainHead:    100,
			lastIngested: -1,
			want:         0,
		},
		{
			name:         "last ingested ahead of chain head does not return negative",
			chainHead:    95,
			lastIngested: 100,
			want:         0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ingestLagLedgers(tt.chainHead, tt.lastIngested)
			assert.Equal(t, tt.want, got)
		})
	}
}
