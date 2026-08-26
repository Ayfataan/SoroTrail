package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// validAddr returns a 56-character string that starts with the given
// prefix and is otherwise filled with a legal base32 character ("A").
func validAddr(prefix byte) string {
	b := make([]byte, 56)
	b[0] = prefix
	for i := 1; i < 56; i++ {
		b[i] = 'A'
	}
	return string(b)
}

func TestIsValidAddress(t *testing.T) {
	validG := validAddr('G')
	validC := validAddr('C')

	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name:  "valid G address returns true",
			input: validG,
			want:  true,
		},
		{
			name:  "valid C contract address returns true",
			input: validC,
			want:  true,
		},
		{
			name:  "55-character string returns false",
			input: validG[:55],
			want:  false,
		},
		{
			name:  "57-character string returns false",
			input: validG + "A",
			want:  false,
		},
		{
			name:  "correct-length string starting with X returns false",
			input: validAddr('X'),
			want:  false,
		},
		{
			name:  "lowercase input returns false",
			input: "g" + validG[1:],
			want:  false,
		},
		{
			name:  "base32-invalid characters 0, 1, 8, 9 return false",
			input: "G0000000000000000000000000000000000000000000000000000000",
			want:  false,
		},
		{
			name:  "empty string returns false",
			input: "",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidAddress(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

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
