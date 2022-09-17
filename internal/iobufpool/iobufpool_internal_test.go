package iobufpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolIdx(t *testing.T) {
	tests := []struct {
		size     int
		expected int
	}{
		{size: 0, expected: 0},
		{size: 1, expected: 0},
		{size: 255, expected: 0},
		{size: 256, expected: 0},
		{size: 257, expected: 1},
		{size: 511, expected: 1},
		{size: 512, expected: 1},
		{size: 513, expected: 2},
		{size: 1023, expected: 2},
		{size: 1024, expected: 2},
		{size: 1025, expected: 3},
		{size: 2047, expected: 3},
		{size: 2048, expected: 3},
		{size: 2049, expected: 4},
		{size: 8388607, expected: 15},
		{size: 8388608, expected: 15},
		{size: 8388609, expected: 16},
	}
	for _, tt := range tests {
		idx := getPoolIdx(tt.size)
		assert.Equalf(t, tt.expected, idx, "size: %d", tt.size)
	}
}
