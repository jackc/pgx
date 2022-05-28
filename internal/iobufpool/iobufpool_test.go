package iobufpool_test

import (
	"testing"

	"github.com/jackc/pgx/v5/internal/iobufpool"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	tests := []struct {
		requestedLen int
		expectedLen  int
	}{
		{requestedLen: 0, expectedLen: 256},
		{requestedLen: 128, expectedLen: 256},
		{requestedLen: 255, expectedLen: 256},
		{requestedLen: 256, expectedLen: 256},
		{requestedLen: 257, expectedLen: 512},
		{requestedLen: 511, expectedLen: 512},
		{requestedLen: 512, expectedLen: 512},
		{requestedLen: 513, expectedLen: 1024},
		{requestedLen: 1023, expectedLen: 1024},
		{requestedLen: 1024, expectedLen: 1024},
		{requestedLen: 33554431, expectedLen: 33554432},
		{requestedLen: 33554432, expectedLen: 33554432},

		// Above 32 MiB skip the pool and allocate exactly the requested size.
		{requestedLen: 33554433, expectedLen: 33554433},
	}
	for _, tt := range tests {
		buf := iobufpool.Get(tt.requestedLen)
		assert.Equalf(t, tt.expectedLen, len(buf), "requestedLen: %d", tt.requestedLen)
	}
}
