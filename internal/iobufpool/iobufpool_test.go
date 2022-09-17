package iobufpool_test

import (
	"testing"

	"github.com/jackc/pgx/v5/internal/iobufpool"
	"github.com/stretchr/testify/assert"
)

func TestGetCap(t *testing.T) {
	tests := []struct {
		requestedLen int
		expectedCap  int
	}{
		{requestedLen: 0, expectedCap: 256},
		{requestedLen: 128, expectedCap: 256},
		{requestedLen: 255, expectedCap: 256},
		{requestedLen: 256, expectedCap: 256},
		{requestedLen: 257, expectedCap: 512},
		{requestedLen: 511, expectedCap: 512},
		{requestedLen: 512, expectedCap: 512},
		{requestedLen: 513, expectedCap: 1024},
		{requestedLen: 1023, expectedCap: 1024},
		{requestedLen: 1024, expectedCap: 1024},
		{requestedLen: 33554431, expectedCap: 33554432},
		{requestedLen: 33554432, expectedCap: 33554432},

		// Above 32 MiB skip the pool and allocate exactly the requested size.
		{requestedLen: 33554433, expectedCap: 33554433},
	}
	for _, tt := range tests {
		buf := iobufpool.Get(tt.requestedLen)
		assert.Equalf(t, tt.requestedLen, len(buf), "bad len for requestedLen: %d", len(buf), tt.requestedLen)
		assert.Equalf(t, tt.expectedCap, cap(buf), "bad cap for requestedLen: %d", tt.requestedLen)
	}
}

func TestPutHandlesWrongSizedBuffers(t *testing.T) {
	for putBufSize := range []int{0, 1, 128, 250, 256, 257, 1023, 1024, 1025, 1 << 28} {
		putBuf := make([]byte, putBufSize)
		iobufpool.Put(putBuf)

		tests := []struct {
			requestedLen int
			expectedCap  int
		}{
			{requestedLen: 0, expectedCap: 256},
			{requestedLen: 128, expectedCap: 256},
			{requestedLen: 255, expectedCap: 256},
			{requestedLen: 256, expectedCap: 256},
			{requestedLen: 257, expectedCap: 512},
			{requestedLen: 511, expectedCap: 512},
			{requestedLen: 512, expectedCap: 512},
			{requestedLen: 513, expectedCap: 1024},
			{requestedLen: 1023, expectedCap: 1024},
			{requestedLen: 1024, expectedCap: 1024},
			{requestedLen: 33554431, expectedCap: 33554432},
			{requestedLen: 33554432, expectedCap: 33554432},

			// Above 32 MiB skip the pool and allocate exactly the requested size.
			{requestedLen: 33554433, expectedCap: 33554433},
		}
		for _, tt := range tests {
			getBuf := iobufpool.Get(tt.requestedLen)
			assert.Equalf(t, tt.requestedLen, len(getBuf), "len(putBuf): %d, requestedLen: %d", len(putBuf), tt.requestedLen)
			assert.Equalf(t, tt.expectedCap, cap(getBuf), "cap(putBuf): %d, requestedLen: %d", cap(putBuf), tt.requestedLen)
		}
	}
}

func TestPutGetBufferReuse(t *testing.T) {
	// There is no way to guarantee a buffer will be reused. It should be, but a GC between the Put and the Get will cause
	// it not to be. So try many times.
	for i := 0; i < 100000; i++ {
		buf := iobufpool.Get(4)
		buf[0] = 1
		iobufpool.Put(buf)
		buf = iobufpool.Get(4)
		if buf[0] == 1 {
			return
		}
	}

	t.Error("buffer was never reused")
}
