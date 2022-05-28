// Package iobufpool implements a global segregated-fit pool of buffers for IO.
package iobufpool

import "sync"

const minPoolExpOf2 = 8

var pools [18]*sync.Pool

func init() {
	for i := range pools {
		bufLen := 1 << (minPoolExpOf2 + i)
		pools[i] = &sync.Pool{New: func() any { return make([]byte, bufLen) }}
	}
}

// Get gets a []byte with len >= size and len <= size*2.
func Get(size int) []byte {
	i := poolIdx(size)
	if i >= len(pools) {
		return make([]byte, size)
	}
	return pools[i].Get().([]byte)
}

// Put returns buf to the pool.
func Put(buf []byte) {
	i := poolIdx(len(buf))
	if i >= len(pools) {
		return
	}

	pools[i].Put(buf)
}

func poolIdx(size int) int {
	size--
	size >>= minPoolExpOf2
	i := 0
	for size > 0 {
		size >>= 1
		i++
	}

	return i
}
