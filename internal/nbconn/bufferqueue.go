package nbconn

import (
	"sync"
)

const minBufferQueueLen = 8

type bufferQueue struct {
	lock  sync.Mutex
	queue [][]byte
	r, w  int
}

func (bq *bufferQueue) pushBack(buf []byte) {
	bq.lock.Lock()
	defer bq.lock.Unlock()

	if bq.w >= len(bq.queue) {
		bq.growQueue()
	}
	bq.queue[bq.w] = buf
	bq.w++
}

func (bq *bufferQueue) pushFront(buf []byte) {
	bq.lock.Lock()
	defer bq.lock.Unlock()

	if bq.w >= len(bq.queue) {
		bq.growQueue()
	}
	copy(bq.queue[bq.r+1:bq.w+1], bq.queue[bq.r:bq.w])
	bq.queue[bq.r] = buf
	bq.w++
}

func (bq *bufferQueue) popFront() []byte {
	bq.lock.Lock()
	defer bq.lock.Unlock()

	if bq.r == bq.w {
		return nil
	}

	buf := bq.queue[bq.r]
	bq.queue[bq.r] = nil // Clear reference so it can be garbage collected.
	bq.r++

	if bq.r == bq.w {
		bq.r = 0
		bq.w = 0
		if len(bq.queue) > minBufferQueueLen {
			bq.queue = make([][]byte, minBufferQueueLen)
		}
	}

	return buf
}

func (bq *bufferQueue) growQueue() {
	desiredLen := (len(bq.queue) + 1) * 3 / 2
	if desiredLen < minBufferQueueLen {
		desiredLen = minBufferQueueLen
	}

	newQueue := make([][]byte, desiredLen)
	copy(newQueue, bq.queue)
	bq.queue = newQueue
}
