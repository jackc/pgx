package ctxwatch

import (
	"context"
	"sync/atomic"
)

// ContextWatcher watches a context and performs an action when the context is canceled. It can watch one context at a
// time.
type ContextWatcher struct {
	onCancel             func()
	onUnwatchAfterCancel func()

	watchInProgress uint32
	watchChan       chan context.Context
	unwatchChan     chan struct{}
}

// NewContextWatcher returns a ContextWatcher. onCancel will be called when a watched context is canceled.
// OnUnwatchAfterCancel will be called when Unwatch is called and the watched context had already been canceled and
// onCancel called.
func NewContextWatcher(onCancel func(), onUnwatchAfterCancel func()) *ContextWatcher {
	cw := &ContextWatcher{
		onCancel:             onCancel,
		onUnwatchAfterCancel: onUnwatchAfterCancel,
	}
	return cw
}

func (cw *ContextWatcher) watch() {
	for ctx := range cw.watchChan {
		select {
		case <-ctx.Done():
			cw.onCancel()
			<-cw.watchChan
			cw.onUnwatchAfterCancel()
			cw.unwatchChan <- struct{}{}
		case <-cw.watchChan:
			cw.unwatchChan <- struct{}{}
		}
	}
}

// Watch starts watching ctx. If ctx is canceled then the onCancel function passed to NewContextWatcher will be called.
func (cw *ContextWatcher) Watch(ctx context.Context) {
	if atomic.SwapUint32(&cw.watchInProgress, 1) != 0 {
		panic("Watch already in progress")
	}
	if ctx.Done() == nil {
		atomic.StoreUint32(&cw.watchInProgress, 0)
		return
	}
	// watch never gets spawned if ctx is always context.Background()
	if cw.watchChan == nil {
		cw.watchChan = make(chan context.Context, 1)
		cw.unwatchChan = make(chan struct{}, 1)
		go cw.watch()
	}
	cw.watchChan <- ctx
}

// Unwatch stops watching the previously watched context. If the onCancel function passed to NewContextWatcher was
// called then onUnwatchAfterCancel will also be called.
func (cw *ContextWatcher) Unwatch() {
	if atomic.SwapUint32(&cw.watchInProgress, 0) != 1 {
		return
	}
	cw.watchChan <- nil
	<-cw.unwatchChan
}

func (cw *ContextWatcher) Stop() {
	cw.Unwatch()
	if cw.watchChan != nil {
		close(cw.watchChan)
	}
}
