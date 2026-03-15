package ctxwatch

import (
	"context"
	"sync"
)

// ContextWatcher watches a context and performs an action when the context is canceled. It can watch one context at a
// time.
type ContextWatcher struct {
	handler Handler

	// Lock protects the members below.
	lock sync.Mutex
	// Stop is the handle for an "after func". See [context.AfterFunc].
	stop func() bool
	done chan struct{}
}

// NewContextWatcher returns a ContextWatcher. onCancel will be called when a watched context is canceled.
// OnUnwatchAfterCancel will be called when Unwatch is called and the watched context had already been canceled and
// onCancel called.
func NewContextWatcher(handler Handler) *ContextWatcher {
	cw := &ContextWatcher{
		handler: handler,
	}

	return cw
}

// Watch starts watching ctx. If ctx is canceled then the onCancel function passed to NewContextWatcher will be called.
func (cw *ContextWatcher) Watch(ctx context.Context) {
	cw.lock.Lock()
	defer cw.lock.Unlock()

	if cw.stop != nil {
		panic("watch already in progress")
	}

	if ctx.Done() != nil {
		cw.done = make(chan struct{})
		cw.stop = context.AfterFunc(ctx, func() {
			cw.handler.HandleCancel(ctx)
			close(cw.done)
		})
	}
}

// Unwatch stops watching the previously watched context. If the onCancel function passed to NewContextWatcher was
// called then onUnwatchAfterCancel will also be called.
func (cw *ContextWatcher) Unwatch() {
	cw.lock.Lock()
	defer cw.lock.Unlock()

	if cw.stop != nil {
		if !cw.stop() {
			<-cw.done
			cw.handler.HandleUnwatchAfterCancel()
		}
		cw.stop = nil
		cw.done = nil
	}
}

type Handler interface {
	// HandleCancel is called when the context that a ContextWatcher is currently watching is canceled. canceledCtx is the
	// context that was canceled.
	HandleCancel(canceledCtx context.Context)

	// HandleUnwatchAfterCancel is called when a ContextWatcher that called HandleCancel on this Handler is unwatched.
	HandleUnwatchAfterCancel()
}
