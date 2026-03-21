package ctxwatch_test

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
)

func TestContextWatchGoroutineBuildup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var cancelFuncCalls int64
		var cleanupFuncCalls int64
		h := &testHandler{
			handleCancel: func(context.Context) {
				atomic.AddInt64(&cancelFuncCalls, 1)
			},
			handleUnwatchAfterCancel: func() {
				atomic.AddInt64(&cleanupFuncCalls, 1)
			},
		}
		ctx, done := context.WithCancel(t.Context())
		defer done()
		floor := runtime.NumGoroutine()

		for range 10 {
			cw := ctxwatch.NewContextWatcher(h)
			cw.Watch(ctx)
			defer cw.Unwatch()
		}
		synctest.Wait()
		done()
		for range 10 {
			cw := ctxwatch.NewContextWatcher(h)
			cw.Watch(ctx)
			cw.Unwatch()
		}

		synctest.Wait()
		outstanding := runtime.NumGoroutine() - floor
		t.Log("outstanding goroutines:", outstanding)
		if outstanding != 0 {
			t.Fail()
		}

		actualCancelFuncCalls := atomic.LoadInt64(&cancelFuncCalls)
		t.Log("cancel:", actualCancelFuncCalls)
		if actualCancelFuncCalls != 20 {
			t.Fail()
		}
		actualCleanupFuncCalls := atomic.LoadInt64(&cleanupFuncCalls)
		t.Log("cleanup:", actualCleanupFuncCalls)
		if actualCleanupFuncCalls != 10 {
			t.Fail()
		}
	})
}
