package ctxwatch_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgconn/internal/ctxwatch"
	"github.com/stretchr/testify/require"
)

func TestContextWatcherContextCancelled(t *testing.T) {
	canceledChan := make(chan struct{})
	cleanupCalled := false
	cw := ctxwatch.NewContextWatcher(func() {
		canceledChan <- struct{}{}
	}, func() {
		cleanupCalled = true
	})

	ctx, cancel := context.WithCancel(context.Background())
	cw.Watch(ctx)
	cancel()

	select {
	case <-canceledChan:
	case <-time.NewTimer(time.Second).C:
		t.Fatal("Timed out waiting for cancel func to be called")
	}

	cw.Unwatch()

	require.True(t, cleanupCalled, "Cleanup func was not called")
}

func TestContextWatcherUnwatchdBeforeContextCancelled(t *testing.T) {
	cw := ctxwatch.NewContextWatcher(func() {
		t.Error("cancel func should not have been called")
	}, func() {
		t.Error("cleanup func should not have been called")
	})

	ctx, cancel := context.WithCancel(context.Background())
	cw.Watch(ctx)
	cw.Unwatch()
	cancel()
}

func TestContextWatcherMultipleWatchPanics(t *testing.T) {
	cw := ctxwatch.NewContextWatcher(func() {}, func() {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cw.Watch(ctx)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	require.Panics(t, func() { cw.Watch(ctx2) }, "Expected panic when Watch called multiple times")
}

func TestContextWatcherUnwatchIsAlwaysSafe(t *testing.T) {
	cw := ctxwatch.NewContextWatcher(func() {}, func() {})
	cw.Unwatch() // unwatch when not / never watching

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cw.Watch(ctx)
	cw.Unwatch()
	cw.Unwatch() // double unwatch
}

func TestContextWatcherStress(t *testing.T) {
	var cancelFuncCalls int64
	var cleanupFuncCalls int64

	cw := ctxwatch.NewContextWatcher(func() {
		atomic.AddInt64(&cancelFuncCalls, 1)
	}, func() {
		atomic.AddInt64(&cleanupFuncCalls, 1)
	})

	cycleCount := 100000

	for i := 0; i < cycleCount; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cw.Watch(ctx)
		if i%2 == 0 {
			cancel()
		}

		// Without time.Sleep, cw.Unwatch will almost always run before the cancel func which means cancel will never happen. This gives us a better mix.
		if i%3 == 0 {
			time.Sleep(time.Nanosecond)
		}

		cw.Unwatch()
		if i%2 == 1 {
			cancel()
		}
	}

	actualCancelFuncCalls := atomic.LoadInt64(&cancelFuncCalls)
	actualCleanupFuncCalls := atomic.LoadInt64(&cleanupFuncCalls)

	if actualCancelFuncCalls == 0 {
		t.Fatal("actualCancelFuncCalls == 0")
	}

	maxCancelFuncCalls := int64(cycleCount) / 2
	if actualCancelFuncCalls > maxCancelFuncCalls {
		t.Errorf("cancel func calls should be no more than %d but was %d", actualCancelFuncCalls, maxCancelFuncCalls)
	}

	if actualCancelFuncCalls != actualCleanupFuncCalls {
		t.Errorf("cancel func calls (%d) should be equal to cleanup func calls (%d) but was not", actualCancelFuncCalls, actualCleanupFuncCalls)
	}
}

func BenchmarkContextWatcherUncancellable(b *testing.B) {
	cw := ctxwatch.NewContextWatcher(func() {}, func() {})

	for i := 0; i < b.N; i++ {
		cw.Watch(context.Background())
		cw.Unwatch()
	}
}

func BenchmarkContextWatcherCancelled(b *testing.B) {
	cw := ctxwatch.NewContextWatcher(func() {}, func() {})

	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cw.Watch(ctx)
		cancel()
		cw.Unwatch()
	}
}

func BenchmarkContextWatcherCancellable(b *testing.B) {
	cw := ctxwatch.NewContextWatcher(func() {}, func() {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < b.N; i++ {
		cw.Watch(ctx)
		cw.Unwatch()
	}
}
