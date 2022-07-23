// Package testingadapter provides a logger that writes to a test or benchmark
// log.
package testingadapter

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/tracelog"
)

// TestingLogger interface defines the subset of testing.TB methods used by this
// adapter.
type TestingLogger interface {
	Log(args ...any)
}

type Logger struct {
	l TestingLogger
}

func NewLogger(l TestingLogger) *Logger {
	return &Logger{l: l}
}

func (l *Logger) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	logArgs := make([]any, 0, 2+len(data))
	logArgs = append(logArgs, level, msg)
	for k, v := range data {
		logArgs = append(logArgs, fmt.Sprintf("%s=%v", k, v))
	}
	l.l.Log(logArgs...)
}
