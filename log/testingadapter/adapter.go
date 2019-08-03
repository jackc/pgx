// Package testingadapter provides a logger that writes to a test or benchmark
// log.
package testingadapter

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
)

// TestingLogger interface defines the subset of testing.TB methods used by this
// adapter.
type TestingLogger interface {
	Log(args ...interface{})
}

type Logger struct {
	l TestingLogger
}

func NewLogger(l TestingLogger) *Logger {
	return &Logger{l: l}
}

func (l *Logger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	logArgs := make([]interface{}, 0, 2+len(data))
	logArgs = append(logArgs, level, msg)
	for k, v := range data {
		logArgs = append(logArgs, fmt.Sprintf("%s=%v", k, v))
	}
	l.l.Log(logArgs...)
}
