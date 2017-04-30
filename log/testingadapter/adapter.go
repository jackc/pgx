// Package testingadapter provides a logger that writes to a test or benchmark
// log.
package testingadapter

import (
	"github.com/jackc/pgx"
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

func (l *Logger) Log(level pgx.LogLevel, msg string, ctx ...interface{}) {
	l.l.Log(level, msg, ctx)
}
