// Package log15adapter provides a logger that writes to a github.com/inconshreveable/log15.Logger
// log.
package log15adapter

import (
	"github.com/jackc/pgx"
)

// Log15Logger interface defines the subset of
// github.com/inconshreveable/log15.Logger that this adapter uses.
type Log15Logger interface {
	Debug(msg string, ctx ...interface{})
	Info(msg string, ctx ...interface{})
	Warn(msg string, ctx ...interface{})
	Error(msg string, ctx ...interface{})
	Crit(msg string, ctx ...interface{})
}

type Logger struct {
	l Log15Logger
}

func NewLogger(l Log15Logger) *Logger {
	return &Logger{l: l}
}

func (l *Logger) Log(level pgx.LogLevel, msg string, ctx ...interface{}) {
	switch level {
	case pgx.LogLevelTrace:
		l.l.Debug(msg, append(ctx, "PGX_LOG_LEVEL", level)...)
	case pgx.LogLevelDebug:
		l.l.Debug(msg, ctx...)
	case pgx.LogLevelInfo:
		l.l.Info(msg, ctx...)
	case pgx.LogLevelWarn:
		l.l.Warn(msg, ctx...)
	case pgx.LogLevelError:
		l.l.Error(msg, ctx...)
	default:
		l.l.Error(msg, append(ctx, "INVALID_PGX_LOG_LEVEL", level)...)
	}
}
