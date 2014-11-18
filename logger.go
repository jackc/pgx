package pgx

import (
	"encoding/hex"
	"fmt"
)

// Logger is the interface used to get logging from pgx internals.
// https://github.com/inconshreveable/log15 is the recommended logging package.
// This logging interface was extracted from there. However, it should be simple
// to adapt any logger to this interface.
type Logger interface {
	// Log a message at the given level with context key/value pairs
	Debug(msg string, ctx ...interface{})
	Info(msg string, ctx ...interface{})
	Warn(msg string, ctx ...interface{})
	Error(msg string, ctx ...interface{})
}

type discardLogger struct{}

// default discardLogger instance
var dlogger = &discardLogger{}

func (l *discardLogger) Debug(msg string, ctx ...interface{}) {}
func (l *discardLogger) Info(msg string, ctx ...interface{})  {}
func (l *discardLogger) Warn(msg string, ctx ...interface{})  {}
func (l *discardLogger) Error(msg string, ctx ...interface{}) {}

type connLogger struct {
	logger Logger
	pid    int32
}

func (l *connLogger) Debug(msg string, ctx ...interface{}) {
	ctx = append(ctx, "pid", l.pid)
	l.logger.Debug(msg, ctx...)
}

func (l *connLogger) Info(msg string, ctx ...interface{}) {
	ctx = append(ctx, "pid", l.pid)
	l.logger.Info(msg, ctx...)
}

func (l *connLogger) Warn(msg string, ctx ...interface{}) {
	ctx = append(ctx, "pid", l.pid)
	l.logger.Warn(msg, ctx...)
}

func (l *connLogger) Error(msg string, ctx ...interface{}) {
	ctx = append(ctx, "pid", l.pid)
	l.logger.Error(msg, ctx...)
}

func logQueryArgs(args []interface{}) []interface{} {
	logArgs := make([]interface{}, 0, len(args))

	for _, a := range args {
		switch v := a.(type) {
		case []byte:
			if len(v) < 64 {
				a = hex.EncodeToString(v)
			} else {
				a = fmt.Sprintf("%x (truncated %d bytes)", v[:64], len(v)-64)
			}
		case string:
			if len(v) > 64 {
				a = fmt.Sprintf("%s (truncated %d bytes)", v[:64], len(v)-64)
			}
		}
		logArgs = append(logArgs, a)
	}

	return logArgs
}
