package pgx

import (
	"strconv"
)

type Logger interface {
	Error(msg string)
	Warning(msg string)
	Info(msg string)
	Debug(msg string)
}

type nullLogger string

func (l nullLogger) Error(msg string)   {}
func (l nullLogger) Warning(msg string) {}
func (l nullLogger) Info(msg string)    {}
func (l nullLogger) Debug(msg string)   {}

type pidLogger struct {
	prefix     string
	baseLogger Logger
}

func newPidLogger(pid int32, baseLogger Logger) *pidLogger {
	prefix := "(" + strconv.FormatInt(int64(pid), 10) + ") "
	return &pidLogger{prefix: prefix, baseLogger: baseLogger}
}

func (l *pidLogger) Error(msg string)   { l.baseLogger.Error(l.prefix + msg) }
func (l *pidLogger) Warning(msg string) { l.baseLogger.Warning(l.prefix + msg) }
func (l *pidLogger) Info(msg string)    { l.baseLogger.Info(l.prefix + msg) }
func (l *pidLogger) Debug(msg string)   { l.baseLogger.Debug(l.prefix + msg) }
