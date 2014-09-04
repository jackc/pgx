package pgx

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
