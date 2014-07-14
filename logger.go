package pgx

// Logger is the interface used to get logging from pgx internals.
// https://github.com/inconshreveable/log15 is the recommended logging package.
// This logging interface was extracted from there. However, it should be simple
// to adapt any logger to this interface.
type Logger interface {
	// New returns a new Logger that has this logger's context plus the given context
	New(ctx ...interface{}) Logger

	// Log a message at the given level with context key/value pairs
	Debug(msg string, ctx ...interface{})
	Info(msg string, ctx ...interface{})
	Warn(msg string, ctx ...interface{})
	Error(msg string, ctx ...interface{})
	Crit(msg string, ctx ...interface{})
}

type DiscardLogger struct{}

func (l *DiscardLogger) New(ctx ...interface{}) Logger        { return l }
func (l *DiscardLogger) Debug(msg string, ctx ...interface{}) {}
func (l *DiscardLogger) Info(msg string, ctx ...interface{})  {}
func (l *DiscardLogger) Warn(msg string, ctx ...interface{})  {}
func (l *DiscardLogger) Error(msg string, ctx ...interface{}) {}
func (l *DiscardLogger) Crit(msg string, ctx ...interface{})  {}
