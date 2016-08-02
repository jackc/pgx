package pgx

import (
	"encoding/hex"
	"errors"
	"fmt"
)

// The values for log levels are chosen such that the zero value means that no
// log level was specified and we can default to LogLevelDebug to preserve
// the behavior that existed prior to log level introduction.
const (
	LogLevelTrace = 6
	LogLevelDebug = 5
	LogLevelInfo  = 4
	LogLevelWarn  = 3
	LogLevelError = 2
	LogLevelNone  = 1
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

// LogLevelFromString converts log level string to constant
//
// Valid levels:
//	trace
//	debug
//	info
//	warn
//	error
//	none
func LogLevelFromString(s string) (int, error) {
	switch s {
	case "trace":
		return LogLevelTrace, nil
	case "debug":
		return LogLevelDebug, nil
	case "info":
		return LogLevelInfo, nil
	case "warn":
		return LogLevelWarn, nil
	case "error":
		return LogLevelError, nil
	case "none":
		return LogLevelNone, nil
	default:
		return 0, errors.New("invalid log level")
	}
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
