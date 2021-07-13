package kitlogadapter

import (
	"context"

	"github.com/go-kit/log"
	kitlevel "github.com/go-kit/log/level"
	"github.com/jackc/pgx/v4"
)

type Logger struct {
	l log.Logger
}

func NewLogger(l log.Logger) *Logger {
	return &Logger{l: l}
}

func (l *Logger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	logger := l.l
	for k, v := range data {
		logger = log.With(logger, k, v)
	}

	switch level {
	case pgx.LogLevelTrace:
		logger.Log("PGX_LOG_LEVEL", level, "msg", msg)
	case pgx.LogLevelDebug:
		kitlevel.Debug(logger).Log("msg", msg)
	case pgx.LogLevelInfo:
		kitlevel.Info(logger).Log("msg", msg)
	case pgx.LogLevelWarn:
		kitlevel.Warn(logger).Log("msg", msg)
	case pgx.LogLevelError:
		kitlevel.Error(logger).Log("msg", msg)
	default:
		logger.Log("INVALID_PGX_LOG_LEVEL", level, "error", msg)
	}
}
