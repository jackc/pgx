package kitlogadapter

import (
	"context"

	"github.com/go-kit/kit/log"
	kitlevel "github.com/go-kit/kit/log/level"
	"github.com/jackc/pgx/v4"
)

type Logger struct {
	l log.Logger
}

func NewLogger(l log.Logger) *Logger {
	return &Logger{l: l}
}

func (l *Logger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	var logger log.Logger
	if data != nil {
		logger = log.With(l.l, data)
	} else {
		logger = l.l
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
