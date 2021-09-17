// Package zerologadapter provides a logger that writes to a github.com/rs/zerolog.
package zerologadapter

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog"
)

type Logger struct {
	logger     zerolog.Logger
	withFunc   func(context.Context, zerolog.Context) zerolog.Context
	skipModule bool
}

// option options for configuring the logger when creating a new logger.
type option func(logger *Logger)

// WithContextFunc adds possibility to get request scoped values from the
// ctx.Context before logging lines.
func WithContextFunc(withFunc func(context.Context, zerolog.Context) zerolog.Context) option {
	return func(logger *Logger) {
		logger.withFunc = withFunc
	}
}

// WithoutPGXModule disables adding module:pgx to the default logger context.
func WithoutPGXModule() option {
	return func(logger *Logger) {
		logger.skipModule = true
	}
}

// NewLogger accepts a zerolog.Logger as input and returns a new custom pgx
// logging facade as output.
func NewLogger(logger zerolog.Logger, options ...option) *Logger {
	l := Logger{
		logger: logger,
	}
	for _, opt := range options {
		opt(&l)
	}
	if !l.skipModule {
		l.logger = l.logger.With().Str("module", "pgx").Logger()
	}
	return &l
}

func (pl *Logger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	var zlevel zerolog.Level
	switch level {
	case pgx.LogLevelNone:
		zlevel = zerolog.NoLevel
	case pgx.LogLevelError:
		zlevel = zerolog.ErrorLevel
	case pgx.LogLevelWarn:
		zlevel = zerolog.WarnLevel
	case pgx.LogLevelInfo:
		zlevel = zerolog.InfoLevel
	case pgx.LogLevelDebug:
		zlevel = zerolog.DebugLevel
	default:
		zlevel = zerolog.DebugLevel
	}
	zctx := pl.logger.With()
	if pl.withFunc != nil {
		zctx = pl.withFunc(ctx, zctx)
	}
	pgxlog := zctx.Fields(data).Logger()
	pgxlog.WithLevel(zlevel).Msg(msg)
}
