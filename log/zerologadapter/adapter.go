// Package zerologadapter provides a logger that writes to a github.com/rs/zerolog.
package zerologadapter

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog"
)

type Logger struct {
	logger      zerolog.Logger
	withFunc    func(context.Context, zerolog.Context) zerolog.Context
	fromContext bool
	skipModule  bool
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
	l.init(options)
	return &l
}

// NewContextLogger creates logger that extracts the zerolog.Logger from the
// context.Context by using `zerolog.Ctx`. The zerolog.DefaultContextLogger will
// be used if no logger is associated with the context.
func NewContextLogger(options ...option) *Logger {
	l := Logger{
		fromContext: true,
	}
	l.init(options)
	return &l
}

func (pl *Logger) init(options []option) {
	for _, opt := range options {
		opt(pl)
	}
	if !pl.skipModule {
		pl.logger = pl.logger.With().Str("module", "pgx").Logger()
	}
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

	var zctx zerolog.Context
	if pl.fromContext {
		logger := zerolog.Ctx(ctx)
		zctx = logger.With()
	} else {
		zctx = pl.logger.With()
	}
	if pl.withFunc != nil {
		zctx = pl.withFunc(ctx, zctx)
	}

	pgxlog := zctx.Logger()
	event := pgxlog.WithLevel(zlevel)
	if event.Enabled() {
		if pl.fromContext && !pl.skipModule {
			event.Str("module", "pgx")
		}
		event.Fields(data).Msg(msg)
	}
}
