package zerologadapter_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/rs/zerolog"
)

func TestLogger(t *testing.T) {

	t.Run("default", func(t *testing.T) {
		var buf bytes.Buffer
		zlogger := zerolog.New(&buf)
		logger := zerologadapter.NewLogger(zlogger)
		logger.Log(context.Background(), pgx.LogLevelInfo, "hello", map[string]interface{}{"one": "two"})
		const want = `{"level":"info","module":"pgx","one":"two","message":"hello"}
`
		got := buf.String()
		if got != want {
			t.Errorf("%s != %s", got, want)
		}
	})

	t.Run("disable pgx module", func(t *testing.T) {
		var buf bytes.Buffer
		zlogger := zerolog.New(&buf)
		logger := zerologadapter.NewLogger(zlogger, zerologadapter.WithoutPGXModule())
		logger.Log(context.Background(), pgx.LogLevelInfo, "hello", nil)
		const want = `{"level":"info","message":"hello"}
`
		got := buf.String()
		if got != want {
			t.Errorf("%s != %s", got, want)
		}
	})

	t.Run("from context", func(t *testing.T) {
		var buf bytes.Buffer
		zlogger := zerolog.New(&buf)
		ctx := zlogger.WithContext(context.Background())
		logger := zerologadapter.NewContextLogger()
		logger.Log(ctx, pgx.LogLevelInfo, "hello", map[string]interface{}{"one": "two"})
		const want = `{"level":"info","module":"pgx","one":"two","message":"hello"}
`

		got := buf.String()
		if got != want {
			t.Log(got)
			t.Log(want)
			t.Errorf("%s != %s", got, want)
		}
	})

	var buf bytes.Buffer
	type key string
	var ck key
	zlogger := zerolog.New(&buf)
	logger := zerologadapter.NewLogger(zlogger,
		zerologadapter.WithContextFunc(func(ctx context.Context, logWith zerolog.Context) zerolog.Context {
			// You can use zerolog.hlog.IDFromCtx(ctx) or even
			// zerolog.log.Ctx(ctx) to fetch the whole logger instance from the
			// context if you want.
			id, ok := ctx.Value(ck).(string)
			if ok {
				logWith = logWith.Str("req_id", id)
			}
			return logWith
		}),
	)

	t.Run("no request id", func(t *testing.T) {
		buf.Reset()
		ctx := context.Background()
		logger.Log(ctx, pgx.LogLevelInfo, "hello", nil)
		const want = `{"level":"info","module":"pgx","message":"hello"}
`
		got := buf.String()
		if got != want {
			t.Errorf("%s != %s", got, want)
		}
	})

	t.Run("with request id", func(t *testing.T) {
		buf.Reset()
		ctx := context.WithValue(context.Background(), ck, "1")
		logger.Log(ctx, pgx.LogLevelInfo, "hello", map[string]interface{}{"two": "2"})
		const want = `{"level":"info","module":"pgx","req_id":"1","two":"2","message":"hello"}
`
		got := buf.String()
		if got != want {
			t.Errorf("%s != %s", got, want)
		}
	})
}
