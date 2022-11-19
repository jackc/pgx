// Package tracelog provides a tracer that acts as a traditional logger.
package tracelog

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// LogLevel represents the pgx logging level. See LogLevel* constants for
// possible values.
type LogLevel int

// The values for log levels are chosen such that the zero value means that no
// log level was specified.
const (
	LogLevelTrace = LogLevel(6)
	LogLevelDebug = LogLevel(5)
	LogLevelInfo  = LogLevel(4)
	LogLevelWarn  = LogLevel(3)
	LogLevelError = LogLevel(2)
	LogLevelNone  = LogLevel(1)
)

func (ll LogLevel) String() string {
	switch ll {
	case LogLevelTrace:
		return "trace"
	case LogLevelDebug:
		return "debug"
	case LogLevelInfo:
		return "info"
	case LogLevelWarn:
		return "warn"
	case LogLevelError:
		return "error"
	case LogLevelNone:
		return "none"
	default:
		return fmt.Sprintf("invalid level %d", ll)
	}
}

// Logger is the interface used to get log output from pgx.
type Logger interface {
	// Log a message at the given level with data key/value pairs. data may be nil.
	Log(ctx context.Context, level LogLevel, msg string, data map[string]any)
}

// LoggerFunc is a wrapper around a function to satisfy the pgx.Logger interface
type LoggerFunc func(ctx context.Context, level LogLevel, msg string, data map[string]interface{})

// Log delegates the logging request to the wrapped function
func (f LoggerFunc) Log(ctx context.Context, level LogLevel, msg string, data map[string]interface{}) {
	f(ctx, level, msg, data)
}

// LogLevelFromString converts log level string to constant
//
// Valid levels:
//
//	trace
//	debug
//	info
//	warn
//	error
//	none
func LogLevelFromString(s string) (LogLevel, error) {
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

func logQueryArgs(args []any) []any {
	logArgs := make([]any, 0, len(args))

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
				var l int = 0
				for w := 0; l < 64; l += w {
					_, w = utf8.DecodeRuneInString(v[l:])
				}
				if len(v) > l {
					a = fmt.Sprintf("%s (truncated %d bytes)", v[:l], len(v)-l)
				}
			}
		}
		logArgs = append(logArgs, a)
	}

	return logArgs
}

// TraceLog implements pgx.QueryTracer, pgx.BatchTracer, pgx.ConnectTracer, and pgx.CopyFromTracer. All fields are
// required.
type TraceLog struct {
	Logger   Logger
	LogLevel LogLevel
}

type ctxKey int

const (
	_ ctxKey = iota
	tracelogQueryCtxKey
	tracelogBatchCtxKey
	tracelogCopyFromCtxKey
	tracelogConnectCtxKey
	tracelogPrepareCtxKey
)

type traceQueryData struct {
	startTime time.Time
	sql       string
	args      []any
}

func (tl *TraceLog) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, tracelogQueryCtxKey, &traceQueryData{
		startTime: time.Now(),
		sql:       data.SQL,
		args:      data.Args,
	})
}

func (tl *TraceLog) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	queryData := ctx.Value(tracelogQueryCtxKey).(*traceQueryData)

	endTime := time.Now()
	interval := endTime.Sub(queryData.startTime)

	if data.Err != nil {
		if tl.shouldLog(LogLevelError) {
			tl.log(ctx, conn, LogLevelError, "Query", map[string]any{"sql": queryData.sql, "args": logQueryArgs(queryData.args), "err": data.Err, "time": interval})
		}
		return
	}

	if tl.shouldLog(LogLevelInfo) {
		tl.log(ctx, conn, LogLevelInfo, "Query", map[string]any{"sql": queryData.sql, "args": logQueryArgs(queryData.args), "time": interval, "commandTag": data.CommandTag.String()})
	}
}

type traceBatchData struct {
	startTime time.Time
}

func (tl *TraceLog) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	return context.WithValue(ctx, tracelogBatchCtxKey, &traceBatchData{
		startTime: time.Now(),
	})
}

func (tl *TraceLog) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	if data.Err != nil {
		if tl.shouldLog(LogLevelError) {
			tl.log(ctx, conn, LogLevelError, "BatchQuery", map[string]any{"sql": data.SQL, "args": logQueryArgs(data.Args), "err": data.Err})
		}
		return
	}

	if tl.shouldLog(LogLevelInfo) {
		tl.log(ctx, conn, LogLevelInfo, "BatchQuery", map[string]any{"sql": data.SQL, "args": logQueryArgs(data.Args), "commandTag": data.CommandTag.String()})
	}
}

func (tl *TraceLog) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	queryData := ctx.Value(tracelogBatchCtxKey).(*traceBatchData)

	endTime := time.Now()
	interval := endTime.Sub(queryData.startTime)

	if data.Err != nil {
		if tl.shouldLog(LogLevelError) {
			tl.log(ctx, conn, LogLevelError, "BatchClose", map[string]any{"err": data.Err, "time": interval})
		}
		return
	}

	if tl.shouldLog(LogLevelInfo) {
		tl.log(ctx, conn, LogLevelInfo, "BatchClose", map[string]any{"time": interval})
	}
}

type traceCopyFromData struct {
	startTime   time.Time
	TableName   pgx.Identifier
	ColumnNames []string
}

func (tl *TraceLog) TraceCopyFromStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	return context.WithValue(ctx, tracelogCopyFromCtxKey, &traceCopyFromData{
		startTime:   time.Now(),
		TableName:   data.TableName,
		ColumnNames: data.ColumnNames,
	})
}

func (tl *TraceLog) TraceCopyFromEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceCopyFromEndData) {
	copyFromData := ctx.Value(tracelogCopyFromCtxKey).(*traceCopyFromData)

	endTime := time.Now()
	interval := endTime.Sub(copyFromData.startTime)

	if data.Err != nil {
		if tl.shouldLog(LogLevelError) {
			tl.log(ctx, conn, LogLevelError, "CopyFrom", map[string]any{"tableName": copyFromData.TableName, "columnNames": copyFromData.ColumnNames, "err": data.Err, "time": interval})
		}
		return
	}

	if tl.shouldLog(LogLevelInfo) {
		tl.log(ctx, conn, LogLevelInfo, "CopyFrom", map[string]any{"tableName": copyFromData.TableName, "columnNames": copyFromData.ColumnNames, "err": data.Err, "time": interval, "rowCount": data.CommandTag.RowsAffected()})
	}
}

type traceConnectData struct {
	startTime  time.Time
	connConfig *pgx.ConnConfig
}

func (tl *TraceLog) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	return context.WithValue(ctx, tracelogConnectCtxKey, &traceConnectData{
		startTime:  time.Now(),
		connConfig: data.ConnConfig,
	})
}

func (tl *TraceLog) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	connectData := ctx.Value(tracelogConnectCtxKey).(*traceConnectData)

	endTime := time.Now()
	interval := endTime.Sub(connectData.startTime)

	if data.Err != nil {
		if tl.shouldLog(LogLevelError) {
			tl.Logger.Log(ctx, LogLevelError, "Connect", map[string]any{
				"host":     connectData.connConfig.Host,
				"port":     connectData.connConfig.Port,
				"database": connectData.connConfig.Database,
				"time":     interval,
				"err":      data.Err,
			})
		}
		return
	}

	if data.Conn != nil {
		if tl.shouldLog(LogLevelInfo) {
			tl.log(ctx, data.Conn, LogLevelInfo, "Connect", map[string]any{
				"host":     connectData.connConfig.Host,
				"port":     connectData.connConfig.Port,
				"database": connectData.connConfig.Database,
				"time":     interval,
			})
		}
	}
}

type tracePrepareData struct {
	startTime time.Time
	name      string
	sql       string
}

func (tl *TraceLog) TracePrepareStart(ctx context.Context, _ *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	return context.WithValue(ctx, tracelogPrepareCtxKey, &tracePrepareData{
		startTime: time.Now(),
		name:      data.Name,
		sql:       data.SQL,
	})
}

func (tl *TraceLog) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	prepareData := ctx.Value(tracelogPrepareCtxKey).(*tracePrepareData)

	endTime := time.Now()
	interval := endTime.Sub(prepareData.startTime)

	if data.Err != nil {
		if tl.shouldLog(LogLevelError) {
			tl.log(ctx, conn, LogLevelError, "Prepare", map[string]any{"name": prepareData.name, "sql": prepareData.sql, "err": data.Err, "time": interval})
		}
		return
	}

	if tl.shouldLog(LogLevelInfo) {
		tl.log(ctx, conn, LogLevelInfo, "Prepare", map[string]any{"name": prepareData.name, "sql": prepareData.sql, "time": interval, "alreadyPrepared": data.AlreadyPrepared})
	}
}

func (tl *TraceLog) shouldLog(lvl LogLevel) bool {
	return tl.LogLevel >= lvl
}

func (tl *TraceLog) log(ctx context.Context, conn *pgx.Conn, lvl LogLevel, msg string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}

	pgConn := conn.PgConn()
	if pgConn != nil {
		pid := pgConn.PID()
		if pid != 0 {
			data["pid"] = pid
		}
	}

	tl.Logger.Log(ctx, lvl, msg, data)
}
