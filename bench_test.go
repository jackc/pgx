package pgx_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgconn/stmtcache"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func BenchmarkMinimalUnpreparedSelectWithoutStatementCache(b *testing.B) {
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = nil

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	var n int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := conn.QueryRow(context.Background(), "select $1::int8", i).Scan(&n)
		if err != nil {
			b.Fatal(err)
		}

		if n != int64(i) {
			b.Fatalf("expected %d, got %d", i, n)
		}
	}
}

func BenchmarkMinimalUnpreparedSelectWithStatementCacheModeDescribe(b *testing.B) {
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = func(conn *pgconn.PgConn) stmtcache.Cache {
		return stmtcache.New(conn, stmtcache.ModeDescribe, 32)
	}

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	var n int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := conn.QueryRow(context.Background(), "select $1::int8", i).Scan(&n)
		if err != nil {
			b.Fatal(err)
		}

		if n != int64(i) {
			b.Fatalf("expected %d, got %d", i, n)
		}
	}
}

func BenchmarkMinimalUnpreparedSelectWithStatementCacheModePrepare(b *testing.B) {
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = func(conn *pgconn.PgConn) stmtcache.Cache {
		return stmtcache.New(conn, stmtcache.ModePrepare, 32)
	}

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	var n int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := conn.QueryRow(context.Background(), "select $1::int8", i).Scan(&n)
		if err != nil {
			b.Fatal(err)
		}

		if n != int64(i) {
			b.Fatalf("expected %d, got %d", i, n)
		}
	}
}

func BenchmarkMinimalPreparedSelect(b *testing.B) {
	conn := mustConnect(b, mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE")))
	defer closeConn(b, conn)

	_, err := conn.Prepare(context.Background(), "ps1", "select $1::int8")
	if err != nil {
		b.Fatal(err)
	}

	var n int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = conn.QueryRow(context.Background(), "ps1", i).Scan(&n)
		if err != nil {
			b.Fatal(err)
		}

		if n != int64(i) {
			b.Fatalf("expected %d, got %d", i, n)
		}
	}
}

func BenchmarkMinimalPgConnPreparedSelect(b *testing.B) {
	conn := mustConnect(b, mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE")))
	defer closeConn(b, conn)

	pgConn := conn.PgConn()

	_, err := pgConn.Prepare(context.Background(), "ps1", "select $1::int8", nil)
	if err != nil {
		b.Fatal(err)
	}

	encodedBytes := make([]byte, 8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		rr := pgConn.ExecPrepared(context.Background(), "ps1", [][]byte{encodedBytes}, []int16{1}, []int16{1})
		if err != nil {
			b.Fatal(err)
		}

		for rr.NextRow() {
			for i := range rr.Values() {
				if bytes.Compare(rr.Values()[0], encodedBytes) != 0 {
					b.Fatalf("unexpected values: %s %s", rr.Values()[i], encodedBytes)
				}
			}
		}
		_, err = rr.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPointerPointerWithNullValues(b *testing.B) {
	conn := mustConnect(b, mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE")))
	defer closeConn(b, conn)

	_, err := conn.Prepare(context.Background(), "selectNulls", "select 1::int4, 'johnsmith', null::text, null::text, null::text, null::date, null::timestamptz")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var record struct {
			id            int32
			userName      string
			email         *string
			name          *string
			sex           *string
			birthDate     *time.Time
			lastLoginTime *time.Time
		}

		err = conn.QueryRow(context.Background(), "selectNulls").Scan(
			&record.id,
			&record.userName,
			&record.email,
			&record.name,
			&record.sex,
			&record.birthDate,
			&record.lastLoginTime,
		)
		if err != nil {
			b.Fatal(err)
		}

		// These checks both ensure that the correct data was returned
		// and provide a benchmark of accessing the returned values.
		if record.id != 1 {
			b.Fatalf("bad value for id: %v", record.id)
		}
		if record.userName != "johnsmith" {
			b.Fatalf("bad value for userName: %v", record.userName)
		}
		if record.email != nil {
			b.Fatalf("bad value for email: %v", record.email)
		}
		if record.name != nil {
			b.Fatalf("bad value for name: %v", record.name)
		}
		if record.sex != nil {
			b.Fatalf("bad value for sex: %v", record.sex)
		}
		if record.birthDate != nil {
			b.Fatalf("bad value for birthDate: %v", record.birthDate)
		}
		if record.lastLoginTime != nil {
			b.Fatalf("bad value for lastLoginTime: %v", record.lastLoginTime)
		}
	}
}

func BenchmarkPointerPointerWithPresentValues(b *testing.B) {
	conn := mustConnect(b, mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE")))
	defer closeConn(b, conn)

	_, err := conn.Prepare(context.Background(), "selectNulls", "select 1::int4, 'johnsmith', 'johnsmith@example.com', 'John Smith', 'male', '1970-01-01'::date, '2015-01-01 00:00:00'::timestamptz")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var record struct {
			id            int32
			userName      string
			email         *string
			name          *string
			sex           *string
			birthDate     *time.Time
			lastLoginTime *time.Time
		}

		err = conn.QueryRow(context.Background(), "selectNulls").Scan(
			&record.id,
			&record.userName,
			&record.email,
			&record.name,
			&record.sex,
			&record.birthDate,
			&record.lastLoginTime,
		)
		if err != nil {
			b.Fatal(err)
		}

		// These checks both ensure that the correct data was returned
		// and provide a benchmark of accessing the returned values.
		if record.id != 1 {
			b.Fatalf("bad value for id: %v", record.id)
		}
		if record.userName != "johnsmith" {
			b.Fatalf("bad value for userName: %v", record.userName)
		}
		if record.email == nil || *record.email != "johnsmith@example.com" {
			b.Fatalf("bad value for email: %v", record.email)
		}
		if record.name == nil || *record.name != "John Smith" {
			b.Fatalf("bad value for name: %v", record.name)
		}
		if record.sex == nil || *record.sex != "male" {
			b.Fatalf("bad value for sex: %v", record.sex)
		}
		if record.birthDate == nil || *record.birthDate != time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) {
			b.Fatalf("bad value for birthDate: %v", record.birthDate)
		}
		if record.lastLoginTime == nil || *record.lastLoginTime != time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local) {
			b.Fatalf("bad value for lastLoginTime: %v", record.lastLoginTime)
		}
	}
}

func BenchmarkSelectWithoutLogging(b *testing.B) {
	conn := mustConnect(b, mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE")))
	defer closeConn(b, conn)

	benchmarkSelectWithLog(b, conn)
}

type discardLogger struct{}

func (dl discardLogger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
}

func BenchmarkSelectWithLoggingTraceDiscard(b *testing.B) {
	var logger discardLogger
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.Logger = logger
	config.LogLevel = pgx.LogLevelTrace

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	benchmarkSelectWithLog(b, conn)
}

func BenchmarkSelectWithLoggingDebugWithDiscard(b *testing.B) {
	var logger discardLogger
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.Logger = logger
	config.LogLevel = pgx.LogLevelDebug

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	benchmarkSelectWithLog(b, conn)
}

func BenchmarkSelectWithLoggingInfoWithDiscard(b *testing.B) {
	var logger discardLogger
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.Logger = logger
	config.LogLevel = pgx.LogLevelInfo

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	benchmarkSelectWithLog(b, conn)
}

func BenchmarkSelectWithLoggingErrorWithDiscard(b *testing.B) {
	var logger discardLogger
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.Logger = logger
	config.LogLevel = pgx.LogLevelError

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	benchmarkSelectWithLog(b, conn)
}

func benchmarkSelectWithLog(b *testing.B, conn *pgx.Conn) {
	_, err := conn.Prepare(context.Background(), "test", "select 1::int4, 'johnsmith', 'johnsmith@example.com', 'John Smith', 'male', '1970-01-01'::date, '2015-01-01 00:00:00'::timestamptz")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var record struct {
			id            int32
			userName      string
			email         string
			name          string
			sex           string
			birthDate     time.Time
			lastLoginTime time.Time
		}

		err = conn.QueryRow(context.Background(), "test").Scan(
			&record.id,
			&record.userName,
			&record.email,
			&record.name,
			&record.sex,
			&record.birthDate,
			&record.lastLoginTime,
		)
		if err != nil {
			b.Fatal(err)
		}

		// These checks both ensure that the correct data was returned
		// and provide a benchmark of accessing the returned values.
		if record.id != 1 {
			b.Fatalf("bad value for id: %v", record.id)
		}
		if record.userName != "johnsmith" {
			b.Fatalf("bad value for userName: %v", record.userName)
		}
		if record.email != "johnsmith@example.com" {
			b.Fatalf("bad value for email: %v", record.email)
		}
		if record.name != "John Smith" {
			b.Fatalf("bad value for name: %v", record.name)
		}
		if record.sex != "male" {
			b.Fatalf("bad value for sex: %v", record.sex)
		}
		if record.birthDate != time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) {
			b.Fatalf("bad value for birthDate: %v", record.birthDate)
		}
		if record.lastLoginTime != time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local) {
			b.Fatalf("bad value for lastLoginTime: %v", record.lastLoginTime)
		}
	}
}

const benchmarkWriteTableCreateSQL = `drop table if exists t;

create table t(
	varchar_1 varchar not null,
	varchar_2 varchar not null,
	varchar_null_1 varchar,
	date_1 date not null,
	date_null_1 date,
	int4_1 int4 not null,
	int4_2 int4 not null,
	int4_null_1 int4,
	tstz_1 timestamptz not null,
	tstz_2 timestamptz,
	bool_1 bool not null,
	bool_2 bool not null,
	bool_3 bool not null
);
`

const benchmarkWriteTableInsertSQL = `insert into t(
	varchar_1,
	varchar_2,
	varchar_null_1,
	date_1,
	date_null_1,
	int4_1,
	int4_2,
	int4_null_1,
	tstz_1,
	tstz_2,
	bool_1,
	bool_2,
	bool_3
) values (
	$1::varchar,
	$2::varchar,
	$3::varchar,
	$4::date,
	$5::date,
	$6::int4,
	$7::int4,
	$8::int4,
	$9::timestamptz,
	$10::timestamptz,
	$11::bool,
	$12::bool,
	$13::bool
)`

type benchmarkWriteTableCopyFromSrc struct {
	count int
	idx   int
	row   []interface{}
}

func (s *benchmarkWriteTableCopyFromSrc) Next() bool {
	s.idx++
	return s.idx < s.count
}

func (s *benchmarkWriteTableCopyFromSrc) Values() ([]interface{}, error) {
	return s.row, nil
}

func (s *benchmarkWriteTableCopyFromSrc) Err() error {
	return nil
}

func newBenchmarkWriteTableCopyFromSrc(count int) pgx.CopyFromSource {
	return &benchmarkWriteTableCopyFromSrc{
		count: count,
		row: []interface{}{
			"varchar_1",
			"varchar_2",
			&pgtype.Text{Status: pgtype.Null},
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.Local),
			&pgtype.Date{Status: pgtype.Null},
			1,
			2,
			&pgtype.Int4{Status: pgtype.Null},
			time.Date(2001, 1, 1, 0, 0, 0, 0, time.Local),
			time.Date(2002, 1, 1, 0, 0, 0, 0, time.Local),
			true,
			false,
			true,
		},
	}
}

func benchmarkWriteNRowsViaInsert(b *testing.B, n int) {
	conn := mustConnect(b, mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE")))
	defer closeConn(b, conn)

	mustExec(b, conn, benchmarkWriteTableCreateSQL)
	_, err := conn.Prepare(context.Background(), "insert_t", benchmarkWriteTableInsertSQL)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		src := newBenchmarkWriteTableCopyFromSrc(n)

		tx, err := conn.Begin(context.Background())
		if err != nil {
			b.Fatal(err)
		}

		for src.Next() {
			values, _ := src.Values()
			if _, err = tx.Exec(context.Background(), "insert_t", values...); err != nil {
				b.Fatalf("Exec unexpectedly failed with: %v", err)
			}
		}

		err = tx.Commit(context.Background())
		if err != nil {
			b.Fatal(err)
		}
	}
}

type queryArgs []interface{}

func (qa *queryArgs) Append(v interface{}) string {
	*qa = append(*qa, v)
	return "$" + strconv.Itoa(len(*qa))
}

// note this function is only used for benchmarks -- it doesn't escape tableName
// or columnNames
func multiInsert(conn *pgx.Conn, tableName string, columnNames []string, rowSrc pgx.CopyFromSource) (int, error) {
	maxRowsPerInsert := 65535 / len(columnNames)
	rowsThisInsert := 0
	rowCount := 0

	sqlBuf := &bytes.Buffer{}
	args := make(queryArgs, 0)

	resetQuery := func() {
		sqlBuf.Reset()
		fmt.Fprintf(sqlBuf, "insert into %s(%s) values", tableName, strings.Join(columnNames, ", "))

		args = args[0:0]

		rowsThisInsert = 0
	}
	resetQuery()

	tx, err := conn.Begin(context.Background())
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())

	for rowSrc.Next() {
		if rowsThisInsert > 0 {
			sqlBuf.WriteByte(',')
		}

		sqlBuf.WriteByte('(')

		values, err := rowSrc.Values()
		if err != nil {
			return 0, err
		}

		for i, val := range values {
			if i > 0 {
				sqlBuf.WriteByte(',')
			}
			sqlBuf.WriteString(args.Append(val))
		}

		sqlBuf.WriteByte(')')

		rowsThisInsert++

		if rowsThisInsert == maxRowsPerInsert {
			_, err := tx.Exec(context.Background(), sqlBuf.String(), args...)
			if err != nil {
				return 0, err
			}

			rowCount += rowsThisInsert
			resetQuery()
		}
	}

	if rowsThisInsert > 0 {
		_, err := tx.Exec(context.Background(), sqlBuf.String(), args...)
		if err != nil {
			return 0, err
		}

		rowCount += rowsThisInsert
	}

	if err := tx.Commit(context.Background()); err != nil {
		return 0, nil
	}

	return rowCount, nil

}

func benchmarkWriteNRowsViaMultiInsert(b *testing.B, n int) {
	conn := mustConnect(b, mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE")))
	defer closeConn(b, conn)

	mustExec(b, conn, benchmarkWriteTableCreateSQL)
	_, err := conn.Prepare(context.Background(), "insert_t", benchmarkWriteTableInsertSQL)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		src := newBenchmarkWriteTableCopyFromSrc(n)

		_, err := multiInsert(conn, "t",
			[]string{"varchar_1",
				"varchar_2",
				"varchar_null_1",
				"date_1",
				"date_null_1",
				"int4_1",
				"int4_2",
				"int4_null_1",
				"tstz_1",
				"tstz_2",
				"bool_1",
				"bool_2",
				"bool_3"},
			src)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkWriteNRowsViaCopy(b *testing.B, n int) {
	conn := mustConnect(b, mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE")))
	defer closeConn(b, conn)

	mustExec(b, conn, benchmarkWriteTableCreateSQL)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		src := newBenchmarkWriteTableCopyFromSrc(n)

		_, err := conn.CopyFrom(context.Background(),
			pgx.Identifier{"t"},
			[]string{"varchar_1",
				"varchar_2",
				"varchar_null_1",
				"date_1",
				"date_null_1",
				"int4_1",
				"int4_2",
				"int4_null_1",
				"tstz_1",
				"tstz_2",
				"bool_1",
				"bool_2",
				"bool_3"},
			src)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWrite5RowsViaInsert(b *testing.B) {
	benchmarkWriteNRowsViaInsert(b, 5)
}

func BenchmarkWrite5RowsViaMultiInsert(b *testing.B) {
	benchmarkWriteNRowsViaMultiInsert(b, 5)
}

func BenchmarkWrite5RowsViaCopy(b *testing.B) {
	benchmarkWriteNRowsViaCopy(b, 5)
}

func BenchmarkWrite10RowsViaInsert(b *testing.B) {
	benchmarkWriteNRowsViaInsert(b, 10)
}

func BenchmarkWrite10RowsViaMultiInsert(b *testing.B) {
	benchmarkWriteNRowsViaMultiInsert(b, 10)
}

func BenchmarkWrite10RowsViaCopy(b *testing.B) {
	benchmarkWriteNRowsViaCopy(b, 10)
}

func BenchmarkWrite100RowsViaInsert(b *testing.B) {
	benchmarkWriteNRowsViaInsert(b, 100)
}

func BenchmarkWrite100RowsViaMultiInsert(b *testing.B) {
	benchmarkWriteNRowsViaMultiInsert(b, 100)
}

func BenchmarkWrite100RowsViaCopy(b *testing.B) {
	benchmarkWriteNRowsViaCopy(b, 100)
}

func BenchmarkWrite1000RowsViaInsert(b *testing.B) {
	benchmarkWriteNRowsViaInsert(b, 1000)
}

func BenchmarkWrite1000RowsViaMultiInsert(b *testing.B) {
	benchmarkWriteNRowsViaMultiInsert(b, 1000)
}

func BenchmarkWrite1000RowsViaCopy(b *testing.B) {
	benchmarkWriteNRowsViaCopy(b, 1000)
}

func BenchmarkWrite10000RowsViaInsert(b *testing.B) {
	benchmarkWriteNRowsViaInsert(b, 10000)
}

func BenchmarkWrite10000RowsViaMultiInsert(b *testing.B) {
	benchmarkWriteNRowsViaMultiInsert(b, 10000)
}

func BenchmarkWrite10000RowsViaCopy(b *testing.B) {
	benchmarkWriteNRowsViaCopy(b, 10000)
}

func BenchmarkMultipleQueriesNonBatchNoStatementCache(b *testing.B) {
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = nil

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	benchmarkMultipleQueriesNonBatch(b, conn, 3)
}

func BenchmarkMultipleQueriesNonBatchPrepareStatementCache(b *testing.B) {
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = func(conn *pgconn.PgConn) stmtcache.Cache {
		return stmtcache.New(conn, stmtcache.ModePrepare, 32)
	}

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	benchmarkMultipleQueriesNonBatch(b, conn, 3)
}

func BenchmarkMultipleQueriesNonBatchDescribeStatementCache(b *testing.B) {
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = func(conn *pgconn.PgConn) stmtcache.Cache {
		return stmtcache.New(conn, stmtcache.ModeDescribe, 32)
	}

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	benchmarkMultipleQueriesNonBatch(b, conn, 3)
}

func benchmarkMultipleQueriesNonBatch(b *testing.B, conn *pgx.Conn, queryCount int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < queryCount; j++ {
			rows, err := conn.Query(context.Background(), "select n from generate_series(0, 5) n")
			if err != nil {
				b.Fatal(err)
			}

			for k := 0; rows.Next(); k++ {
				var n int
				if err := rows.Scan(&n); err != nil {
					b.Fatal(err)
				}
				if n != k {
					b.Fatalf("n => %v, want %v", n, k)
				}
			}

			if rows.Err() != nil {
				b.Fatal(rows.Err())
			}
		}
	}
}

func BenchmarkMultipleQueriesBatchNoStatementCache(b *testing.B) {
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = nil

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	benchmarkMultipleQueriesBatch(b, conn, 3)
}

func BenchmarkMultipleQueriesBatchPrepareStatementCache(b *testing.B) {
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = func(conn *pgconn.PgConn) stmtcache.Cache {
		return stmtcache.New(conn, stmtcache.ModePrepare, 32)
	}

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	benchmarkMultipleQueriesBatch(b, conn, 3)
}

func BenchmarkMultipleQueriesBatchDescribeStatementCache(b *testing.B) {
	config := mustParseConfig(b, os.Getenv("PGX_TEST_DATABASE"))
	config.BuildStatementCache = func(conn *pgconn.PgConn) stmtcache.Cache {
		return stmtcache.New(conn, stmtcache.ModeDescribe, 32)
	}

	conn := mustConnect(b, config)
	defer closeConn(b, conn)

	benchmarkMultipleQueriesBatch(b, conn, 3)
}

func benchmarkMultipleQueriesBatch(b *testing.B, conn *pgx.Conn, queryCount int) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := &pgx.Batch{}
		for j := 0; j < queryCount; j++ {
			batch.Queue("select n from generate_series(0,5) n")
		}

		br := conn.SendBatch(context.Background(), batch)

		for j := 0; j < queryCount; j++ {
			rows, err := br.Query()
			if err != nil {
				b.Fatal(err)
			}

			for k := 0; rows.Next(); k++ {
				var n int
				if err := rows.Scan(&n); err != nil {
					b.Fatal(err)
				}
				if n != k {
					b.Fatalf("n => %v, want %v", n, k)
				}
			}

			if rows.Err() != nil {
				b.Fatal(rows.Err())
			}
		}

		err := br.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectManyUnknownEnum(b *testing.B) {
	conn := mustConnectString(b, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(b, conn)

	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	require.NoError(b, err)
	defer tx.Rollback(ctx)

	_, err = tx.Exec(context.Background(), "drop type if exists color;")
	require.NoError(b, err)

	_, err = tx.Exec(ctx, `create type color as enum ('blue', 'green', 'orange')`)
	require.NoError(b, err)

	b.ResetTimer()
	var x, y, z string
	for i := 0; i < b.N; i++ {
		rows, err := conn.Query(ctx, "select 'blue'::color, 'green'::color, 'orange'::color from generate_series(1,10)")
		if err != nil {
			b.Fatal(err)
		}

		for rows.Next() {
			err = rows.Scan(&x, &y, &z)
			if err != nil {
				b.Fatal(err)
			}

			if x != "blue" {
				b.Fatal("unexpected result")
			}
			if y != "green" {
				b.Fatal("unexpected result")
			}
			if z != "orange" {
				b.Fatal("unexpected result")
			}
		}

		if rows.Err() != nil {
			b.Fatal(rows.Err())
		}
	}
}

func BenchmarkSelectManyRegisteredEnum(b *testing.B) {
	conn := mustConnectString(b, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(b, conn)

	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	require.NoError(b, err)
	defer tx.Rollback(ctx)

	_, err = tx.Exec(context.Background(), "drop type if exists color;")
	require.NoError(b, err)

	_, err = tx.Exec(ctx, `create type color as enum ('blue', 'green', 'orange')`)
	require.NoError(b, err)

	var oid uint32
	err = conn.QueryRow(context.Background(), "select oid from pg_type where typname=$1;", "color").Scan(&oid)
	require.NoError(b, err)

	et := pgtype.NewEnumType("color", []string{"blue", "green", "orange"})
	conn.ConnInfo().RegisterDataType(pgtype.DataType{Value: et, Name: "color", OID: oid})

	b.ResetTimer()
	var x, y, z string
	for i := 0; i < b.N; i++ {
		rows, err := conn.Query(ctx, "select 'blue'::color, 'green'::color, 'orange'::color from generate_series(1,10)")
		if err != nil {
			b.Fatal(err)
		}

		for rows.Next() {
			err = rows.Scan(&x, &y, &z)
			if err != nil {
				b.Fatal(err)
			}

			if x != "blue" {
				b.Fatal("unexpected result")
			}
			if y != "green" {
				b.Fatal("unexpected result")
			}
			if z != "orange" {
				b.Fatal("unexpected result")
			}
		}

		if rows.Err() != nil {
			b.Fatal(rows.Err())
		}
	}
}

func getSelectRowsCounts(b *testing.B) []int64 {
	var rowCounts []int64
	{
		s := os.Getenv("PGX_BENCH_SELECT_ROWS_COUNTS")
		if s != "" {
			for _, p := range strings.Split(s, " ") {
				n, err := strconv.ParseInt(p, 10, 64)
				if err != nil {
					b.Fatalf("Bad PGX_BENCH_SELECT_ROWS_COUNTS value: %v", err)
				}
				rowCounts = append(rowCounts, n)
			}
		}
	}

	if len(rowCounts) == 0 {
		rowCounts = []int64{1, 10, 100, 1000}
	}

	return rowCounts
}

type BenchRowSimple struct {
	ID         int32
	FirstName  string
	LastName   string
	Sex        string
	BirthDate  time.Time
	Weight     int32
	Height     int32
	UpdateTime time.Time
}

func BenchmarkSelectRowsScanSimple(b *testing.B) {
	conn := mustConnectString(b, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(b, conn)

	rowCounts := getSelectRowsCounts(b)

	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("%d rows", rowCount), func(b *testing.B) {
			br := &BenchRowSimple{}
			for i := 0; i < b.N; i++ {
				rows, err := conn.Query(context.Background(), "select n, 'Adam', 'Smith ' || n, 'male', '1952-06-16'::date, 258, 72, '2001-01-28 01:02:03-05'::timestamptz from generate_series(100001, 100000 + $1) n", rowCount)
				if err != nil {
					b.Fatal(err)
				}

				for rows.Next() {
					rows.Scan(&br.ID, &br.FirstName, &br.LastName, &br.Sex, &br.BirthDate, &br.Weight, &br.Height, &br.UpdateTime)
				}

				if rows.Err() != nil {
					b.Fatal(rows.Err())
				}
			}
		})
	}
}

type BenchRowStringBytes struct {
	ID         int32
	FirstName  []byte
	LastName   []byte
	Sex        []byte
	BirthDate  time.Time
	Weight     int32
	Height     int32
	UpdateTime time.Time
}

func BenchmarkSelectRowsScanStringBytes(b *testing.B) {
	conn := mustConnectString(b, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(b, conn)

	rowCounts := getSelectRowsCounts(b)

	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("%d rows", rowCount), func(b *testing.B) {
			br := &BenchRowStringBytes{}
			for i := 0; i < b.N; i++ {
				rows, err := conn.Query(context.Background(), "select n, 'Adam', 'Smith ' || n, 'male', '1952-06-16'::date, 258, 72, '2001-01-28 01:02:03-05'::timestamptz from generate_series(100001, 100000 + $1) n", rowCount)
				if err != nil {
					b.Fatal(err)
				}

				for rows.Next() {
					rows.Scan(&br.ID, &br.FirstName, &br.LastName, &br.Sex, &br.BirthDate, &br.Weight, &br.Height, &br.UpdateTime)
				}

				if rows.Err() != nil {
					b.Fatal(rows.Err())
				}
			}
		})
	}
}

type BenchRowDecoder struct {
	ID         pgtype.Int4
	FirstName  pgtype.Text
	LastName   pgtype.Text
	Sex        pgtype.Text
	BirthDate  pgtype.Date
	Weight     pgtype.Int4
	Height     pgtype.Int4
	UpdateTime pgtype.Timestamptz
}

func BenchmarkSelectRowsScanDecoder(b *testing.B) {
	conn := mustConnectString(b, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(b, conn)

	rowCounts := getSelectRowsCounts(b)

	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("%d rows", rowCount), func(b *testing.B) {
			formats := []struct {
				name string
				code int16
			}{
				{"text", pgx.TextFormatCode},
				{"binary", pgx.BinaryFormatCode},
			}
			for _, format := range formats {
				b.Run(format.name, func(b *testing.B) {

					br := &BenchRowDecoder{}
					for i := 0; i < b.N; i++ {
						rows, err := conn.Query(
							context.Background(),
							"select n, 'Adam', 'Smith ' || n, 'male', '1952-06-16'::date, 258, 72, '2001-01-28 01:02:03-05'::timestamptz from generate_series(100001, 100000 + $1) n",
							pgx.QueryResultFormats{format.code},
							rowCount,
						)
						if err != nil {
							b.Fatal(err)
						}

						for rows.Next() {
							rows.Scan(&br.ID, &br.FirstName, &br.LastName, &br.Sex, &br.BirthDate, &br.Weight, &br.Height, &br.UpdateTime)
						}

						if rows.Err() != nil {
							b.Fatal(rows.Err())
						}
					}
				})
			}
		})
	}
}

func BenchmarkSelectRowsExplicitDecoding(b *testing.B) {
	conn := mustConnectString(b, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(b, conn)

	rowCounts := getSelectRowsCounts(b)

	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("%d rows", rowCount), func(b *testing.B) {
			br := &BenchRowDecoder{}
			for i := 0; i < b.N; i++ {
				rows, err := conn.Query(context.Background(), "select n, 'Adam', 'Smith ' || n, 'male', '1952-06-16'::date, 258, 72, '2001-01-28 01:02:03-05'::timestamptz from generate_series(100001, 100000 + $1) n", rowCount)
				if err != nil {
					b.Fatal(err)
				}

				for rows.Next() {
					rawValues := rows.RawValues()

					err = br.ID.DecodeBinary(conn.ConnInfo(), rawValues[0])
					if err != nil {
						b.Fatal(err)
					}

					err = br.FirstName.DecodeText(conn.ConnInfo(), rawValues[1])
					if err != nil {
						b.Fatal(err)
					}

					err = br.LastName.DecodeText(conn.ConnInfo(), rawValues[2])
					if err != nil {
						b.Fatal(err)
					}

					err = br.Sex.DecodeText(conn.ConnInfo(), rawValues[3])
					if err != nil {
						b.Fatal(err)
					}

					err = br.BirthDate.DecodeBinary(conn.ConnInfo(), rawValues[4])
					if err != nil {
						b.Fatal(err)
					}

					err = br.Weight.DecodeBinary(conn.ConnInfo(), rawValues[5])
					if err != nil {
						b.Fatal(err)
					}

					err = br.Height.DecodeBinary(conn.ConnInfo(), rawValues[6])
					if err != nil {
						b.Fatal(err)
					}

					err = br.UpdateTime.DecodeBinary(conn.ConnInfo(), rawValues[7])
					if err != nil {
						b.Fatal(err)
					}
				}

				if rows.Err() != nil {
					b.Fatal(rows.Err())
				}
			}
		})
	}
}

func BenchmarkSelectRowsPgConnExecText(b *testing.B) {
	conn := mustConnectString(b, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(b, conn)

	rowCounts := getSelectRowsCounts(b)

	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("%d rows", rowCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				mrr := conn.PgConn().Exec(context.Background(), fmt.Sprintf("select n, 'Adam', 'Smith ' || n, 'male', '1952-06-16'::date, 258, 72, '2001-01-28 01:02:03-05'::timestamptz from generate_series(100001, 100000 + %d) n", rowCount))
				for mrr.NextResult() {
					rr := mrr.ResultReader()
					for rr.NextRow() {
						rr.Values()
					}
				}

				err := mrr.Close()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSelectRowsPgConnExecParams(b *testing.B) {
	conn := mustConnectString(b, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(b, conn)

	rowCounts := getSelectRowsCounts(b)

	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("%d rows", rowCount), func(b *testing.B) {
			formats := []struct {
				name string
				code int16
			}{
				{"text", pgx.TextFormatCode},
				{"binary - mostly", pgx.BinaryFormatCode},
			}
			for _, format := range formats {
				b.Run(format.name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						rr := conn.PgConn().ExecParams(
							context.Background(),
							"select n, 'Adam', 'Smith ' || n, 'male', '1952-06-16'::date, 258, 72, '2001-01-28 01:02:03-05'::timestamptz from generate_series(100001, 100000 + $1) n",
							[][]byte{[]byte(strconv.FormatInt(rowCount, 10))},
							nil,
							nil,
							[]int16{format.code, pgx.TextFormatCode, pgx.TextFormatCode, pgx.TextFormatCode, format.code, format.code, format.code, format.code},
						)
						for rr.NextRow() {
							rr.Values()
						}

						_, err := rr.Close()
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkSelectRowsPgConnExecPrepared(b *testing.B) {
	conn := mustConnectString(b, os.Getenv("PGX_TEST_DATABASE"))
	defer closeConn(b, conn)

	rowCounts := getSelectRowsCounts(b)

	_, err := conn.PgConn().Prepare(context.Background(), "ps1", "select n, 'Adam', 'Smith ' || n, 'male', '1952-06-16'::date, 258, 72, '2001-01-28 01:02:03-05'::timestamptz from generate_series(100001, 100000 + $1) n", nil)
	if err != nil {
		b.Fatal(err)
	}

	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("%d rows", rowCount), func(b *testing.B) {
			formats := []struct {
				name string
				code int16
			}{
				{"text", pgx.TextFormatCode},
				{"binary - mostly", pgx.BinaryFormatCode},
			}
			for _, format := range formats {
				b.Run(format.name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						rr := conn.PgConn().ExecPrepared(
							context.Background(),
							"ps1",
							[][]byte{[]byte(strconv.FormatInt(rowCount, 10))},
							nil,
							[]int16{format.code, pgx.TextFormatCode, pgx.TextFormatCode, pgx.TextFormatCode, format.code, format.code, format.code, format.code},
						)
						for rr.NextRow() {
							rr.Values()
						}

						_, err := rr.Close()
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

type queryRecorder struct {
	conn      net.Conn
	writeBuf  []byte
	readCount int
}

func (qr *queryRecorder) Read(b []byte) (n int, err error) {
	n, err = qr.conn.Read(b)
	qr.readCount += n
	return n, err
}

func (qr *queryRecorder) Write(b []byte) (n int, err error) {
	qr.writeBuf = append(qr.writeBuf, b...)
	return qr.conn.Write(b)
}

func (qr *queryRecorder) Close() error {
	return qr.conn.Close()
}

func (qr *queryRecorder) LocalAddr() net.Addr {
	return qr.conn.LocalAddr()
}

func (qr *queryRecorder) RemoteAddr() net.Addr {
	return qr.conn.RemoteAddr()
}

func (qr *queryRecorder) SetDeadline(t time.Time) error {
	return qr.conn.SetDeadline(t)
}

func (qr *queryRecorder) SetReadDeadline(t time.Time) error {
	return qr.conn.SetReadDeadline(t)
}

func (qr *queryRecorder) SetWriteDeadline(t time.Time) error {
	return qr.conn.SetWriteDeadline(t)
}

// BenchmarkSelectRowsRawPrepared hijacks a pgconn connection and inserts a queryRecorder. It then executes the query
// once. The benchmark is simply sending the exact query bytes over the wire to the server and reading the expected
// number of bytes back. It does nothing else. This should be the theoretical maximum performance a Go application
// could achieve.
func BenchmarkSelectRowsRawPrepared(b *testing.B) {
	rowCounts := getSelectRowsCounts(b)

	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("%d rows", rowCount), func(b *testing.B) {
			formats := []struct {
				name string
				code int16
			}{
				{"text", pgx.TextFormatCode},
				{"binary - mostly", pgx.BinaryFormatCode},
			}
			for _, format := range formats {
				b.Run(format.name, func(b *testing.B) {
					conn := mustConnectString(b, os.Getenv("PGX_TEST_DATABASE")).PgConn()
					defer conn.Close(context.Background())

					_, err := conn.Prepare(context.Background(), "ps1", "select n, 'Adam', 'Smith ' || n, 'male', '1952-06-16'::date, 258, 72, '2001-01-28 01:02:03-05'::timestamptz from generate_series(100001, 100000 + $1) n", nil)
					if err != nil {
						b.Fatal(err)
					}

					hijackedConn, err := conn.Hijack()
					require.NoError(b, err)

					qr := &queryRecorder{
						conn: hijackedConn.Conn,
					}

					hijackedConn.Conn = qr
					hijackedConn.Frontend = hijackedConn.Config.BuildFrontend(qr, qr)
					conn, err = pgconn.Construct(hijackedConn)
					require.NoError(b, err)

					{
						rr := conn.ExecPrepared(
							context.Background(),
							"ps1",
							[][]byte{[]byte(strconv.FormatInt(rowCount, 10))},
							nil,
							[]int16{format.code, pgx.TextFormatCode, pgx.TextFormatCode, pgx.TextFormatCode, format.code, format.code, format.code, format.code},
						)
						_, err := rr.Close()
						require.NoError(b, err)
					}

					buf := make([]byte, qr.readCount)

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						_, err := qr.conn.Write(qr.writeBuf)
						if err != nil {
							b.Fatal(err)
						}

						_, err = io.ReadFull(qr.conn, buf)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
