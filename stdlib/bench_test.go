package stdlib_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func getSelectRowsCounts(b *testing.B) []int64 {
	var rowCounts []int64
	{
		s := os.Getenv("PGX_BENCH_SELECT_ROWS_COUNTS")
		if s != "" {
			for p := range strings.SplitSeq(s, " ") {
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
	db := openDB(b)
	defer closeDB(b, db)

	rowCounts := getSelectRowsCounts(b)

	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("%d rows", rowCount), func(b *testing.B) {
			br := &BenchRowSimple{}
			for b.Loop() {
				rows, err := db.Query("select n, 'Adam', 'Smith ' || n, 'male', '1952-06-16'::date, 258, 72, '2001-01-28 01:02:03-05'::timestamptz from generate_series(1, $1) n", rowCount)
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

type BenchRowNull struct {
	ID         sql.NullInt32
	FirstName  sql.NullString
	LastName   sql.NullString
	Sex        sql.NullString
	BirthDate  sql.NullTime
	Weight     sql.NullInt32
	Height     sql.NullInt32
	UpdateTime sql.NullTime
}

func BenchmarkSelectRowsScanNull(b *testing.B) {
	db := openDB(b)
	defer closeDB(b, db)

	rowCounts := getSelectRowsCounts(b)

	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("%d rows", rowCount), func(b *testing.B) {
			br := &BenchRowSimple{}
			for b.Loop() {
				rows, err := db.Query("select n, 'Adam', 'Smith ' || n, 'male', '1952-06-16'::date, 258, 72, '2001-01-28 01:02:03-05'::timestamptz from generate_series(100000, 100000 +  $1) n", rowCount)
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

const benchStringArraySize = 10

func benchStringArrayInput() []string {
	input := make([]string, benchStringArraySize)
	for i := range input {
		input[i] = fmt.Sprintf("String %d", i)
	}
	return input
}

func benchStringArraySelectSQL() string {
	var b strings.Builder
	b.WriteString("select array[")
	for i := 0; i < benchStringArraySize; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "'String %d'", i)
	}
	b.WriteString("]::text[]")
	return b.String()
}

// BenchmarkStringArrayEncodeArgument measures encoding a Go []string into a PostgreSQL
// array parameter. The encode path is unchanged by RowsColumnScanner, so this number
// should be the same on Go 1.26 and Go 1.27.
func BenchmarkStringArrayEncodeArgument(b *testing.B) {
	db := openDB(b)
	defer closeDB(b, db)

	input := benchStringArrayInput()
	b.ResetTimer()

	for b.Loop() {
		var n int64
		err := db.QueryRow("select cardinality($1::text[])", input).Scan(&n)
		if err != nil {
			b.Fatal(err)
		}
		if n != int64(len(input)) {
			b.Fatalf("Expected %d, got %d", len(input), n)
		}
	}
}

// BenchmarkStringArrayScanResultSQLScanner scans a PostgreSQL text[] result into a
// []string using the *pgtype.Map.SQLScanner adapter. This is the only way to do this with
// stdlib before Go 1.27.
func BenchmarkStringArrayScanResultSQLScanner(b *testing.B) {
	db := openDB(b)
	defer closeDB(b, db)

	m := pgtype.NewMap()
	query := benchStringArraySelectSQL()
	b.ResetTimer()

	for b.Loop() {
		var result []string
		err := db.QueryRow(query).Scan(m.SQLScanner(&result))
		if err != nil {
			b.Fatal(err)
		}
		if len(result) != benchStringArraySize {
			b.Fatalf("Expected %d, got %d", benchStringArraySize, len(result))
		}
	}
}

// BenchmarkStringArrayScanResultNativePgx scans a PostgreSQL text[] result into a
// []string using native pgx (bypassing database/sql entirely). This is the upper-bound
// performance reference: the stdlib variants pay for the extra database/sql layer.
func BenchmarkStringArrayScanResultNativePgx(b *testing.B) {
	ctx := context.Background()

	config, err := pgx.ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		b.Fatal(err)
	}

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close(ctx)

	query := benchStringArraySelectSQL()
	b.ResetTimer()

	for b.Loop() {
		var result []string
		err := conn.QueryRow(ctx, query).Scan(&result)
		if err != nil {
			b.Fatal(err)
		}
		if len(result) != benchStringArraySize {
			b.Fatalf("Expected %d, got %d", benchStringArraySize, len(result))
		}
	}
}
