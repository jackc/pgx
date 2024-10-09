// sanitize_benchmark_test.go
package sanitize_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/internal/sanitize"
)

var benchmarkSanitizeResult string

const benchmarkQuery = "" +
	`SELECT * 
   FROM "water_containers" 
   WHERE NOT "id" = $1			-- int64
   AND "tags" NOT IN $2         -- nil
   AND "volume" > $3            -- float64
   AND "transportable" = $4     -- bool
   AND position($5 IN "sign")   -- bytes
   AND "label" LIKE $6          -- string
   AND "created_at" > $7;       -- time.Time`

var benchmarkArgs = []any{
	int64(12345),
	nil,
	float64(500),
	true,
	[]byte("8BADF00D"),
	"kombucha's han'dy awokowa",
	time.Date(2015, 10, 1, 0, 0, 0, 0, time.UTC),
}

func BenchmarkSanitize(b *testing.B) {
	query, err := sanitize.NewQuery(benchmarkQuery)
	if err != nil {
		b.Fatalf("failed to create query: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		benchmarkSanitizeResult, err = query.Sanitize(benchmarkArgs...)
		if err != nil {
			b.Fatalf("failed to sanitize query: %v", err)
		}
	}
}

var benchmarkNewSQLResult string

func BenchmarkSanitizeSQL(b *testing.B) {
	b.ReportAllocs()
	var err error
	for i := 0; i < b.N; i++ {
		benchmarkNewSQLResult, err = sanitize.SanitizeSQL(benchmarkQuery, benchmarkArgs...)
		if err != nil {
			b.Fatalf("failed to sanitize SQL: %v", err)
		}
	}
}
