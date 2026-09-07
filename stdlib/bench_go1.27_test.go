//go:build go1.27

package stdlib_test

import (
	"testing"
)

// BenchmarkStringArrayScanResultDirect scans a PostgreSQL text[] result directly into a
// []string via the new driver.RowsColumnScanner interface (no SQLScanner wrapper). This
// is the new path enabled by Go 1.27.
func BenchmarkStringArrayScanResultDirect(b *testing.B) {
	db := openDB(b)
	defer closeDB(b, db)

	query := benchStringArraySelectSQL()
	b.ResetTimer()

	for b.Loop() {
		var result []string
		err := db.QueryRow(query).Scan(&result)
		if err != nil {
			b.Fatal(err)
		}
		if len(result) != benchStringArraySize {
			b.Fatalf("Expected %d, got %d", benchStringArraySize, len(result))
		}
	}
}
