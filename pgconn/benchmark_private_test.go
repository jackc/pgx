package pgconn

import (
	"strings"
	"testing"
)

func BenchmarkCommandTagRowsAffected(b *testing.B) {
	benchmarks := []struct {
		commandTag   string
		rowsAffected int64
	}{
		{"UPDATE 1", 1},
		{"UPDATE 123456789", 123456789},
		{"INSERT 0 1", 1},
		{"INSERT 0 123456789", 123456789},
	}

	for _, bm := range benchmarks {
		ct := CommandTag{s: bm.commandTag}
		b.Run(bm.commandTag, func(b *testing.B) {
			var n int64
			for i := 0; i < b.N; i++ {
				n = ct.RowsAffected()
			}
			if n != bm.rowsAffected {
				b.Errorf("expected %d got %d", bm.rowsAffected, n)
			}
		})
	}
}

func BenchmarkCommandTagTypeFromString(b *testing.B) {
	ct := CommandTag{s: "UPDATE 1"}

	var update bool
	for i := 0; i < b.N; i++ {
		update = strings.HasPrefix(ct.String(), "UPDATE")
	}
	if !update {
		b.Error("expected update")
	}
}

func BenchmarkCommandTagInsert(b *testing.B) {
	benchmarks := []struct {
		commandTag string
		is         bool
	}{
		{"INSERT 1", true},
		{"INSERT 1234567890", true},
		{"UPDATE 1", false},
		{"UPDATE 1234567890", false},
		{"DELETE 1", false},
		{"DELETE 1234567890", false},
		{"SELECT 1", false},
		{"SELECT 1234567890", false},
		{"UNKNOWN 1234567890", false},
	}

	for _, bm := range benchmarks {
		ct := CommandTag{s: bm.commandTag}
		b.Run(bm.commandTag, func(b *testing.B) {
			var is bool
			for i := 0; i < b.N; i++ {
				is = ct.Insert()
			}
			if is != bm.is {
				b.Errorf("expected %v got %v", bm.is, is)
			}
		})
	}
}
