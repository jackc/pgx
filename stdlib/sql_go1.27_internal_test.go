//go:build go1.27

package stdlib

import (
	"database/sql"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestIsSQLScanDestination(t *testing.T) {
	type namedString string
	type pointerCycle *pointerCycle

	for _, tt := range []struct {
		typ  reflect.Type
		want bool
	}{
		{reflect.TypeFor[*int64](), true},
		{reflect.TypeFor[**namedString](), true},
		{reflect.TypeFor[*any](), true},
		{reflect.TypeFor[*[]byte](), true},
		{reflect.TypeFor[*sql.RawBytes](), true},
		{reflect.TypeFor[*time.Time](), true},
		{reflect.TypeFor[*sql.NullString](), true},
		{reflect.TypeFor[**sql.NullString](), true},
		{reflect.TypeFor[*[]string](), false},
		{reflect.TypeFor[*pgtype.FlatArray[byte]](), false},
		{reflect.TypeFor[**pgtype.FlatArray[byte]](), false},
		{reflect.TypeFor[*pgtype.PreallocBytes](), false},
		{reflect.TypeFor[*pgtype.Range[int32]](), false},
		{reflect.TypeFor[pointerCycle](), false},
	} {
		t.Run(tt.typ.String(), func(t *testing.T) {
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					for range 20 {
						if got := isSQLScanDestination(tt.typ); got != tt.want {
							t.Errorf("isSQLScanDestination(%v) = %v, want %v", tt.typ, got, tt.want)
						}
					}
				})
			}
			wg.Wait()
		})
	}
}

func BenchmarkIsSQLScanDestination(b *testing.B) {
	for _, typ := range []reflect.Type{
		reflect.TypeFor[*int64](),
		reflect.TypeFor[*time.Time](),
		reflect.TypeFor[*sql.NullString](),
		reflect.TypeFor[*pgtype.FlatArray[byte]](),
	} {
		b.Run(typ.String(), func(b *testing.B) {
			b.Run("uncached", func(b *testing.B) {
				for b.Loop() {
					isSQLScanDestinationUncached(typ)
				}
			})
			b.Run("cached", func(b *testing.B) {
				isSQLScanDestination(typ)
				for b.Loop() {
					isSQLScanDestination(typ)
				}
			})
		})
	}
}
