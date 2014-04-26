package pgx_test

import (
	"database/sql"
	"fmt"
	"github.com/JackC/pgx"
	_ "github.com/lib/pq"
	"math/rand"
	"testing"
)

var db *sql.DB

func init() {
	var err error
	db, err = sql.Open("postgres", "host=/private/tmp user=pgx_md5 password=secret dbname=pgx_test sslmode=disable")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
}

type narrow struct {
	id int32
	a  int32
	b  int32
	c  int32
	d  int32
}

func BenchmarkSQLPgxSelect10RowsByID(b *testing.B) {
	conn := getSharedConnection(b)
	createNarrowTestData(b, conn)

	// Get random ids outside of timing
	ids := make([]int32, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = 1 + rand.Int31n(9999)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records := make([]narrow, 0)
		onDataRow := func(r *pgx.DataRowReader) error {
			var rec narrow
			rec.id = r.ReadValue().(int32)
			rec.a = r.ReadValue().(int32)
			rec.b = r.ReadValue().(int32)
			rec.c = r.ReadValue().(int32)
			rec.d = r.ReadValue().(int32)
			records = append(records, rec)
			return nil
		}
		err := conn.SelectFunc("getMultipleNarrowById", onDataRow, ids[i], ids[i]+10)

		if err != nil {
			b.Fatalf("SelectFunc unexpectedly failed with %v: %v", "getMultipleNarrowById", err)
		}
	}
}

func BenchmarkSQLPqSelect10RowsByID(b *testing.B) {
	conn := getSharedConnection(b)
	createNarrowTestData(b, conn)

	// Get random ids outside of timing
	ids := make([]int32, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = 1 + rand.Int31n(9999)
	}

	stmt, err := db.Prepare("select * from narrow where id between $1 and $2")
	if err != nil {
		b.Fatalf("db.Prepare unexpectedly failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records := make([]narrow, 0)
		rows, err := stmt.Query(ids[i], ids[i]+10)
		if err != nil {
			b.Fatalf("stmt.Query unexpectedly failed: %v", err)
		}
		for rows.Next() {
			var rec narrow
			if err := rows.Scan(&rec.id, &rec.a, &rec.b, &rec.c, &rec.d); err != nil {
				b.Fatalf("rows.Scan unexpectedly failed: %v", err)
			}
			records = append(records, rec)
		}
		if err := rows.Err(); err != nil {
			b.Fatalf("rows.Err unexpectedly is: %v", err)
		}
	}
}
