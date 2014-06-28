package pgx_test

import (
	"github.com/jackc/pgx"
	"io/ioutil"
	"math/rand"
	"testing"
)

func createNarrowTestData(b *testing.B, conn *pgx.Conn) {
	mustExec(b, conn, `
		drop table if exists narrow;

		create table narrow(
			id serial primary key,
			a int not null,
			b int not null,
			c int not null,
			d int not null
		);

		insert into narrow(a, b, c, d)
		select (random()*1000000)::int, (random()*1000000)::int, (random()*1000000)::int, (random()*1000000)::int
		from generate_series(1, 10000);

		analyze narrow;
	`)

	mustPrepare(b, conn, "getNarrowById", "select * from narrow where id=$1")
	mustPrepare(b, conn, "getMultipleNarrowById", "select * from narrow where id between $1 and $2")
	mustPrepare(b, conn, "getMultipleNarrowByIdAsJSON", "select json_agg(row_to_json(narrow)) from narrow where id between $1 and $2")
}

func BenchmarkSelectValuePreparedNarrow(b *testing.B) {
	conn := mustConnect(b, *defaultConnConfig)
	defer closeConn(b, conn)
	createNarrowTestData(b, conn)

	// Get random ids outside of timing
	ids := make([]int32, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = 1 + rand.Int31n(9999)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectValue(b, conn, "getMultipleNarrowByIdAsJSON", ids[i], ids[i]+10)
	}
}

func BenchmarkSelectValueToPreparedNarrow(b *testing.B) {
	conn := mustConnect(b, *defaultConnConfig)
	defer closeConn(b, conn)
	createNarrowTestData(b, conn)

	// Get random ids outside of timing
	ids := make([]int32, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = 1 + rand.Int31n(9999)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectValueTo(b, conn, ioutil.Discard, "getMultipleNarrowByIdAsJSON", ids[i], ids[i]+10)
	}
}

func BenchmarkConnPool(b *testing.B) {
	config := pgx.ConnPoolConfig{ConnConfig: *defaultConnConfig, MaxConnections: 5}
	pool, err := pgx.NewConnPool(config)
	if err != nil {
		b.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var conn *pgx.Conn
		if conn, err = pool.Acquire(); err != nil {
			b.Fatalf("Unable to acquire connection: %v", err)
		}
		pool.Release(conn)
	}
}
