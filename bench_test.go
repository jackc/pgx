package pgx_test

import (
	"github.com/jackc/pgx"
	"testing"
	"time"
)

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

func BenchmarkNullXWithNullValues(b *testing.B) {
	conn := mustConnect(b, *defaultConnConfig)
	defer closeConn(b, conn)

	_, err := conn.Prepare("selectNulls", "select 1::int4, 'johnsmith', null::text, null::text, null::text, null::date, null::timestamptz")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var record struct {
			id            int32
			userName      string
			email         pgx.NullString
			name          pgx.NullString
			sex           pgx.NullString
			birthDate     pgx.NullTime
			lastLoginTime pgx.NullTime
		}

		err = conn.QueryRow("selectNulls").Scan(
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
		if record.email.Valid {
			b.Fatalf("bad value for email: %v", record.email)
		}
		if record.name.Valid {
			b.Fatalf("bad value for name: %v", record.name)
		}
		if record.sex.Valid {
			b.Fatalf("bad value for sex: %v", record.sex)
		}
		if record.birthDate.Valid {
			b.Fatalf("bad value for birthDate: %v", record.birthDate)
		}
		if record.lastLoginTime.Valid {
			b.Fatalf("bad value for lastLoginTime: %v", record.lastLoginTime)
		}
	}
}

func BenchmarkNullXWithPresentValues(b *testing.B) {
	conn := mustConnect(b, *defaultConnConfig)
	defer closeConn(b, conn)

	_, err := conn.Prepare("selectNulls", "select 1::int4, 'johnsmith', 'johnsmith@example.com', 'John Smith', 'male', '1970-01-01'::date, '2015-01-01 00:00:00'::timestamptz")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var record struct {
			id            int32
			userName      string
			email         pgx.NullString
			name          pgx.NullString
			sex           pgx.NullString
			birthDate     pgx.NullTime
			lastLoginTime pgx.NullTime
		}

		err = conn.QueryRow("selectNulls").Scan(
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
		if !record.email.Valid || record.email.String != "johnsmith@example.com" {
			b.Fatalf("bad value for email: %v", record.email)
		}
		if !record.name.Valid || record.name.String != "John Smith" {
			b.Fatalf("bad value for name: %v", record.name)
		}
		if !record.sex.Valid || record.sex.String != "male" {
			b.Fatalf("bad value for sex: %v", record.sex)
		}
		if !record.birthDate.Valid || record.birthDate.Time != time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local) {
			b.Fatalf("bad value for birthDate: %v", record.birthDate)
		}
		if !record.lastLoginTime.Valid || record.lastLoginTime.Time != time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local) {
			b.Fatalf("bad value for lastLoginTime: %v", record.lastLoginTime)
		}
	}
}

func BenchmarkPointerPointerWithNullValues(b *testing.B) {
	conn := mustConnect(b, *defaultConnConfig)
	defer closeConn(b, conn)

	_, err := conn.Prepare("selectNulls", "select 1::int4, 'johnsmith', null::text, null::text, null::text, null::date, null::timestamptz")
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

		err = conn.QueryRow("selectNulls").Scan(
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
	conn := mustConnect(b, *defaultConnConfig)
	defer closeConn(b, conn)

	_, err := conn.Prepare("selectNulls", "select 1::int4, 'johnsmith', 'johnsmith@example.com', 'John Smith', 'male', '1970-01-01'::date, '2015-01-01 00:00:00'::timestamptz")
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

		err = conn.QueryRow("selectNulls").Scan(
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
		if record.birthDate == nil || *record.birthDate != time.Date(1970, 1, 1, 0, 0, 0, 0, time.Local) {
			b.Fatalf("bad value for birthDate: %v", record.birthDate)
		}
		if record.lastLoginTime == nil || *record.lastLoginTime != time.Date(2015, 1, 1, 0, 0, 0, 0, time.Local) {
			b.Fatalf("bad value for lastLoginTime: %v", record.lastLoginTime)
		}
	}
}
