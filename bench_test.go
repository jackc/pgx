package pgx_test

import (
	"github.com/JackC/pgx"
	"io/ioutil"
	"math/rand"
	"testing"
)

var testJoinsDataLoaded bool
var narrowTestDataLoaded bool
var int2TextVsBinaryTestDataLoaded bool
var int4TextVsBinaryTestDataLoaded bool
var int8TextVsBinaryTestDataLoaded bool
var float4TextVsBinaryTestDataLoaded bool
var float8TextVsBinaryTestDataLoaded bool
var boolTextVsBinaryTestDataLoaded bool
var timestampTzTextVsBinaryTestDataLoaded bool

func createNarrowTestData(b *testing.B, conn *pgx.Connection) {
	if narrowTestDataLoaded {
		return
	}

	mustExecute(b, conn, `
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

	narrowTestDataLoaded = true
}

func removeBinaryEncoders() (encoders map[pgx.Oid]func(*pgx.MessageReader, int32) interface{}) {
	encoders = make(map[pgx.Oid]func(*pgx.MessageReader, int32) interface{})
	for k, v := range pgx.ValueTranscoders {
		encoders[k] = v.DecodeBinary
		pgx.ValueTranscoders[k].DecodeBinary = nil
	}
	return
}

func restoreBinaryEncoders(encoders map[pgx.Oid]func(*pgx.MessageReader, int32) interface{}) {
	for k, v := range encoders {
		pgx.ValueTranscoders[k].DecodeBinary = v
	}
}

func BenchmarkSelectRowSimpleNarrow(b *testing.B) {
	conn := getSharedConnection(b)
	createNarrowTestData(b, conn)

	// Get random ids outside of timing
	ids := make([]int32, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = 1 + rand.Int31n(9999)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mustSelectRow(b, conn, "select * from narrow where id=$1", ids[i])
	}
}

func BenchmarkSelectRowPreparedNarrow(b *testing.B) {
	conn := getSharedConnection(b)
	createNarrowTestData(b, conn)

	// Get random ids outside of timing
	ids := make([]int32, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = 1 + rand.Int31n(9999)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRow(b, conn, "getNarrowById", ids[i])
	}
}

func BenchmarkSelectRowsSimpleNarrow(b *testing.B) {
	conn := getSharedConnection(b)
	createNarrowTestData(b, conn)

	// Get random ids outside of timing
	ids := make([]int32, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = 1 + rand.Int31n(9999)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "select * from narrow where id between $1 and $2", ids[i], ids[i]+10)
	}
}

func BenchmarkSelectRowsPreparedNarrow(b *testing.B) {
	conn := getSharedConnection(b)
	createNarrowTestData(b, conn)

	// Get random ids outside of timing
	ids := make([]int32, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = 1 + rand.Int31n(9999)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "getMultipleNarrowById", ids[i], ids[i]+10)
	}
}

func BenchmarkSelectValuePreparedNarrow(b *testing.B) {
	conn := getSharedConnection(b)
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
	conn := getSharedConnection(b)
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

func createJoinsTestData(b *testing.B, conn *pgx.Connection) {
	if testJoinsDataLoaded {
		return
	}

	mustExecute(b, conn, `
		drop table if exists product_component;
		drop table if exists component;
		drop table if exists product;

		create table component(
			id serial primary key,
			filler1 varchar not null default '01234567890123456789',
			filler2 varchar not null default '01234567890123456789',
			filler3 varchar not null default '01234567890123456789',
			weight int not null,
			cost int not null
		);

		insert into component(weight, cost)
		select (random()*100)::int, (random()*1000)::int
		from generate_series(1, 1000) n;

		create index on component (weight);
		create index on component (cost);

		create table product(
			id serial primary key,
			filler1 varchar not null default '01234567890123456789',
			filler2 varchar not null default '01234567890123456789',
			filler3 varchar not null default '01234567890123456789',
			filler4 varchar not null default '01234567890123456789',
			filler5 varchar not null default '01234567890123456789'
		);

		insert into product(id)
		select n
		from generate_series(1, 10000) n;

		create table product_component(
			id serial primary key,
			product_id int not null references product,
			component_id int not null references component,
			quantity int not null
		);

		insert into product_component(product_id, component_id, quantity)
		select product.id, component.id, 1 + (random()*10)::int
		from product
		  join component on (random() * 200)::int = 1;

		create unique index on product_component(product_id, component_id);
		create index on product_component(product_id);
		create index on product_component(component_id);

		analyze;
	`)

	mustPrepare(b, conn, "joinAggregate", `
		select product.id, sum(cost*quantity) as total_cost
		from product
			join product_component on product.id=product_component.product_id
			join component on component.id=product_component.component_id
		group by product.id
		having sum(weight*quantity) > 10
		order by total_cost desc
	`)

	testJoinsDataLoaded = true
}

func BenchmarkSelectRowsSimpleJoins(b *testing.B) {
	conn := getSharedConnection(b)
	createJoinsTestData(b, conn)

	sql := `
		select product.id, sum(cost*quantity) as total_cost
		from product
			join product_component on product.id=product_component.product_id
			join component on component.id=product_component.component_id
		group by product.id
		having sum(weight*quantity) > 10
		order by total_cost desc
	`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, sql)
	}
}

func BenchmarkSelectRowsPreparedJoins(b *testing.B) {
	conn := getSharedConnection(b)
	createJoinsTestData(b, conn)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "joinAggregate")
	}
}

func createInt2TextVsBinaryTestData(b *testing.B, conn *pgx.Connection) {
	if int2TextVsBinaryTestDataLoaded {
		return
	}

	mustExecute(b, conn, `
		drop table if exists t;

		create temporary table t(
			a int2 not null,
			b int2 not null,
			c int2 not null,
			d int2 not null,
			e int2 not null
		);

		insert into t(a, b, c, d, e)
		select
			(random() * 32000)::int2, (random() * 32000)::int2, (random() * 32000)::int2, (random() * 32000)::int2, (random() * 32000)::int2
		from generate_series(1, 10);
	`)

	int2TextVsBinaryTestDataLoaded = true
}

func BenchmarkInt2Text(b *testing.B) {
	conn := getSharedConnection(b)
	createInt2TextVsBinaryTestData(b, conn)

	encoders := removeBinaryEncoders()
	defer func() { restoreBinaryEncoders(encoders) }()

	mustPrepare(b, conn, "selectInt16", "select * from t")
	defer func() { conn.Deallocate("selectInt16") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectInt16")
	}
}

func BenchmarkInt2Binary(b *testing.B) {
	conn := getSharedConnection(b)
	createInt2TextVsBinaryTestData(b, conn)
	mustPrepare(b, conn, "selectInt16", "select * from t")
	defer func() { conn.Deallocate("selectInt16") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectInt16")
	}
}

func createInt4TextVsBinaryTestData(b *testing.B, conn *pgx.Connection) {
	if int4TextVsBinaryTestDataLoaded {
		return
	}

	mustExecute(b, conn, `
		drop table if exists t;

		create temporary table t(
			a int4 not null,
			b int4 not null,
			c int4 not null,
			d int4 not null,
			e int4 not null
		);

		insert into t(a, b, c, d, e)
		select
			(random() * 1000000)::int4, (random() * 1000000)::int4, (random() * 1000000)::int4, (random() * 1000000)::int4, (random() * 1000000)::int4
		from generate_series(1, 10);
	`)

	int4TextVsBinaryTestDataLoaded = true
}

func BenchmarkInt4Text(b *testing.B) {
	conn := getSharedConnection(b)
	createInt4TextVsBinaryTestData(b, conn)

	encoders := removeBinaryEncoders()
	defer func() { restoreBinaryEncoders(encoders) }()

	mustPrepare(b, conn, "selectInt32", "select * from t")
	defer func() { conn.Deallocate("selectInt32") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectInt32")
	}
}

func BenchmarkInt4Binary(b *testing.B) {
	conn := getSharedConnection(b)
	createInt4TextVsBinaryTestData(b, conn)
	mustPrepare(b, conn, "selectInt32", "select * from t")
	defer func() { conn.Deallocate("selectInt32") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectInt32")
	}
}

func createInt8TextVsBinaryTestData(b *testing.B, conn *pgx.Connection) {
	if int8TextVsBinaryTestDataLoaded {
		return
	}

	mustExecute(b, conn, `
		drop table if exists t;

		create temporary table t(
			a int8 not null,
			b int8 not null,
			c int8 not null,
			d int8 not null,
			e int8 not null
		);

		insert into t(a, b, c, d, e)
		select
			(random() * 1000000)::int8, (random() * 1000000)::int8, (random() * 1000000)::int8, (random() * 1000000)::int8, (random() * 1000000)::int8
		from generate_series(1, 10);
	`)

	int8TextVsBinaryTestDataLoaded = true
}

func BenchmarkInt8Text(b *testing.B) {
	conn := getSharedConnection(b)
	createInt8TextVsBinaryTestData(b, conn)

	encoders := removeBinaryEncoders()
	defer func() { restoreBinaryEncoders(encoders) }()

	mustPrepare(b, conn, "selectInt64", "select * from t")
	defer func() { conn.Deallocate("selectInt64") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectInt64")
	}
}

func BenchmarkInt8Binary(b *testing.B) {
	conn := getSharedConnection(b)
	createInt8TextVsBinaryTestData(b, conn)
	mustPrepare(b, conn, "selectInt64", "select * from t")
	defer func() { conn.Deallocate("selectInt64") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectInt64")
	}
}

func createFloat4TextVsBinaryTestData(b *testing.B, conn *pgx.Connection) {
	if float4TextVsBinaryTestDataLoaded {
		return
	}

	mustExecute(b, conn, `
		drop table if exists t;

		create temporary table t(
			a float4 not null,
			b float4 not null,
			c float4 not null,
			d float4 not null,
			e float4 not null
		);

		insert into t(a, b, c, d, e)
		select
			(random() * 1000000)::float4, (random() * 1000000)::float4, (random() * 1000000)::float4, (random() * 1000000)::float4, (random() * 1000000)::float4
		from generate_series(1, 10);
	`)

	float4TextVsBinaryTestDataLoaded = true
}

func BenchmarkFloat4Text(b *testing.B) {
	conn := getSharedConnection(b)
	createFloat4TextVsBinaryTestData(b, conn)

	encoders := removeBinaryEncoders()
	defer func() { restoreBinaryEncoders(encoders) }()

	mustPrepare(b, conn, "selectFloat32", "select * from t")
	defer func() { conn.Deallocate("selectFloat32") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectFloat32")
	}
}

func BenchmarkFloat4Binary(b *testing.B) {
	conn := getSharedConnection(b)
	createFloat4TextVsBinaryTestData(b, conn)
	mustPrepare(b, conn, "selectFloat32", "select * from t")
	defer func() { conn.Deallocate("selectFloat32") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectFloat32")
	}
}

func createFloat8TextVsBinaryTestData(b *testing.B, conn *pgx.Connection) {
	if float8TextVsBinaryTestDataLoaded {
		return
	}

	mustExecute(b, conn, `
		drop table if exists t;

		create temporary table t(
			a float8 not null,
			b float8 not null,
			c float8 not null,
			d float8 not null,
			e float8 not null
		);

		insert into t(a, b, c, d, e)
		select
			(random() * 1000000)::float8, (random() * 1000000)::float8, (random() * 1000000)::float8, (random() * 1000000)::float8, (random() * 1000000)::float8
		from generate_series(1, 10);
	`)

	float8TextVsBinaryTestDataLoaded = true
}

func BenchmarkFloat8Text(b *testing.B) {
	conn := getSharedConnection(b)
	createFloat8TextVsBinaryTestData(b, conn)

	encoders := removeBinaryEncoders()
	defer func() { restoreBinaryEncoders(encoders) }()

	mustPrepare(b, conn, "selectFloat32", "select * from t")
	defer func() { conn.Deallocate("selectFloat32") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectFloat32")
	}
}

func BenchmarkFloat8Binary(b *testing.B) {
	conn := getSharedConnection(b)
	createFloat8TextVsBinaryTestData(b, conn)
	mustPrepare(b, conn, "selectFloat32", "select * from t")
	defer func() { conn.Deallocate("selectFloat32") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectFloat32")
	}
}

func createBoolTextVsBinaryTestData(b *testing.B, conn *pgx.Connection) {
	if boolTextVsBinaryTestDataLoaded {
		return
	}

	mustExecute(b, conn, `
		drop table if exists t;

		create temporary table t(
			a bool not null,
			b bool not null,
			c bool not null,
			d bool not null,
			e bool not null
		);

		insert into t(a, b, c, d, e)
		select
			random() > 0.5, random() > 0.5, random() > 0.5, random() > 0.5, random() > 0.5
		from generate_series(1, 10);
	`)

	boolTextVsBinaryTestDataLoaded = true
}

func BenchmarkBoolText(b *testing.B) {
	conn := getSharedConnection(b)
	createBoolTextVsBinaryTestData(b, conn)

	encoders := removeBinaryEncoders()
	defer func() { restoreBinaryEncoders(encoders) }()

	mustPrepare(b, conn, "selectBool", "select * from t")
	defer func() { conn.Deallocate("selectBool") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectBool")
	}
}

func BenchmarkBoolBinary(b *testing.B) {
	conn := getSharedConnection(b)
	createBoolTextVsBinaryTestData(b, conn)
	mustPrepare(b, conn, "selectBool", "select * from t")
	defer func() { conn.Deallocate("selectBool") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectBool")
	}
}

func createTimestampTzTextVsBinaryTestData(b *testing.B, conn *pgx.Connection) {
	if timestampTzTextVsBinaryTestDataLoaded {
		return
	}

	mustExecute(b, conn, `
		drop table if exists t;

		create temporary table t(
			a timestamptz not null,
			b timestamptz not null,
			c timestamptz not null,
			d timestamptz not null,
			e timestamptz not null
		);

		insert into t(a, b, c, d, e)
		select
			now() - '10 years'::interval * random(),
			now() - '10 years'::interval * random(),
			now() - '10 years'::interval * random(),
		  now() - '10 years'::interval * random(),
		  now() - '10 years'::interval * random()
		from generate_series(1, 10);
	`)

	timestampTzTextVsBinaryTestDataLoaded = true
}

func BenchmarkTimestampTzText(b *testing.B) {
	conn := getSharedConnection(b)
	createTimestampTzTextVsBinaryTestData(b, conn)

	encoders := removeBinaryEncoders()
	defer func() { restoreBinaryEncoders(encoders) }()

	mustPrepare(b, conn, "selectTimestampTz", "select * from t")
	defer func() { conn.Deallocate("selectTimestampTz") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectTimestampTz")
	}
}

func BenchmarkTimestampTzBinary(b *testing.B) {
	conn := getSharedConnection(b)
	createTimestampTzTextVsBinaryTestData(b, conn)
	mustPrepare(b, conn, "selectTimestampTz", "select * from t")
	defer func() { conn.Deallocate("selectTimestampTz") }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustSelectRows(b, conn, "selectTimestampTz")
	}
}

func BenchmarkConnectionPool(b *testing.B) {
	options := pgx.ConnectionPoolOptions{MaxConnections: 5}
	pool, err := pgx.NewConnectionPool(*defaultConnectionParameters, options)
	if err != nil {
		b.Fatalf("Unable to create connection pool: %v", err)
	}
	defer pool.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var conn *pgx.Connection
		if conn, err = pool.Acquire(); err != nil {
			b.Fatalf("Unable to acquire connection: %v", err)
		}
		pool.Release(conn)
	}

}
