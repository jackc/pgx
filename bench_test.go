package pgx

import (
	"fmt"
	"math/rand"
	"testing"
)

var testDataLoaded bool

func loadTestData() {
	if testDataLoaded {
		return
	}

	var err error

	conn := getSharedConnection()

	_, err = conn.Execute(`
		drop table if exists narrow;
		drop table if exists product_component;
		drop table if exists component;
		drop table if exists product;

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
	if err != nil {
		panic(fmt.Sprintf("Unable to create test data: %v", err))
	}

	err = conn.Prepare("getNarrowById", "select * from narrow where id=$1")
	if err != nil {
		panic("Unable to prepare getNarrowById")
	}

	err = conn.Prepare("getMultipleNarrowById", "select * from narrow where id between $1 and $2")
	if err != nil {
		panic("Unable to prepare getMultipleNarrowById")
	}

	err = conn.Prepare("joinAggregate", `
		select product.id, sum(cost*quantity) as total_cost
		from product
			join product_component on product.id=product_component.product_id
			join component on component.id=product_component.component_id
		group by product.id
		having sum(weight*quantity) > 10
		order by total_cost desc
	`)
	if err != nil {
		panic("Unable to prepare joinAggregate")
	}

	testDataLoaded = true
}

func BenchmarkSelectRowSimpleNarrow(b *testing.B) {
	loadTestData()
	conn := getSharedConnection()

	// Get random ids outside of timing
	ids := make([]int32, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = 1 + rand.Int31n(9999)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.SelectRow("select * from narrow where id=$1", ids[i]); err != nil {
			b.Fatalf("Failure while benchmarking: %v", err)
		}
	}
}

func BenchmarkSelectRowPreparedNarrow(b *testing.B) {
	loadTestData()
	conn := getSharedConnection()

	// Get random ids outside of timing
	ids := make([]int32, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = 1 + rand.Int31n(9999)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.SelectRow("getNarrowById", ids[i]); err != nil {
			b.Fatalf("Failure while benchmarking: %v", err)
		}
	}
}

func BenchmarkSelectRowsSimpleNarrow(b *testing.B) {
	loadTestData()
	conn := getSharedConnection()

	// Get random ids outside of timing
	ids := make([]int32, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = 1 + rand.Int31n(9999)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.SelectRows("select * from narrow where id between $1 and $2", ids[i], ids[i]+10); err != nil {
			b.Fatalf("Failure while benchmarking: %v", err)
		}
	}
}

func BenchmarkSelectRowsPreparedNarrow(b *testing.B) {
	loadTestData()
	conn := getSharedConnection()

	// Get random ids outside of timing
	ids := make([]int32, b.N)
	for i := 0; i < b.N; i++ {
		ids[i] = 1 + rand.Int31n(9999)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.SelectRows("getMultipleNarrowById", ids[i], ids[i]+10); err != nil {
			b.Fatalf("Failure while benchmarking: %v", err)
		}
	}
}

func BenchmarkSelectRowsSimpleJoins(b *testing.B) {
	loadTestData()
	conn := getSharedConnection()

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
		if _, err := conn.SelectRows(sql); err != nil {
			b.Fatalf("Failure while benchmarking: %v", err)
		}
	}
}

func BenchmarkSelectRowsPreparedJoins(b *testing.B) {
	loadTestData()
	conn := getSharedConnection()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.SelectRows("joinAggregate"); err != nil {
			b.Fatalf("Failure while benchmarking: %v", err)
		}
	}
}
