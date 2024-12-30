package main

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	pool, err := pgxpool.New(context.Background(), "postgres://gamerhound:gamerhound@localhost:5432/gamerhound")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	// Create the enum type.
	_, err = pool.Exec(context.Background(), `DROP TYPE IF EXISTS test_enum_type`)
	if err != nil {
		log.Print(err)
		return
	}
	_, err = pool.Exec(context.Background(), `CREATE TYPE test_enum_type AS ENUM ('a', 'b')`)
	if err != nil {
		log.Print(err)
		return
	}

	err = testQuery(pool, "SELECT 'a'", "a")
	if err != nil {
		log.Printf("test TEXT error: %s\n", err)
	}

	err = testQuery(pool, "SELECT 'a'::test_enum_type", "a")
	if err != nil {
		log.Printf("test ENUM error: %s\n", err)
	}

	err = testQuery(pool, "SELECT '{}'::jsonb", "{}")
	if err != nil {
		log.Printf("test JSONB error: %s\n", err)
	}
}

// T implements the sql.Scanner interface.
type T struct {
	v *any
}

func (t T) Scan(v any) error {
	*t.v = v
	return nil
}

// testQuery executes the query and checks if the scanned value matches
// the expected result.
func testQuery(pool *pgxpool.Pool, query string, expected any) error {
	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return err
	}
	// defer rows.Close()

	var got any
	t := T{v: &got}
	for rows.Next() {
		if err := rows.Scan(t); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if !reflect.DeepEqual(got, expected) {
		return fmt.Errorf("expected %#v; got %#v", expected, got)
	}
	return nil
}
