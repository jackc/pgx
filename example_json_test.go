package pgx_test

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4"
)

func Example_JSON() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	type person struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	input := person{
		Name: "John",
		Age:  42,
	}

	var output person

	err = conn.QueryRow(context.Background(), "select $1::json", input).Scan(&output)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(output.Name, output.Age)
	// Output:
	// John 42
}
