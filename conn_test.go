package pqx

import (
	"testing"
)

func TestConnect(t *testing.T) {
	conn, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432"})
	if err != nil {
		t.Fatal("Unable to establish connection")
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}


func TestQuery(t *testing.T) {
	conn, err := Connect(map[string]string{"socket": "/private/tmp/.s.PGSQL.5432"})
	if err != nil {
		t.Fatal("Unable to establish connection")
	}

	// var rows []map[string]string
	_, err = conn.Query("SELECT * FROM people")
	if err != nil {
		t.Fatal("Query failed")
	}

	err = conn.Close()
	if err != nil {
		t.Fatal("Unable to close connection")
	}
}