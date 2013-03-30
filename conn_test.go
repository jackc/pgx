package pqx

import (
	"testing"
)

func TestConnect(t *testing.T) {
	_, err := Connect(map[string] string { "socket": "/private/tmp/.s.PGSQL.5432" })
	if err != nil {
		t.Fatal("Unable to establish connection")
	}
}
