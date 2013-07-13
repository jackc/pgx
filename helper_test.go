package pgx

type test interface {
	Fatalf(format string, args ...interface{})
}

func mustPrepare(t test, conn *Connection, name, sql string) {
	if err := conn.Prepare(name, sql); err != nil {
		t.Fatalf("Could not prepare %v: %v", name, err)
	}
}
