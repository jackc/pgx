package pgx

type test interface {
	Fatalf(format string, args ...interface{})
}

func mustPrepare(t test, conn *Connection, name, sql string) {
	if err := conn.Prepare(name, sql); err != nil {
		t.Fatalf("Could not prepare %v: %v", name, err)
	}
}

func mustExecute(t test, conn *Connection, sql string, arguments ...interface{}) (commandTag string) {
	var err error
	if commandTag, err = conn.Execute(sql, arguments...); err != nil {
		t.Fatalf("Execute unexpectedly failed with %v: %v", sql, err)
	}
	return
}

func mustSelectRow(t test, conn *Connection, sql string, arguments ...interface{}) (row map[string]interface{}) {
	var err error
	if row, err = conn.SelectRow(sql, arguments...); err != nil {
		t.Fatalf("SelectRow unexpectedly failed with %v: %v", sql, err)
	}
	return
}

func mustSelectRows(t test, conn *Connection, sql string, arguments ...interface{}) (rows []map[string]interface{}) {
	var err error
	if rows, err = conn.SelectRows(sql, arguments...); err != nil {
		t.Fatalf("SelectRows unexpected failed with %v: %v", sql, err)
	}
	return
}

func mustSelectValue(t test, conn *Connection, sql string, arguments ...interface{}) (value interface{}) {
	var err error
	if value, err = conn.SelectValue(sql, arguments...); err != nil {
		t.Fatalf("SelectValue unexpectedly failed with %v: %v", sql, err)
	}
	return
}
