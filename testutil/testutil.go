package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
)

func MustConnectDatabaseSQL(t testing.TB, driverName string) *sql.DB {
	var sqlDriverName string
	switch driverName {
	case "github.com/lib/pq":
		sqlDriverName = "postgres"
	case "github.com/jackc/pgx/stdlib":
		sqlDriverName = "pgx"
	default:
		t.Fatalf("Unknown driver %v", driverName)
	}

	db, err := sql.Open(sqlDriverName, os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}

	return db
}

func MustConnectPgx(t testing.TB) *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	if err != nil {
		t.Fatal(err)
	}

	return conn
}

func MustClose(t testing.TB, conn interface {
	Close() error
}) {
	err := conn.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func MustCloseContext(t testing.TB, conn interface {
	Close(context.Context) error
}) {
	err := conn.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
}

type forceTextEncoder struct {
	e pgtype.TextEncoder
}

func (f forceTextEncoder) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	return f.e.EncodeText(ci, buf)
}

type forceBinaryEncoder struct {
	e pgtype.BinaryEncoder
}

func (f forceBinaryEncoder) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	return f.e.EncodeBinary(ci, buf)
}

func ForceEncoder(e interface{}, formatCode int16) interface{} {
	switch formatCode {
	case pgx.TextFormatCode:
		if e, ok := e.(pgtype.TextEncoder); ok {
			return forceTextEncoder{e: e}
		}
	case pgx.BinaryFormatCode:
		if e, ok := e.(pgtype.BinaryEncoder); ok {
			return forceBinaryEncoder{e: e.(pgtype.BinaryEncoder)}
		}
	}
	return nil
}

func TestSuccessfulTranscode(t testing.TB, pgTypeName string, values []interface{}) {
	TestSuccessfulTranscodeEqFunc(t, pgTypeName, values, func(a, b interface{}) bool {
		return reflect.DeepEqual(a, b)
	})
}

func TestSuccessfulTranscodeEqFunc(t testing.TB, pgTypeName string, values []interface{}, eqFunc func(a, b interface{}) bool) {
	TestPgxSuccessfulTranscodeEqFunc(t, pgTypeName, values, eqFunc)
	for _, driverName := range []string{"github.com/lib/pq", "github.com/jackc/pgx/stdlib"} {
		TestDatabaseSQLSuccessfulTranscodeEqFunc(t, driverName, pgTypeName, values, eqFunc)
	}
}

func TestPgxSuccessfulTranscodeEqFunc(t testing.TB, pgTypeName string, values []interface{}, eqFunc func(a, b interface{}) bool) {
	conn := MustConnectPgx(t)
	defer MustCloseContext(t, conn)

	_, err := conn.Prepare(context.Background(), "test", fmt.Sprintf("select $1::%s", pgTypeName))
	if err != nil {
		t.Fatal(err)
	}

	formats := []struct {
		name       string
		formatCode int16
	}{
		{name: "TextFormat", formatCode: pgx.TextFormatCode},
		{name: "BinaryFormat", formatCode: pgx.BinaryFormatCode},
	}

	for i, v := range values {
		for _, paramFormat := range formats {
			for _, resultFormat := range formats {
				vEncoder := ForceEncoder(v, paramFormat.formatCode)
				if vEncoder == nil {
					t.Logf("Skipping Param %s Result %s: %#v does not implement %v for encoding", paramFormat.name, resultFormat.name, v, paramFormat.name)
					continue
				}
				switch resultFormat.formatCode {
				case pgx.TextFormatCode:
					if _, ok := v.(pgtype.TextEncoder); !ok {
						t.Logf("Skipping Param %s Result %s: %#v does not implement %v for decoding", paramFormat.name, resultFormat.name, v, resultFormat.name)
						continue
					}
				case pgx.BinaryFormatCode:
					if _, ok := v.(pgtype.BinaryEncoder); !ok {
						t.Logf("Skipping Param %s Result %s: %#v does not implement %v for decoding", paramFormat.name, resultFormat.name, v, resultFormat.name)
						continue
					}
				}

				// Derefence value if it is a pointer
				derefV := v
				refVal := reflect.ValueOf(v)
				if refVal.Kind() == reflect.Ptr {
					derefV = refVal.Elem().Interface()
				}

				result := reflect.New(reflect.TypeOf(derefV))

				err := conn.QueryRow(context.Background(), "test", pgx.QueryResultFormats{resultFormat.formatCode}, vEncoder).Scan(result.Interface())
				if err != nil {
					t.Errorf("Param %s Result %s %d: %v", paramFormat.name, resultFormat.name, i, err)
				}

				if !eqFunc(result.Elem().Interface(), derefV) {
					t.Errorf("Param %s Result %s %d: expected %v, got %v", paramFormat.name, resultFormat.name, i, derefV, result.Elem().Interface())
				}
			}
		}
	}
}

func TestDatabaseSQLSuccessfulTranscodeEqFunc(t testing.TB, driverName, pgTypeName string, values []interface{}, eqFunc func(a, b interface{}) bool) {
	conn := MustConnectDatabaseSQL(t, driverName)
	defer MustClose(t, conn)

	ps, err := conn.Prepare(fmt.Sprintf("select $1::%s", pgTypeName))
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range values {
		// Derefence value if it is a pointer
		derefV := v
		refVal := reflect.ValueOf(v)
		if refVal.Kind() == reflect.Ptr {
			derefV = refVal.Elem().Interface()
		}

		result := reflect.New(reflect.TypeOf(derefV))
		err := ps.QueryRow(v).Scan(result.Interface())
		if err != nil {
			t.Errorf("%v %d: %v", driverName, i, err)
		}

		if !eqFunc(result.Elem().Interface(), derefV) {
			t.Errorf("%v %d: expected %v, got %v", driverName, i, derefV, result.Elem().Interface())
		}
	}
}

type NormalizeTest struct {
	SQL   string
	Value interface{}
}

func TestSuccessfulNormalize(t testing.TB, tests []NormalizeTest) {
	TestSuccessfulNormalizeEqFunc(t, tests, func(a, b interface{}) bool {
		return reflect.DeepEqual(a, b)
	})
}

func TestSuccessfulNormalizeEqFunc(t testing.TB, tests []NormalizeTest, eqFunc func(a, b interface{}) bool) {
	TestPgxSuccessfulNormalizeEqFunc(t, tests, eqFunc)
	for _, driverName := range []string{"github.com/lib/pq", "github.com/jackc/pgx/stdlib"} {
		TestDatabaseSQLSuccessfulNormalizeEqFunc(t, driverName, tests, eqFunc)
	}
}

func TestPgxSuccessfulNormalizeEqFunc(t testing.TB, tests []NormalizeTest, eqFunc func(a, b interface{}) bool) {
	conn := MustConnectPgx(t)
	defer MustCloseContext(t, conn)

	formats := []struct {
		name       string
		formatCode int16
	}{
		{name: "TextFormat", formatCode: pgx.TextFormatCode},
		{name: "BinaryFormat", formatCode: pgx.BinaryFormatCode},
	}

	for i, tt := range tests {
		for _, fc := range formats {
			psName := fmt.Sprintf("test%d", i)
			_, err := conn.Prepare(context.Background(), psName, tt.SQL)
			if err != nil {
				t.Fatal(err)
			}

			queryResultFormats := pgx.QueryResultFormats{fc.formatCode}
			if ForceEncoder(tt.Value, fc.formatCode) == nil {
				t.Logf("Skipping: %#v does not implement %v", tt.Value, fc.name)
				continue
			}
			// Derefence value if it is a pointer
			derefV := tt.Value
			refVal := reflect.ValueOf(tt.Value)
			if refVal.Kind() == reflect.Ptr {
				derefV = refVal.Elem().Interface()
			}

			result := reflect.New(reflect.TypeOf(derefV))
			err = conn.QueryRow(context.Background(), psName, queryResultFormats).Scan(result.Interface())
			if err != nil {
				t.Errorf("%v %d: %v", fc.name, i, err)
			}

			if !eqFunc(result.Elem().Interface(), derefV) {
				t.Errorf("%v %d: expected %v, got %v", fc.name, i, derefV, result.Elem().Interface())
			}
		}
	}
}

func TestDatabaseSQLSuccessfulNormalizeEqFunc(t testing.TB, driverName string, tests []NormalizeTest, eqFunc func(a, b interface{}) bool) {
	conn := MustConnectDatabaseSQL(t, driverName)
	defer MustClose(t, conn)

	for i, tt := range tests {
		ps, err := conn.Prepare(tt.SQL)
		if err != nil {
			t.Errorf("%d. %v", i, err)
			continue
		}

		// Derefence value if it is a pointer
		derefV := tt.Value
		refVal := reflect.ValueOf(tt.Value)
		if refVal.Kind() == reflect.Ptr {
			derefV = refVal.Elem().Interface()
		}

		result := reflect.New(reflect.TypeOf(derefV))
		err = ps.QueryRow().Scan(result.Interface())
		if err != nil {
			t.Errorf("%v %d: %v", driverName, i, err)
		}

		if !eqFunc(result.Elem().Interface(), derefV) {
			t.Errorf("%v %d: expected %v, got %v", driverName, i, derefV, result.Elem().Interface())
		}
	}
}

func TestGoZeroToNullConversion(t testing.TB, pgTypeName string, zero interface{}) {
	TestPgxGoZeroToNullConversion(t, pgTypeName, zero)
	for _, driverName := range []string{"github.com/lib/pq", "github.com/jackc/pgx/stdlib"} {
		TestDatabaseSQLGoZeroToNullConversion(t, driverName, pgTypeName, zero)
	}
}

func TestNullToGoZeroConversion(t testing.TB, pgTypeName string, zero interface{}) {
	TestPgxNullToGoZeroConversion(t, pgTypeName, zero)
	for _, driverName := range []string{"github.com/lib/pq", "github.com/jackc/pgx/stdlib"} {
		TestDatabaseSQLNullToGoZeroConversion(t, driverName, pgTypeName, zero)
	}
}

func TestPgxGoZeroToNullConversion(t testing.TB, pgTypeName string, zero interface{}) {
	conn := MustConnectPgx(t)
	defer MustCloseContext(t, conn)

	_, err := conn.Prepare(context.Background(), "test", fmt.Sprintf("select $1::%s is null", pgTypeName))
	if err != nil {
		t.Fatal(err)
	}

	formats := []struct {
		name       string
		formatCode int16
	}{
		{name: "TextFormat", formatCode: pgx.TextFormatCode},
		{name: "BinaryFormat", formatCode: pgx.BinaryFormatCode},
	}

	for _, paramFormat := range formats {
		vEncoder := ForceEncoder(zero, paramFormat.formatCode)
		if vEncoder == nil {
			t.Logf("Skipping Param %s: %#v does not implement %v for encoding", paramFormat.name, zero, paramFormat.name)
			continue
		}

		var result bool
		err := conn.QueryRow(context.Background(), "test", vEncoder).Scan(&result)
		if err != nil {
			t.Errorf("Param %s: %v", paramFormat.name, err)
		}

		if !result {
			t.Errorf("Param %s: did not convert zero to null", paramFormat.name)
		}
	}
}

func TestPgxNullToGoZeroConversion(t testing.TB, pgTypeName string, zero interface{}) {
	conn := MustConnectPgx(t)
	defer MustCloseContext(t, conn)

	_, err := conn.Prepare(context.Background(), "test", fmt.Sprintf("select null::%s", pgTypeName))
	if err != nil {
		t.Fatal(err)
	}

	formats := []struct {
		name       string
		formatCode int16
	}{
		{name: "TextFormat", formatCode: pgx.TextFormatCode},
		{name: "BinaryFormat", formatCode: pgx.BinaryFormatCode},
	}

	for _, resultFormat := range formats {

		switch resultFormat.formatCode {
		case pgx.TextFormatCode:
			if _, ok := zero.(pgtype.TextEncoder); !ok {
				t.Logf("Skipping Result %s: %#v does not implement %v for decoding", resultFormat.name, zero, resultFormat.name)
				continue
			}
		case pgx.BinaryFormatCode:
			if _, ok := zero.(pgtype.BinaryEncoder); !ok {
				t.Logf("Skipping Result %s: %#v does not implement %v for decoding", resultFormat.name, zero, resultFormat.name)
				continue
			}
		}

		// Derefence value if it is a pointer
		derefZero := zero
		refVal := reflect.ValueOf(zero)
		if refVal.Kind() == reflect.Ptr {
			derefZero = refVal.Elem().Interface()
		}

		result := reflect.New(reflect.TypeOf(derefZero))

		err := conn.QueryRow(context.Background(), "test").Scan(result.Interface())
		if err != nil {
			t.Errorf("Result %s: %v", resultFormat.name, err)
		}

		if !reflect.DeepEqual(result.Elem().Interface(), derefZero) {
			t.Errorf("Result %s: did not convert null to zero", resultFormat.name)
		}
	}
}

func TestDatabaseSQLGoZeroToNullConversion(t testing.TB, driverName, pgTypeName string, zero interface{}) {
	conn := MustConnectDatabaseSQL(t, driverName)
	defer MustClose(t, conn)

	ps, err := conn.Prepare(fmt.Sprintf("select $1::%s is null", pgTypeName))
	if err != nil {
		t.Fatal(err)
	}

	var result bool
	err = ps.QueryRow(zero).Scan(&result)
	if err != nil {
		t.Errorf("%v %v", driverName, err)
	}

	if !result {
		t.Errorf("%v: did not convert zero to null", driverName)
	}
}

func TestDatabaseSQLNullToGoZeroConversion(t testing.TB, driverName, pgTypeName string, zero interface{}) {
	conn := MustConnectDatabaseSQL(t, driverName)
	defer MustClose(t, conn)

	ps, err := conn.Prepare(fmt.Sprintf("select null::%s", pgTypeName))
	if err != nil {
		t.Fatal(err)
	}

	// Derefence value if it is a pointer
	derefZero := zero
	refVal := reflect.ValueOf(zero)
	if refVal.Kind() == reflect.Ptr {
		derefZero = refVal.Elem().Interface()
	}

	result := reflect.New(reflect.TypeOf(derefZero))

	err = ps.QueryRow().Scan(result.Interface())
	if err != nil {
		t.Errorf("%v %v", driverName, err)
	}

	if !reflect.DeepEqual(result.Elem().Interface(), derefZero) {
		t.Errorf("%s: did not convert null to zero", driverName)
	}
}
