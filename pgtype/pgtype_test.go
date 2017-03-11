package pgtype_test

import (
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"testing"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

// Test for renamed types
type _string string
type _bool bool
type _int8 int8
type _int16 int16
type _int16Slice []int16
type _int32Slice []int32
type _int64Slice []int64
type _float32Slice []float32
type _float64Slice []float64
type _byteSlice []byte

func mustConnectPgx(t testing.TB) *pgx.Conn {
	config, err := pgx.ParseURI(os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.Connect(config)
	if err != nil {
		t.Fatal(err)
	}

	return conn
}

func mustClose(t testing.TB, conn interface {
	Close() error
}) {
	err := conn.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func mustParseCIDR(t testing.TB, s string) *net.IPNet {
	_, ipnet, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatal(err)
	}

	return ipnet
}

type forceTextEncoder struct {
	e pgtype.TextEncoder
}

func (f forceTextEncoder) EncodeText(w io.Writer) (bool, error) {
	return f.e.EncodeText(w)
}

type forceBinaryEncoder struct {
	e pgtype.BinaryEncoder
}

func (f forceBinaryEncoder) EncodeBinary(w io.Writer) (bool, error) {
	return f.e.EncodeBinary(w)
}

func forceEncoder(e interface{}, formatCode int16) interface{} {
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

func testSuccessfulTranscode(t testing.TB, pgTypeName string, values []interface{}) {
	testSuccessfulTranscodeEqFunc(t, pgTypeName, values, func(a, b interface{}) bool {
		return reflect.DeepEqual(a, b)
	})
}

func testSuccessfulTranscodeEqFunc(t testing.TB, pgTypeName string, values []interface{}, eqFunc func(a, b interface{}) bool) {
	conn := mustConnectPgx(t)
	defer mustClose(t, conn)

	ps, err := conn.Prepare("test", fmt.Sprintf("select $1::%s", pgTypeName))
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
		for _, fc := range formats {
			ps.FieldDescriptions[0].FormatCode = fc.formatCode
			vEncoder := forceEncoder(v, fc.formatCode)
			if vEncoder == nil {
				t.Logf("%v does not implement %v", fc.name)
				continue
			}
			// Derefence value if it is a pointer
			derefV := v
			refVal := reflect.ValueOf(v)
			if refVal.Kind() == reflect.Ptr {
				derefV = refVal.Elem().Interface()
			}

			result := reflect.New(reflect.TypeOf(derefV))
			err := conn.QueryRow("test", forceEncoder(v, fc.formatCode)).Scan(result.Interface())
			if err != nil {
				t.Errorf("%v %d: %v", fc.name, i, err)
			}

			if !eqFunc(result.Elem().Interface(), derefV) {
				t.Errorf("%v %d: expected %v, got %v", fc.name, i, derefV, result.Elem().Interface())
			}
		}
	}
}
