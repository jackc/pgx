package pgtype_test

import (
	"math/big"
	"testing"

	"github.com/jackc/pgx/pgtype"
)

func mustParseBigInt(t *testing.T, src string) *big.Int {
	i := &big.Int{}
	if _, ok := i.SetString(src, 10); !ok {
		t.Fatalf("could not parse big.Int: %s", src)
	}
	return i
}

func TestNumericNormalize(t *testing.T) {
	testSuccessfulNormalize(t, []normalizeTest{
		{
			sql:   "select '0'::numeric",
			value: pgtype.Numeric{Int: big.NewInt(0), Exp: 0, Status: pgtype.Present},
		},
		{
			sql:   "select '1'::numeric",
			value: pgtype.Numeric{Int: big.NewInt(1), Exp: 0, Status: pgtype.Present},
		},
		{
			sql:   "select '10.00'::numeric",
			value: pgtype.Numeric{Int: big.NewInt(1000), Exp: -2, Status: pgtype.Present},
		},
		{
			sql:   "select '1e-3'::numeric",
			value: pgtype.Numeric{Int: big.NewInt(1), Exp: -3, Status: pgtype.Present},
		},
		{
			sql:   "select '-1'::numeric",
			value: pgtype.Numeric{Int: big.NewInt(-1), Exp: 0, Status: pgtype.Present},
		},
		{
			sql:   "select '10000'::numeric",
			value: pgtype.Numeric{Int: big.NewInt(1), Exp: 4, Status: pgtype.Present},
		},
		{
			sql:   "select '3.14'::numeric",
			value: pgtype.Numeric{Int: big.NewInt(314), Exp: -2, Status: pgtype.Present},
		},
		{
			sql:   "select '1.1'::numeric",
			value: pgtype.Numeric{Int: big.NewInt(11), Exp: -1, Status: pgtype.Present},
		},
		{
			sql:   "select '100010001'::numeric",
			value: pgtype.Numeric{Int: big.NewInt(100010001), Exp: 0, Status: pgtype.Present},
		},
		{
			sql:   "select '100010001.0001'::numeric",
			value: pgtype.Numeric{Int: big.NewInt(1000100010001), Exp: -4, Status: pgtype.Present},
		},
		{
			sql: "select '4237234789234789289347892374324872138321894178943189043890124832108934.43219085471578891547854892438945012347981'::numeric",
			value: pgtype.Numeric{
				Int:    mustParseBigInt(t, "423723478923478928934789237432487213832189417894318904389012483210893443219085471578891547854892438945012347981"),
				Exp:    -41,
				Status: pgtype.Present,
			},
		},
		{
			sql: "select '0.8925092023480223478923478978978937897879595901237890234789243679037419057877231734823098432903527585734549035904590854890345905434578345789347890402348952348905890489054234237489234987723894789234'::numeric",
			value: pgtype.Numeric{
				Int:    mustParseBigInt(t, "8925092023480223478923478978978937897879595901237890234789243679037419057877231734823098432903527585734549035904590854890345905434578345789347890402348952348905890489054234237489234987723894789234"),
				Exp:    -196,
				Status: pgtype.Present,
			},
		},
		{
			sql: "select '0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000123'::numeric",
			value: pgtype.Numeric{
				Int:    mustParseBigInt(t, "123"),
				Exp:    -186,
				Status: pgtype.Present,
			},
		},
	})
}

func TestNumericTranscode(t *testing.T) {
	testSuccessfulTranscodeEqFunc(t, "numeric", []interface{}{
		pgtype.Numeric{Int: big.NewInt(0), Exp: 0, Status: pgtype.Present},
		pgtype.Numeric{Int: big.NewInt(1), Exp: 0, Status: pgtype.Present},
		pgtype.Numeric{Int: big.NewInt(-1), Exp: 0, Status: pgtype.Present},
		pgtype.Numeric{Int: big.NewInt(1), Exp: 6, Status: pgtype.Present},

		// preserves significant zeroes
		pgtype.Numeric{Int: big.NewInt(10000000), Exp: -1, Status: pgtype.Present},
		pgtype.Numeric{Int: big.NewInt(10000000), Exp: -2, Status: pgtype.Present},
		pgtype.Numeric{Int: big.NewInt(10000000), Exp: -3, Status: pgtype.Present},
		pgtype.Numeric{Int: big.NewInt(10000000), Exp: -4, Status: pgtype.Present},
		pgtype.Numeric{Int: big.NewInt(10000000), Exp: -5, Status: pgtype.Present},
		pgtype.Numeric{Int: big.NewInt(10000000), Exp: -6, Status: pgtype.Present},

		pgtype.Numeric{Int: big.NewInt(314), Exp: -2, Status: pgtype.Present},
		pgtype.Numeric{Int: big.NewInt(123), Exp: -7, Status: pgtype.Present},
		pgtype.Numeric{Int: big.NewInt(123), Exp: -8, Status: pgtype.Present},
		pgtype.Numeric{Int: big.NewInt(123), Exp: -9, Status: pgtype.Present},
		pgtype.Numeric{Int: big.NewInt(123), Exp: -1500, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "2437"), Exp: 23790, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "243723409723490243842378942378901237502734019231380123"), Exp: 23790, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 80, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "3723409723490243842378942378901237502734019231380123"), Exp: 81, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "723409723490243842378942378901237502734019231380123"), Exp: 82, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "23409723490243842378942378901237502734019231380123"), Exp: 83, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "3409723490243842378942378901237502734019231380123"), Exp: 84, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "913423409823409243892349028349023482934092340892390101"), Exp: -14021, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -90, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "3423409823409243892349028349023482934092340892390101"), Exp: -91, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "423409823409243892349028349023482934092340892390101"), Exp: -92, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "23409823409243892349028349023482934092340892390101"), Exp: -93, Status: pgtype.Present},
		pgtype.Numeric{Int: mustParseBigInt(t, "3409823409243892349028349023482934092340892390101"), Exp: -94, Status: pgtype.Present},
		pgtype.Numeric{Status: pgtype.Null},
	}, func(aa, bb interface{}) bool {
		a := aa.(pgtype.Numeric)
		b := bb.(pgtype.Numeric)

		return a.Status == a.Status &&
			a.Exp == b.Exp &&
			((a.Int == nil && b.Int == nil) || (a.Int != nil && b.Int != nil && a.Int.Cmp(b.Int) == 0))
	})
}

// func TestNumericSet(t *testing.T) {
// 	successfulTests := []struct {
// 		source interface{}
// 		result pgtype.Numeric
// 	}{
// 		{source: float32(1), result: pgtype.Numeric{Float: 1, Status: pgtype.Present}},
// 		{source: float64(1), result: pgtype.Numeric{Float: 1, Status: pgtype.Present}},
// 		{source: int8(1), result: pgtype.Numeric{Float: 1, Status: pgtype.Present}},
// 		{source: int16(1), result: pgtype.Numeric{Float: 1, Status: pgtype.Present}},
// 		{source: int32(1), result: pgtype.Numeric{Float: 1, Status: pgtype.Present}},
// 		{source: int64(1), result: pgtype.Numeric{Float: 1, Status: pgtype.Present}},
// 		{source: int8(-1), result: pgtype.Numeric{Float: -1, Status: pgtype.Present}},
// 		{source: int16(-1), result: pgtype.Numeric{Float: -1, Status: pgtype.Present}},
// 		{source: int32(-1), result: pgtype.Numeric{Float: -1, Status: pgtype.Present}},
// 		{source: int64(-1), result: pgtype.Numeric{Float: -1, Status: pgtype.Present}},
// 		{source: uint8(1), result: pgtype.Numeric{Float: 1, Status: pgtype.Present}},
// 		{source: uint16(1), result: pgtype.Numeric{Float: 1, Status: pgtype.Present}},
// 		{source: uint32(1), result: pgtype.Numeric{Float: 1, Status: pgtype.Present}},
// 		{source: uint64(1), result: pgtype.Numeric{Float: 1, Status: pgtype.Present}},
// 		{source: "1", result: pgtype.Numeric{Float: 1, Status: pgtype.Present}},
// 		{source: _int8(1), result: pgtype.Numeric{Float: 1, Status: pgtype.Present}},
// 	}

// 	for i, tt := range successfulTests {
// 		var r pgtype.Numeric
// 		err := r.Set(tt.source)
// 		if err != nil {
// 			t.Errorf("%d: %v", i, err)
// 		}

// 		if r != tt.result {
// 			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
// 		}
// 	}
// }

// func TestNumericAssignTo(t *testing.T) {
// 	var i8 int8
// 	var i16 int16
// 	var i32 int32
// 	var i64 int64
// 	var i int
// 	var ui8 uint8
// 	var ui16 uint16
// 	var ui32 uint32
// 	var ui64 uint64
// 	var ui uint
// 	var pi8 *int8
// 	var _i8 _int8
// 	var _pi8 *_int8
// 	var f32 float32
// 	var f64 float64
// 	var pf32 *float32
// 	var pf64 *float64

// 	simpleTests := []struct {
// 		src      pgtype.Numeric
// 		dst      interface{}
// 		expected interface{}
// 	}{
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &f32, expected: float32(42)},
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &f64, expected: float64(42)},
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &i16, expected: int16(42)},
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &i32, expected: int32(42)},
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &i64, expected: int64(42)},
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &i, expected: int(42)},
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &ui8, expected: uint8(42)},
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &ui16, expected: uint16(42)},
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &ui32, expected: uint32(42)},
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &ui64, expected: uint64(42)},
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &ui, expected: uint(42)},
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &_i8, expected: _int8(42)},
// 		{src: pgtype.Numeric{Float: 0, Status: pgtype.Null}, dst: &pi8, expected: ((*int8)(nil))},
// 		{src: pgtype.Numeric{Float: 0, Status: pgtype.Null}, dst: &_pi8, expected: ((*_int8)(nil))},
// 	}

// 	for i, tt := range simpleTests {
// 		err := tt.src.AssignTo(tt.dst)
// 		if err != nil {
// 			t.Errorf("%d: %v", i, err)
// 		}

// 		if dst := reflect.ValueOf(tt.dst).Elem().Interface(); dst != tt.expected {
// 			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
// 		}
// 	}

// 	pointerAllocTests := []struct {
// 		src      pgtype.Numeric
// 		dst      interface{}
// 		expected interface{}
// 	}{
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &pf32, expected: float32(42)},
// 		{src: pgtype.Numeric{Float: 42, Status: pgtype.Present}, dst: &pf64, expected: float64(42)},
// 	}

// 	for i, tt := range pointerAllocTests {
// 		err := tt.src.AssignTo(tt.dst)
// 		if err != nil {
// 			t.Errorf("%d: %v", i, err)
// 		}

// 		if dst := reflect.ValueOf(tt.dst).Elem().Elem().Interface(); dst != tt.expected {
// 			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
// 		}
// 	}

// 	errorTests := []struct {
// 		src pgtype.Numeric
// 		dst interface{}
// 	}{
// 		{src: pgtype.Numeric{Float: 150, Status: pgtype.Present}, dst: &i8},
// 		{src: pgtype.Numeric{Float: 40000, Status: pgtype.Present}, dst: &i16},
// 		{src: pgtype.Numeric{Float: -1, Status: pgtype.Present}, dst: &ui8},
// 		{src: pgtype.Numeric{Float: -1, Status: pgtype.Present}, dst: &ui16},
// 		{src: pgtype.Numeric{Float: -1, Status: pgtype.Present}, dst: &ui32},
// 		{src: pgtype.Numeric{Float: -1, Status: pgtype.Present}, dst: &ui64},
// 		{src: pgtype.Numeric{Float: -1, Status: pgtype.Present}, dst: &ui},
// 		{src: pgtype.Numeric{Float: 0, Status: pgtype.Null}, dst: &i32},
// 	}

// 	for i, tt := range errorTests {
// 		err := tt.src.AssignTo(tt.dst)
// 		if err == nil {
// 			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
// 		}
// 	}
// }
