package pgtype_test

import (
	"context"
	"encoding/json"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/testutil"
	"github.com/stretchr/testify/require"
)

// For test purposes only. Note that it does not normalize values. e.g. (Int: 1, Exp: 3) will not equal (Int: 1000, Exp: 0)
func numericEqual(left, right *pgtype.Numeric) bool {
	return left.Valid == right.Valid &&
		left.Exp == right.Exp &&
		((left.Int == nil && right.Int == nil) || (left.Int != nil && right.Int != nil && left.Int.Cmp(right.Int) == 0)) &&
		left.NaN == right.NaN
}

// For test purposes only.
func numericNormalizedEqual(left, right *pgtype.Numeric) bool {
	if left.Valid != right.Valid {
		return false
	}

	normLeft := &pgtype.Numeric{Int: (&big.Int{}).Set(left.Int), Valid: left.Valid}
	normRight := &pgtype.Numeric{Int: (&big.Int{}).Set(right.Int), Valid: right.Valid}

	if left.Exp < right.Exp {
		mul := (&big.Int{}).Exp(big.NewInt(10), big.NewInt(int64(right.Exp-left.Exp)), nil)
		normRight.Int.Mul(normRight.Int, mul)
	} else if left.Exp > right.Exp {
		mul := (&big.Int{}).Exp(big.NewInt(10), big.NewInt(int64(left.Exp-right.Exp)), nil)
		normLeft.Int.Mul(normLeft.Int, mul)
	}

	return normLeft.Int.Cmp(normRight.Int) == 0
}

func mustParseBigInt(t *testing.T, src string) *big.Int {
	i := &big.Int{}
	if _, ok := i.SetString(src, 10); !ok {
		t.Fatalf("could not parse big.Int: %s", src)
	}
	return i
}

func TestNumericNormalize(t *testing.T) {
	testutil.TestSuccessfulNormalize(t, []testutil.NormalizeTest{
		{
			SQL:   "select '0'::numeric",
			Value: &pgtype.Numeric{Int: big.NewInt(0), Exp: 0, Valid: true},
		},
		{
			SQL:   "select '1'::numeric",
			Value: &pgtype.Numeric{Int: big.NewInt(1), Exp: 0, Valid: true},
		},
		{
			SQL:   "select '10.00'::numeric",
			Value: &pgtype.Numeric{Int: big.NewInt(1000), Exp: -2, Valid: true},
		},
		{
			SQL:   "select '1e-3'::numeric",
			Value: &pgtype.Numeric{Int: big.NewInt(1), Exp: -3, Valid: true},
		},
		{
			SQL:   "select '-1'::numeric",
			Value: &pgtype.Numeric{Int: big.NewInt(-1), Exp: 0, Valid: true},
		},
		{
			SQL:   "select '10000'::numeric",
			Value: &pgtype.Numeric{Int: big.NewInt(1), Exp: 4, Valid: true},
		},
		{
			SQL:   "select '3.14'::numeric",
			Value: &pgtype.Numeric{Int: big.NewInt(314), Exp: -2, Valid: true},
		},
		{
			SQL:   "select '1.1'::numeric",
			Value: &pgtype.Numeric{Int: big.NewInt(11), Exp: -1, Valid: true},
		},
		{
			SQL:   "select '100010001'::numeric",
			Value: &pgtype.Numeric{Int: big.NewInt(100010001), Exp: 0, Valid: true},
		},
		{
			SQL:   "select '100010001.0001'::numeric",
			Value: &pgtype.Numeric{Int: big.NewInt(1000100010001), Exp: -4, Valid: true},
		},
		{
			SQL: "select '4237234789234789289347892374324872138321894178943189043890124832108934.43219085471578891547854892438945012347981'::numeric",
			Value: &pgtype.Numeric{
				Int:   mustParseBigInt(t, "423723478923478928934789237432487213832189417894318904389012483210893443219085471578891547854892438945012347981"),
				Exp:   -41,
				Valid: true,
			},
		},
		{
			SQL: "select '0.8925092023480223478923478978978937897879595901237890234789243679037419057877231734823098432903527585734549035904590854890345905434578345789347890402348952348905890489054234237489234987723894789234'::numeric",
			Value: &pgtype.Numeric{
				Int:   mustParseBigInt(t, "8925092023480223478923478978978937897879595901237890234789243679037419057877231734823098432903527585734549035904590854890345905434578345789347890402348952348905890489054234237489234987723894789234"),
				Exp:   -196,
				Valid: true,
			},
		},
		{
			SQL: "select '0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000123'::numeric",
			Value: &pgtype.Numeric{
				Int:   mustParseBigInt(t, "123"),
				Exp:   -186,
				Valid: true,
			},
		},
	})
}

func TestNumericTranscode(t *testing.T) {
	max := new(big.Int).Exp(big.NewInt(10), big.NewInt(147454), nil)
	max.Add(max, big.NewInt(1))
	longestNumeric := &pgtype.Numeric{Int: max, Exp: -16383, Valid: true}

	testutil.TestSuccessfulTranscodeEqFunc(t, "numeric", []interface{}{
		&pgtype.Numeric{NaN: true, Valid: true},
		&pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true},
		&pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true},

		&pgtype.Numeric{Int: big.NewInt(0), Exp: 0, Valid: true},
		&pgtype.Numeric{Int: big.NewInt(1), Exp: 0, Valid: true},
		&pgtype.Numeric{Int: big.NewInt(-1), Exp: 0, Valid: true},
		&pgtype.Numeric{Int: big.NewInt(1), Exp: 6, Valid: true},

		// preserves significant zeroes
		&pgtype.Numeric{Int: big.NewInt(10000000), Exp: -1, Valid: true},
		&pgtype.Numeric{Int: big.NewInt(10000000), Exp: -2, Valid: true},
		&pgtype.Numeric{Int: big.NewInt(10000000), Exp: -3, Valid: true},
		&pgtype.Numeric{Int: big.NewInt(10000000), Exp: -4, Valid: true},
		&pgtype.Numeric{Int: big.NewInt(10000000), Exp: -5, Valid: true},
		&pgtype.Numeric{Int: big.NewInt(10000000), Exp: -6, Valid: true},

		&pgtype.Numeric{Int: big.NewInt(314), Exp: -2, Valid: true},
		&pgtype.Numeric{Int: big.NewInt(123), Exp: -7, Valid: true},
		&pgtype.Numeric{Int: big.NewInt(123), Exp: -8, Valid: true},
		&pgtype.Numeric{Int: big.NewInt(123), Exp: -9, Valid: true},
		&pgtype.Numeric{Int: big.NewInt(123), Exp: -1500, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "2437"), Exp: 23790, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "243723409723490243842378942378901237502734019231380123"), Exp: 23790, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 80, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "3723409723490243842378942378901237502734019231380123"), Exp: 81, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "723409723490243842378942378901237502734019231380123"), Exp: 82, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "23409723490243842378942378901237502734019231380123"), Exp: 83, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "3409723490243842378942378901237502734019231380123"), Exp: 84, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "913423409823409243892349028349023482934092340892390101"), Exp: -14021, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -90, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "3423409823409243892349028349023482934092340892390101"), Exp: -91, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "423409823409243892349028349023482934092340892390101"), Exp: -92, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "23409823409243892349028349023482934092340892390101"), Exp: -93, Valid: true},
		&pgtype.Numeric{Int: mustParseBigInt(t, "3409823409243892349028349023482934092340892390101"), Exp: -94, Valid: true},

		longestNumeric,

		&pgtype.Numeric{},
	}, func(aa, bb interface{}) bool {
		a := aa.(pgtype.Numeric)
		b := bb.(pgtype.Numeric)

		return numericEqual(&a, &b)
	})

}

func TestNumericTranscodeFuzz(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	max := &big.Int{}
	max.SetString("9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", 10)

	values := make([]interface{}, 0, 2000)
	for i := 0; i < 10; i++ {
		for j := -50; j < 50; j++ {
			num := (&big.Int{}).Rand(r, max)
			negNum := &big.Int{}
			negNum.Neg(num)
			values = append(values, &pgtype.Numeric{Int: num, Exp: int32(j), Valid: true})
			values = append(values, &pgtype.Numeric{Int: negNum, Exp: int32(j), Valid: true})
		}
	}

	testutil.TestSuccessfulTranscodeEqFunc(t, "numeric", values,
		func(aa, bb interface{}) bool {
			a := aa.(pgtype.Numeric)
			b := bb.(pgtype.Numeric)

			return numericNormalizedEqual(&a, &b)
		})
}

func TestNumericSet(t *testing.T) {
	successfulTests := []struct {
		source interface{}
		result *pgtype.Numeric
	}{
		{source: float32(1), result: &pgtype.Numeric{Int: big.NewInt(1), Valid: true}},
		{source: float32(math.Copysign(0, -1)), result: &pgtype.Numeric{Int: big.NewInt(0), Valid: true}},
		{source: float64(1), result: &pgtype.Numeric{Int: big.NewInt(1), Valid: true}},
		{source: float64(math.Copysign(0, -1)), result: &pgtype.Numeric{Int: big.NewInt(0), Valid: true}},
		{source: int8(1), result: &pgtype.Numeric{Int: big.NewInt(1), Valid: true}},
		{source: int16(1), result: &pgtype.Numeric{Int: big.NewInt(1), Valid: true}},
		{source: int32(1), result: &pgtype.Numeric{Int: big.NewInt(1), Valid: true}},
		{source: int64(1), result: &pgtype.Numeric{Int: big.NewInt(1), Valid: true}},
		{source: int8(-1), result: &pgtype.Numeric{Int: big.NewInt(-1), Valid: true}},
		{source: int16(-1), result: &pgtype.Numeric{Int: big.NewInt(-1), Valid: true}},
		{source: int32(-1), result: &pgtype.Numeric{Int: big.NewInt(-1), Valid: true}},
		{source: int64(-1), result: &pgtype.Numeric{Int: big.NewInt(-1), Valid: true}},
		{source: uint8(1), result: &pgtype.Numeric{Int: big.NewInt(1), Valid: true}},
		{source: uint16(1), result: &pgtype.Numeric{Int: big.NewInt(1), Valid: true}},
		{source: uint32(1), result: &pgtype.Numeric{Int: big.NewInt(1), Valid: true}},
		{source: uint64(1), result: &pgtype.Numeric{Int: big.NewInt(1), Valid: true}},
		{source: "1", result: &pgtype.Numeric{Int: big.NewInt(1), Valid: true}},
		{source: _int8(1), result: &pgtype.Numeric{Int: big.NewInt(1), Valid: true}},
		{source: float64(1000), result: &pgtype.Numeric{Int: big.NewInt(1), Exp: 3, Valid: true}},
		{source: float64(1234), result: &pgtype.Numeric{Int: big.NewInt(1234), Exp: 0, Valid: true}},
		{source: float64(12345678900), result: &pgtype.Numeric{Int: big.NewInt(123456789), Exp: 2, Valid: true}},
		{source: float64(12345.678901), result: &pgtype.Numeric{Int: big.NewInt(12345678901), Exp: -6, Valid: true}},
		{source: math.NaN(), result: &pgtype.Numeric{Int: nil, Exp: 0, Valid: true, NaN: true}},
		{source: float32(math.NaN()), result: &pgtype.Numeric{Int: nil, Exp: 0, Valid: true, NaN: true}},
		{source: pgtype.Infinity, result: &pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}},
		{source: math.Inf(1), result: &pgtype.Numeric{Valid: true, InfinityModifier: pgtype.Infinity}},
		{source: float32(math.Inf(1)), result: &pgtype.Numeric{Valid: true, InfinityModifier: pgtype.Infinity}},
		{source: pgtype.NegativeInfinity, result: &pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true}},
		{source: math.Inf(-1), result: &pgtype.Numeric{Valid: true, InfinityModifier: pgtype.NegativeInfinity}},
		{source: float32(math.Inf(1)), result: &pgtype.Numeric{Valid: true, InfinityModifier: pgtype.Infinity}},
	}

	for i, tt := range successfulTests {
		r := &pgtype.Numeric{}
		err := r.Set(tt.source)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if !numericEqual(r, tt.result) {
			t.Errorf("%d: expected %v to convert to %v, but it was %v", i, tt.source, tt.result, r)
		}
	}
}

func TestNumericAssignTo(t *testing.T) {
	var i8 int8
	var i16 int16
	var i32 int32
	var i64 int64
	var i int
	var ui8 uint8
	var ui16 uint16
	var ui32 uint32
	var ui64 uint64
	var ui uint
	var pi8 *int8
	var _i8 _int8
	var _pi8 *_int8
	var f32 float32
	var f64 float64
	var pf32 *float32
	var pf64 *float64

	simpleTests := []struct {
		src      *pgtype.Numeric
		dst      interface{}
		expected interface{}
	}{
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &f32, expected: float32(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &f64, expected: float64(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Exp: -1, Valid: true}, dst: &f32, expected: float32(4.2)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Exp: -1, Valid: true}, dst: &f64, expected: float64(4.2)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &i16, expected: int16(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &i32, expected: int32(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &i64, expected: int64(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Exp: 3, Valid: true}, dst: &i64, expected: int64(42000)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &i, expected: int(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &ui8, expected: uint8(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &ui16, expected: uint16(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &ui32, expected: uint32(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &ui64, expected: uint64(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &ui, expected: uint(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &_i8, expected: _int8(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(0)}, dst: &pi8, expected: ((*int8)(nil))},
		{src: &pgtype.Numeric{Int: big.NewInt(0)}, dst: &_pi8, expected: ((*_int8)(nil))},
		{src: &pgtype.Numeric{Int: big.NewInt(1006), Exp: -2, Valid: true}, dst: &f64, expected: float64(10.06)}, // https://github.com/jackc/pgtype/issues/27
		{src: &pgtype.Numeric{Valid: true, NaN: true}, dst: &f64, expected: math.NaN()},
		{src: &pgtype.Numeric{Valid: true, NaN: true}, dst: &f32, expected: float32(math.NaN())},
		{src: &pgtype.Numeric{Valid: true, InfinityModifier: pgtype.Infinity}, dst: &f64, expected: math.Inf(1)},
		{src: &pgtype.Numeric{Valid: true, InfinityModifier: pgtype.Infinity}, dst: &f32, expected: float32(math.Inf(1))},
		{src: &pgtype.Numeric{Valid: true, InfinityModifier: pgtype.NegativeInfinity}, dst: &f64, expected: math.Inf(-1)},
		{src: &pgtype.Numeric{Valid: true, InfinityModifier: pgtype.NegativeInfinity}, dst: &f32, expected: float32(math.Inf(-1))},
	}

	for i, tt := range simpleTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		dst := reflect.ValueOf(tt.dst).Elem().Interface()
		switch dstTyped := dst.(type) {
		case float32:
			nanExpected := math.IsNaN(float64(tt.expected.(float32)))
			if nanExpected && !math.IsNaN(float64(dstTyped)) {
				t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
			} else if !nanExpected && dst != tt.expected {
				t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
			}
		case float64:
			nanExpected := math.IsNaN(tt.expected.(float64))
			if nanExpected && !math.IsNaN(dstTyped) {
				t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
			} else if !nanExpected && dst != tt.expected {
				t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
			}
		default:
			if dst != tt.expected {
				t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
			}
		}
	}

	pointerAllocTests := []struct {
		src      *pgtype.Numeric
		dst      interface{}
		expected interface{}
	}{
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &pf32, expected: float32(42)},
		{src: &pgtype.Numeric{Int: big.NewInt(42), Valid: true}, dst: &pf64, expected: float64(42)},
	}

	for i, tt := range pointerAllocTests {
		err := tt.src.AssignTo(tt.dst)
		if err != nil {
			t.Errorf("%d: %v", i, err)
		}

		if dst := reflect.ValueOf(tt.dst).Elem().Elem().Interface(); dst != tt.expected {
			t.Errorf("%d: expected %v to assign %v, but result was %v", i, tt.src, tt.expected, dst)
		}
	}

	errorTests := []struct {
		src *pgtype.Numeric
		dst interface{}
	}{
		{src: &pgtype.Numeric{Int: big.NewInt(150), Valid: true}, dst: &i8},
		{src: &pgtype.Numeric{Int: big.NewInt(40000), Valid: true}, dst: &i16},
		{src: &pgtype.Numeric{Int: big.NewInt(-1), Valid: true}, dst: &ui8},
		{src: &pgtype.Numeric{Int: big.NewInt(-1), Valid: true}, dst: &ui16},
		{src: &pgtype.Numeric{Int: big.NewInt(-1), Valid: true}, dst: &ui32},
		{src: &pgtype.Numeric{Int: big.NewInt(-1), Valid: true}, dst: &ui64},
		{src: &pgtype.Numeric{Int: big.NewInt(-1), Valid: true}, dst: &ui},
		{src: &pgtype.Numeric{Int: big.NewInt(0)}, dst: &i32},
	}

	for i, tt := range errorTests {
		err := tt.src.AssignTo(tt.dst)
		if err == nil {
			t.Errorf("%d: expected error but none was returned (%v -> %v)", i, tt.src, tt.dst)
		}
	}
}

func TestNumericEncodeDecodeBinary(t *testing.T) {
	ci := pgtype.NewConnInfo()
	tests := []interface{}{
		123,
		0.000012345,
		1.00002345,
		math.NaN(),
		float32(math.NaN()),
		math.Inf(1),
		float32(math.Inf(1)),
		math.Inf(-1),
		float32(math.Inf(-1)),
	}

	for i, tt := range tests {
		toString := func(n *pgtype.Numeric) string {
			ci := pgtype.NewConnInfo()
			text, err := n.EncodeText(ci, nil)
			if err != nil {
				t.Errorf("%d (EncodeText): %v", i, err)
			}
			return string(text)
		}
		numeric := &pgtype.Numeric{}
		numeric.Set(tt)

		encoded, err := numeric.EncodeBinary(ci, nil)
		if err != nil {
			t.Errorf("%d (EncodeBinary): %v", i, err)
		}
		decoded := &pgtype.Numeric{}
		err = decoded.DecodeBinary(ci, encoded)
		if err != nil {
			t.Errorf("%d (DecodeBinary): %v", i, err)
		}

		text0 := toString(numeric)
		text1 := toString(decoded)

		if text0 != text1 {
			t.Errorf("%d: expected %v to equal to %v, but doesn't", i, text0, text1)
		}
	}
}

func TestNumericMarshalJSON(t *testing.T) {
	conn := testutil.MustConnectPgx(t)
	defer testutil.MustCloseContext(t, conn)

	for i, tt := range []struct {
		decString string
	}{
		{"NaN"},
		{"0"},
		{"1"},
		{"-1"},
		{"1000000000000000000"},
		{"1234.56789"},
		{"1.56789"},
		{"0.00000000000056789"},
		{"0.00123000"},
		{"123e-3"},
		{"243723409723490243842378942378901237502734019231380123e23790"},
		{"3409823409243892349028349023482934092340892390101e-14021"},
	} {
		var num pgtype.Numeric
		var pgJSON string
		err := conn.QueryRow(context.Background(), `select $1::numeric, to_json($1::numeric)`, tt.decString).Scan(&num, &pgJSON)
		require.NoErrorf(t, err, "%d", i)

		goJSON, err := json.Marshal(num)
		require.NoErrorf(t, err, "%d", i)

		require.Equal(t, pgJSON, string(goJSON))
	}
}
