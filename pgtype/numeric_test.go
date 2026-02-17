package pgtype_test

import (
	"context"
	"encoding/json"
	"math"
	"math/big"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustParseBigInt(t *testing.T, src string) *big.Int {
	i := &big.Int{}
	if _, ok := i.SetString(src, 10); !ok {
		t.Fatalf("could not parse big.Int: %s", src)
	}
	return i
}

func isExpectedEqNumeric(a any) func(any) bool {
	return func(v any) bool {
		aa := a.(pgtype.Numeric)
		vv := v.(pgtype.Numeric)

		if aa.Valid != vv.Valid {
			return false
		}

		// If NULL doesn't matter what the rest of the values are.
		if !aa.Valid {
			return true
		}

		if !(aa.NaN == vv.NaN && aa.InfinityModifier == vv.InfinityModifier) {
			return false
		}

		// If NaN or InfinityModifier are set then Int and Exp don't matter.
		if aa.NaN || aa.InfinityModifier != pgtype.Finite {
			return true
		}

		aaInt := (&big.Int{}).Set(aa.Int)
		vvInt := (&big.Int{}).Set(vv.Int)

		if aa.Exp < vv.Exp {
			mul := (&big.Int{}).Exp(big.NewInt(10), big.NewInt(int64(vv.Exp-aa.Exp)), nil)
			vvInt.Mul(vvInt, mul)
		} else if aa.Exp > vv.Exp {
			mul := (&big.Int{}).Exp(big.NewInt(10), big.NewInt(int64(aa.Exp-vv.Exp)), nil)
			aaInt.Mul(aaInt, mul)
		}

		return aaInt.Cmp(vvInt) == 0
	}
}

func mustParseNumeric(t *testing.T, src string) pgtype.Numeric {
	var n pgtype.Numeric
	plan := pgtype.NumericCodec{}.PlanScan(nil, pgtype.NumericOID, pgtype.TextFormatCode, &n)
	require.NotNil(t, plan)
	err := plan.Scan([]byte(src), &n)
	require.NoError(t, err)
	return n
}

func TestNumericCodec(t *testing.T) {
	skipCockroachDB(t, "server formats numeric text format differently")

	max := new(big.Int).Exp(big.NewInt(10), big.NewInt(147454), nil)
	max.Add(max, big.NewInt(1))
	longestNumeric := pgtype.Numeric{Int: max, Exp: -16383, Valid: true}

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "numeric", []pgxtest.ValueRoundTripTest{
		{Param: mustParseNumeric(t, "1"), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "1"))},
		{Param: mustParseNumeric(t, "3.14159"), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "3.14159"))},
		{Param: mustParseNumeric(t, "100010001"), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "100010001"))},
		{Param: mustParseNumeric(t, "100010001.0001"), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "100010001.0001"))},
		{Param: mustParseNumeric(t, "4237234789234789289347892374324872138321894178943189043890124832108934.43219085471578891547854892438945012347981"), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "4237234789234789289347892374324872138321894178943189043890124832108934.43219085471578891547854892438945012347981"))},
		{Param: mustParseNumeric(t, "0.8925092023480223478923478978978937897879595901237890234789243679037419057877231734823098432903527585734549035904590854890345905434578345789347890402348952348905890489054234237489234987723894789234"), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "0.8925092023480223478923478978978937897879595901237890234789243679037419057877231734823098432903527585734549035904590854890345905434578345789347890402348952348905890489054234237489234987723894789234"))},
		{Param: mustParseNumeric(t, "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000123"), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000123"))},
		{Param: mustParseNumeric(t, "67"+strings.Repeat("0", 44535)+".0"), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "67"+strings.Repeat("0", 44535)+".0"))},
		{Param: pgtype.Numeric{Int: mustParseBigInt(t, "243723409723490243842378942378901237502734019231380123"), Exp: 23790, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "243723409723490243842378942378901237502734019231380123"), Exp: 23790, Valid: true})},
		{Param: pgtype.Numeric{Int: mustParseBigInt(t, "2437"), Exp: 23790, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "2437"), Exp: 23790, Valid: true})},
		{Param: pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 80, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 80, Valid: true})},
		{Param: pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 81, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 81, Valid: true})},
		{Param: pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 82, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 82, Valid: true})},
		{Param: pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 83, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 83, Valid: true})},
		{Param: pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 84, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 84, Valid: true})},
		{Param: pgtype.Numeric{Int: mustParseBigInt(t, "913423409823409243892349028349023482934092340892390101"), Exp: -14021, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "913423409823409243892349028349023482934092340892390101"), Exp: -14021, Valid: true})},
		{Param: pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -90, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -90, Valid: true})},
		{Param: pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -91, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -91, Valid: true})},
		{Param: pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -92, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -92, Valid: true})},
		{Param: pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -93, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -93, Valid: true})},
		{Param: pgtype.Numeric{NaN: true, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{NaN: true, Valid: true})},
		{Param: longestNumeric, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(longestNumeric)},
		{Param: mustParseNumeric(t, "1"), Result: new(int64), Test: isExpectedEq(int64(1))},
		{Param: math.NaN(), Result: new(float64), Test: func(a any) bool { return math.IsNaN(a.(float64)) }},
		{Param: float32(math.NaN()), Result: new(float32), Test: func(a any) bool { return math.IsNaN(float64(a.(float32))) }},
		{Param: int64(-1), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "-1"))},
		{Param: int64(0), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "0"))},
		{Param: int64(1), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "1"))},
		{Param: int64(math.MinInt64), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, strconv.FormatInt(math.MinInt64, 10)))},
		{Param: int64(math.MinInt64 + 1), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, strconv.FormatInt(math.MinInt64+1, 10)))},
		{Param: int64(math.MaxInt64), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, strconv.FormatInt(math.MaxInt64, 10)))},
		{Param: int64(math.MaxInt64 - 1), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, strconv.FormatInt(math.MaxInt64-1, 10)))},
		{Param: uint64(100), Result: new(uint64), Test: isExpectedEq(uint64(100))},
		{Param: uint64(math.MaxUint64), Result: new(uint64), Test: isExpectedEq(uint64(math.MaxUint64))},
		{Param: uint(math.MaxUint), Result: new(uint), Test: isExpectedEq(uint(math.MaxUint))},
		{Param: uint(100), Result: new(uint), Test: isExpectedEq(uint(100))},
		{Param: "1.23", Result: new(string), Test: isExpectedEq("1.23")},
		{Param: pgtype.Numeric{}, Result: new(pgtype.Numeric), Test: isExpectedEq(pgtype.Numeric{})},
		{Param: nil, Result: new(pgtype.Numeric), Test: isExpectedEq(pgtype.Numeric{})},
		{Param: mustParseNumeric(t, "1"), Result: new(string), Test: isExpectedEq("1")},
		{Param: pgtype.Numeric{NaN: true, Valid: true}, Result: new(string), Test: isExpectedEq("NaN")},
	})

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "int8", []pgxtest.ValueRoundTripTest{
		{Param: mustParseNumeric(t, "-1"), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "-1"))},
		{Param: mustParseNumeric(t, "0"), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "0"))},
		{Param: mustParseNumeric(t, "1"), Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(mustParseNumeric(t, "1"))},
	})
}

func TestNumericCodecInfinity(t *testing.T) {
	skipCockroachDB(t, "server formats numeric text format differently")
	skipPostgreSQLVersionLessThan(t, 14)

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "numeric", []pgxtest.ValueRoundTripTest{
		{Param: math.Inf(1), Result: new(float64), Test: isExpectedEq(math.Inf(1))},
		{Param: float32(math.Inf(1)), Result: new(float32), Test: isExpectedEq(float32(math.Inf(1)))},
		{Param: math.Inf(-1), Result: new(float64), Test: isExpectedEq(math.Inf(-1))},
		{Param: float32(math.Inf(-1)), Result: new(float32), Test: isExpectedEq(float32(math.Inf(-1)))},
		{Param: pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true})},
		{Param: pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true})},
		{Param: pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}, Result: new(string), Test: isExpectedEq("Infinity")},
		{Param: pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, Result: new(string), Test: isExpectedEq("-Infinity")},
	})
}

func TestNumericFloat64Valuer(t *testing.T) {
	for i, tt := range []struct {
		n pgtype.Numeric
		f pgtype.Float8
	}{
		{mustParseNumeric(t, "1"), pgtype.Float8{Float64: 1, Valid: true}},
		{mustParseNumeric(t, "0.0000000000000000001"), pgtype.Float8{Float64: 0.0000000000000000001, Valid: true}},
		{mustParseNumeric(t, "-99999999999"), pgtype.Float8{Float64: -99999999999, Valid: true}},
		{pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}, pgtype.Float8{Float64: math.Inf(1), Valid: true}},
		{pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, pgtype.Float8{Float64: math.Inf(-1), Valid: true}},
		{pgtype.Numeric{Valid: true}, pgtype.Float8{Valid: true}},
		{pgtype.Numeric{}, pgtype.Float8{}},
	} {
		f, err := tt.n.Float64Value()
		assert.NoErrorf(t, err, "%d", i)
		assert.Equalf(t, tt.f, f, "%d", i)
	}

	f, err := pgtype.Numeric{NaN: true, Valid: true}.Float64Value()
	assert.NoError(t, err)
	assert.True(t, math.IsNaN(f.Float64))
	assert.True(t, f.Valid)
}

func TestNumericCodecFuzz(t *testing.T) {
	skipCockroachDB(t, "server formats numeric text format differently")

	r := rand.New(rand.NewPCG(0, 0))
	max := &big.Int{}
	max.SetString("9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", 10)

	tests := make([]pgxtest.ValueRoundTripTest, 0, 2000)
	for range 10 {
		for j := -50; j < 50; j++ {
			byteLen := (max.BitLen() + 7) / 8
			bytes := make([]byte, byteLen)
			for k := 0; k < byteLen; {
				val := r.Uint64()
				for b := 0; b < 8 && k < byteLen; b++ {
					bytes[k] = byte(val >> (b * 8))
					k++
				}
			}
			num := new(big.Int).SetBytes(bytes)
			num.Mod(num, max)

			n := pgtype.Numeric{Int: num, Exp: int32(j), Valid: true}
			tests = append(tests, pgxtest.ValueRoundTripTest{Param: n, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(n)})

			negNum := &big.Int{}
			negNum.Neg(num)
			n = pgtype.Numeric{Int: negNum, Exp: int32(j), Valid: true}
			tests = append(tests, pgxtest.ValueRoundTripTest{Param: n, Result: new(pgtype.Numeric), Test: isExpectedEqNumeric(n)})
		}
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "numeric", tests)
}

func TestNumericMarshalJSON(t *testing.T) {
	skipCockroachDB(t, "server formats numeric text format differently")

	defaultConnTestRunner.RunTest(context.Background(), t, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
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
			{"-1.1"},
			{"-1.0231"},
			{"-10.0231"},
			{"-0.1"},   // failed with "invalid character '.' in numeric literal"
			{"-0.01"},  // failed with "invalid character '-' after decimal point in numeric literal"
			{"-0.001"}, // failed with "invalid character '-' after top-level value"
		} {
			var num pgtype.Numeric
			var pgJSON string
			err := conn.QueryRow(ctx, `select $1::numeric, to_json($1::numeric)`, tt.decString).Scan(&num, &pgJSON)
			require.NoErrorf(t, err, "%d", i)

			goJSON, err := json.Marshal(num)
			require.NoErrorf(t, err, "%d", i)

			require.Equal(t, pgJSON, string(goJSON))
		}
	})
}

func TestNumericUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		want    *pgtype.Numeric
		src     []byte
		wantErr bool
	}{
		{
			name:    "null",
			want:    &pgtype.Numeric{},
			src:     []byte(`null`),
			wantErr: false,
		},
		{
			name:    "NaN",
			want:    &pgtype.Numeric{Valid: true, NaN: true},
			src:     []byte(`"NaN"`),
			wantErr: false,
		},
		{
			name:    "0",
			want:    &pgtype.Numeric{Valid: true, Int: big.NewInt(0)},
			src:     []byte("0"),
			wantErr: false,
		},
		{
			name:    "1",
			want:    &pgtype.Numeric{Valid: true, Int: big.NewInt(1)},
			src:     []byte("1"),
			wantErr: false,
		},
		{
			name:    "-1",
			want:    &pgtype.Numeric{Valid: true, Int: big.NewInt(-1)},
			src:     []byte("-1"),
			wantErr: false,
		},
		{
			name:    "bigInt",
			want:    &pgtype.Numeric{Valid: true, Int: big.NewInt(1), Exp: 30},
			src:     []byte("1000000000000000000000000000000"),
			wantErr: false,
		},
		{
			name:    "float: 1234.56789",
			want:    &pgtype.Numeric{Valid: true, Int: big.NewInt(123456789), Exp: -5},
			src:     []byte("1234.56789"),
			wantErr: false,
		},
		{
			name:    "invalid value",
			want:    &pgtype.Numeric{},
			src:     []byte("0xffff"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := &pgtype.Numeric{}
			if err := got.UnmarshalJSON(tt.src); (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UnmarshalJSON() got = %v, want %v", got, tt.want)
			}
		})
	}
}
