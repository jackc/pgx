package pgtype_test

import (
	"context"
	"encoding/json"
	"math"
	"math/big"
	"math/rand"
	"strconv"
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
		{mustParseNumeric(t, "1"), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "1"))},
		{mustParseNumeric(t, "3.14159"), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "3.14159"))},
		{mustParseNumeric(t, "100010001"), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "100010001"))},
		{mustParseNumeric(t, "100010001.0001"), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "100010001.0001"))},
		{mustParseNumeric(t, "4237234789234789289347892374324872138321894178943189043890124832108934.43219085471578891547854892438945012347981"), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "4237234789234789289347892374324872138321894178943189043890124832108934.43219085471578891547854892438945012347981"))},
		{mustParseNumeric(t, "0.8925092023480223478923478978978937897879595901237890234789243679037419057877231734823098432903527585734549035904590854890345905434578345789347890402348952348905890489054234237489234987723894789234"), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "0.8925092023480223478923478978978937897879595901237890234789243679037419057877231734823098432903527585734549035904590854890345905434578345789347890402348952348905890489054234237489234987723894789234"))},
		{mustParseNumeric(t, "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000123"), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000123"))},
		{pgtype.Numeric{Int: mustParseBigInt(t, "243723409723490243842378942378901237502734019231380123"), Exp: 23790, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "243723409723490243842378942378901237502734019231380123"), Exp: 23790, Valid: true})},
		{pgtype.Numeric{Int: mustParseBigInt(t, "2437"), Exp: 23790, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "2437"), Exp: 23790, Valid: true})},
		{pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 80, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 80, Valid: true})},
		{pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 81, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 81, Valid: true})},
		{pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 82, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 82, Valid: true})},
		{pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 83, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 83, Valid: true})},
		{pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 84, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "43723409723490243842378942378901237502734019231380123"), Exp: 84, Valid: true})},
		{pgtype.Numeric{Int: mustParseBigInt(t, "913423409823409243892349028349023482934092340892390101"), Exp: -14021, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "913423409823409243892349028349023482934092340892390101"), Exp: -14021, Valid: true})},
		{pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -90, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -90, Valid: true})},
		{pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -91, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -91, Valid: true})},
		{pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -92, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -92, Valid: true})},
		{pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -93, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{Int: mustParseBigInt(t, "13423409823409243892349028349023482934092340892390101"), Exp: -93, Valid: true})},
		{pgtype.Numeric{NaN: true, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{NaN: true, Valid: true})},
		{longestNumeric, new(pgtype.Numeric), isExpectedEqNumeric(longestNumeric)},
		{mustParseNumeric(t, "1"), new(int64), isExpectedEq(int64(1))},
		{math.NaN(), new(float64), func(a any) bool { return math.IsNaN(a.(float64)) }},
		{float32(math.NaN()), new(float32), func(a any) bool { return math.IsNaN(float64(a.(float32))) }},
		{int64(-1), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "-1"))},
		{int64(0), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "0"))},
		{int64(1), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "1"))},
		{int64(math.MinInt64), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, strconv.FormatInt(math.MinInt64, 10)))},
		{int64(math.MinInt64 + 1), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, strconv.FormatInt(math.MinInt64+1, 10)))},
		{int64(math.MaxInt64), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, strconv.FormatInt(math.MaxInt64, 10)))},
		{int64(math.MaxInt64 - 1), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, strconv.FormatInt(math.MaxInt64-1, 10)))},
		{uint64(100), new(uint64), isExpectedEq(uint64(100))},
		{uint64(math.MaxUint64), new(uint64), isExpectedEq(uint64(math.MaxUint64))},
		{uint(math.MaxUint), new(uint), isExpectedEq(uint(math.MaxUint))},
		{uint(100), new(uint), isExpectedEq(uint(100))},
		{"1.23", new(string), isExpectedEq("1.23")},
		{pgtype.Numeric{}, new(pgtype.Numeric), isExpectedEq(pgtype.Numeric{})},
		{nil, new(pgtype.Numeric), isExpectedEq(pgtype.Numeric{})},
		{mustParseNumeric(t, "1"), new(string), isExpectedEq("1")},
		{pgtype.Numeric{NaN: true, Valid: true}, new(string), isExpectedEq("NaN")},
	})

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "int8", []pgxtest.ValueRoundTripTest{
		{mustParseNumeric(t, "-1"), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "-1"))},
		{mustParseNumeric(t, "0"), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "0"))},
		{mustParseNumeric(t, "1"), new(pgtype.Numeric), isExpectedEqNumeric(mustParseNumeric(t, "1"))},
	})
}

func TestNumericCodecInfinity(t *testing.T) {
	skipCockroachDB(t, "server formats numeric text format differently")
	skipPostgreSQLVersionLessThan(t, 14)

	pgxtest.RunValueRoundTripTests(context.Background(), t, defaultConnTestRunner, nil, "numeric", []pgxtest.ValueRoundTripTest{
		{math.Inf(1), new(float64), isExpectedEq(math.Inf(1))},
		{float32(math.Inf(1)), new(float32), isExpectedEq(float32(math.Inf(1)))},
		{math.Inf(-1), new(float64), isExpectedEq(math.Inf(-1))},
		{float32(math.Inf(-1)), new(float32), isExpectedEq(float32(math.Inf(-1)))},
		{pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true})},
		{pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, new(pgtype.Numeric), isExpectedEqNumeric(pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true})},
		{pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}, new(string), isExpectedEq("Infinity")},
		{pgtype.Numeric{InfinityModifier: pgtype.NegativeInfinity, Valid: true}, new(string), isExpectedEq("-Infinity")},
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

	r := rand.New(rand.NewSource(0))
	max := &big.Int{}
	max.SetString("9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", 10)

	tests := make([]pgxtest.ValueRoundTripTest, 0, 2000)
	for i := 0; i < 10; i++ {
		for j := -50; j < 50; j++ {
			num := (&big.Int{}).Rand(r, max)

			n := pgtype.Numeric{Int: num, Exp: int32(j), Valid: true}
			tests = append(tests, pgxtest.ValueRoundTripTest{n, new(pgtype.Numeric), isExpectedEqNumeric(n)})

			negNum := &big.Int{}
			negNum.Neg(num)
			n = pgtype.Numeric{Int: negNum, Exp: int32(j), Valid: true}
			tests = append(tests, pgxtest.ValueRoundTripTest{n, new(pgtype.Numeric), isExpectedEqNumeric(n)})
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
