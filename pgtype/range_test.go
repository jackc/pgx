package pgtype

import (
	"bytes"
	"math/big"
	"testing"
	"time"
)

func TestParseUntypedTextRange(t *testing.T) {
	tests := []struct {
		src    string
		result untypedTextRange
		err    error
	}{
		{
			src:    `[1,2)`,
			result: untypedTextRange{Lower: "1", Upper: "2", LowerType: Inclusive, UpperType: Exclusive},
			err:    nil,
		},
		{
			src:    `[1,2]`,
			result: untypedTextRange{Lower: "1", Upper: "2", LowerType: Inclusive, UpperType: Inclusive},
			err:    nil,
		},
		{
			src:    `(1,3)`,
			result: untypedTextRange{Lower: "1", Upper: "3", LowerType: Exclusive, UpperType: Exclusive},
			err:    nil,
		},
		{
			src:    ` [1,2) `,
			result: untypedTextRange{Lower: "1", Upper: "2", LowerType: Inclusive, UpperType: Exclusive},
			err:    nil,
		},
		{
			src:    `[ foo , bar )`,
			result: untypedTextRange{Lower: " foo ", Upper: " bar ", LowerType: Inclusive, UpperType: Exclusive},
			err:    nil,
		},
		{
			src:    `["foo","bar")`,
			result: untypedTextRange{Lower: "foo", Upper: "bar", LowerType: Inclusive, UpperType: Exclusive},
			err:    nil,
		},
		{
			src:    `["f""oo","b""ar")`,
			result: untypedTextRange{Lower: `f"oo`, Upper: `b"ar`, LowerType: Inclusive, UpperType: Exclusive},
			err:    nil,
		},
		{
			src:    `["f""oo","b""ar")`,
			result: untypedTextRange{Lower: `f"oo`, Upper: `b"ar`, LowerType: Inclusive, UpperType: Exclusive},
			err:    nil,
		},
		{
			src:    `["","bar")`,
			result: untypedTextRange{Lower: ``, Upper: `bar`, LowerType: Inclusive, UpperType: Exclusive},
			err:    nil,
		},
		{
			src:    `[f\"oo\,,b\\ar\))`,
			result: untypedTextRange{Lower: `f"oo,`, Upper: `b\ar)`, LowerType: Inclusive, UpperType: Exclusive},
			err:    nil,
		},
		{
			src:    `empty`,
			result: untypedTextRange{Lower: "", Upper: "", LowerType: Empty, UpperType: Empty},
			err:    nil,
		},
	}

	for i, tt := range tests {
		r, err := parseUntypedTextRange(tt.src)
		if err != tt.err {
			t.Errorf("%d. `%v`: expected err %v, got %v", i, tt.src, tt.err, err)
			continue
		}

		if r.LowerType != tt.result.LowerType {
			t.Errorf("%d. `%v`: expected result lower type %v, got %v", i, tt.src, string(tt.result.LowerType), string(r.LowerType))
		}

		if r.UpperType != tt.result.UpperType {
			t.Errorf("%d. `%v`: expected result upper type %v, got %v", i, tt.src, string(tt.result.UpperType), string(r.UpperType))
		}

		if r.Lower != tt.result.Lower {
			t.Errorf("%d. `%v`: expected result lower %v, got %v", i, tt.src, tt.result.Lower, r.Lower)
		}

		if r.Upper != tt.result.Upper {
			t.Errorf("%d. `%v`: expected result upper %v, got %v", i, tt.src, tt.result.Upper, r.Upper)
		}
	}
}

func TestParseUntypedBinaryRange(t *testing.T) {
	tests := []struct {
		src    []byte
		result untypedBinaryRange
		err    error
	}{
		{
			src:    []byte{0, 0, 0, 0, 2, 0, 4, 0, 0, 0, 2, 0, 5},
			result: untypedBinaryRange{Lower: []byte{0, 4}, Upper: []byte{0, 5}, LowerType: Exclusive, UpperType: Exclusive},
			err:    nil,
		},
		{
			src:    []byte{1},
			result: untypedBinaryRange{Lower: nil, Upper: nil, LowerType: Empty, UpperType: Empty},
			err:    nil,
		},
		{
			src:    []byte{2, 0, 0, 0, 2, 0, 4, 0, 0, 0, 2, 0, 5},
			result: untypedBinaryRange{Lower: []byte{0, 4}, Upper: []byte{0, 5}, LowerType: Inclusive, UpperType: Exclusive},
			err:    nil,
		},
		{
			src:    []byte{4, 0, 0, 0, 2, 0, 4, 0, 0, 0, 2, 0, 5},
			result: untypedBinaryRange{Lower: []byte{0, 4}, Upper: []byte{0, 5}, LowerType: Exclusive, UpperType: Inclusive},
			err:    nil,
		},
		{
			src:    []byte{6, 0, 0, 0, 2, 0, 4, 0, 0, 0, 2, 0, 5},
			result: untypedBinaryRange{Lower: []byte{0, 4}, Upper: []byte{0, 5}, LowerType: Inclusive, UpperType: Inclusive},
			err:    nil,
		},
		{
			src:    []byte{8, 0, 0, 0, 2, 0, 5},
			result: untypedBinaryRange{Lower: nil, Upper: []byte{0, 5}, LowerType: Unbounded, UpperType: Exclusive},
			err:    nil,
		},
		{
			src:    []byte{12, 0, 0, 0, 2, 0, 5},
			result: untypedBinaryRange{Lower: nil, Upper: []byte{0, 5}, LowerType: Unbounded, UpperType: Inclusive},
			err:    nil,
		},
		{
			src:    []byte{16, 0, 0, 0, 2, 0, 4},
			result: untypedBinaryRange{Lower: []byte{0, 4}, Upper: nil, LowerType: Exclusive, UpperType: Unbounded},
			err:    nil,
		},
		{
			src:    []byte{18, 0, 0, 0, 2, 0, 4},
			result: untypedBinaryRange{Lower: []byte{0, 4}, Upper: nil, LowerType: Inclusive, UpperType: Unbounded},
			err:    nil,
		},
		{
			src:    []byte{24},
			result: untypedBinaryRange{Lower: nil, Upper: nil, LowerType: Unbounded, UpperType: Unbounded},
			err:    nil,
		},
	}

	for i, tt := range tests {
		r, err := parseUntypedBinaryRange(tt.src)
		if err != tt.err {
			t.Errorf("%d. `%v`: expected err %v, got %v", i, tt.src, tt.err, err)
			continue
		}

		if r.LowerType != tt.result.LowerType {
			t.Errorf("%d. `%v`: expected result lower type %v, got %v", i, tt.src, string(tt.result.LowerType), string(r.LowerType))
		}

		if r.UpperType != tt.result.UpperType {
			t.Errorf("%d. `%v`: expected result upper type %v, got %v", i, tt.src, string(tt.result.UpperType), string(r.UpperType))
		}

		if bytes.Compare(r.Lower, tt.result.Lower) != 0 {
			t.Errorf("%d. `%v`: expected result lower %v, got %v", i, tt.src, tt.result.Lower, r.Lower)
		}

		if bytes.Compare(r.Upper, tt.result.Upper) != 0 {
			t.Errorf("%d. `%v`: expected result upper %v, got %v", i, tt.src, tt.result.Upper, r.Upper)
		}
	}
}

func TestRangeDateMarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src    Range[Date]
		result string
	}{
		{src: Range[Date]{}, result: "null"},
		{src: Range[Date]{
			LowerType: Empty,
			UpperType: Empty,
			Valid:     true,
		}, result: `"empty"`},
		{src: Range[Date]{
			Lower:     Date{Time: time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Exclusive,
			UpperType: Exclusive,
			Valid:     true,
		}, result: `"(2022-12-01,2022-12-31)"`},
		{src: Range[Date]{
			Lower:     Date{Time: time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Exclusive,
			UpperType: Inclusive,
			Valid:     true,
		}, result: `"(2022-12-01,2022-12-31]"`},
		{src: Range[Date]{
			Lower:     Date{Time: time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Inclusive,
			UpperType: Exclusive,
			Valid:     true,
		}, result: `"[2022-12-01,2022-12-31)"`},
		{src: Range[Date]{
			Lower:     Date{Time: time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Inclusive,
			UpperType: Inclusive,
			Valid:     true,
		}, result: `"[2022-12-01,2022-12-31]"`},
		{src: Range[Date]{
			Upper:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Unbounded,
			UpperType: Exclusive,
			Valid:     true,
		}, result: `"(,2022-12-31)"`},
		{src: Range[Date]{
			Lower:     Date{Time: time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Inclusive,
			UpperType: Unbounded,
			Valid:     true,
		}, result: `"[2022-12-01,)"`},
		{src: Range[Date]{
			Lower:     Date{InfinityModifier: NegativeInfinity, Valid: true},
			Upper:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Exclusive,
			UpperType: Exclusive,
			Valid:     true,
		}, result: `"(-infinity,2022-12-31)"`},
		{src: Range[Date]{
			Lower:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     Date{InfinityModifier: Infinity, Valid: true},
			LowerType: Inclusive,
			UpperType: Exclusive,
			Valid:     true,
		}, result: `"[2022-12-31,infinity)"`},
	}

	for i, tt := range tests {
		r, err := tt.src.MarshalJSON()
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}

		if string(r) != tt.result {
			t.Errorf("%d: expected %v to encode to %v, got %v", i, tt.src, tt.result, string(r))
		}
	}
}

func TestRangeNumericMarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src    Range[Numeric]
		result string
	}{
		{src: Range[Numeric]{}, result: "null"},
		{src: Range[Numeric]{
			LowerType: Empty,
			UpperType: Empty,
			Valid:     true,
		}, result: `"empty"`},
		{src: Range[Numeric]{
			Lower:     Numeric{Int: big.NewInt(-16), Valid: true},
			Upper:     Numeric{Int: big.NewInt(16), Valid: true},
			LowerType: Exclusive,
			UpperType: Exclusive,
			Valid:     true,
		}, result: `"(-16,16)"`},
		{src: Range[Numeric]{
			Lower:     Numeric{Int: big.NewInt(-16), Valid: true},
			Upper:     Numeric{Int: big.NewInt(16), Valid: true},
			LowerType: Exclusive,
			UpperType: Inclusive,
			Valid:     true,
		}, result: `"(-16,16]"`},
		{src: Range[Numeric]{
			Lower:     Numeric{Int: big.NewInt(-16), Valid: true},
			Upper:     Numeric{Int: big.NewInt(16), Valid: true},
			LowerType: Inclusive,
			UpperType: Exclusive,
			Valid:     true,
		}, result: `"[-16,16)"`},
		{src: Range[Numeric]{
			Lower:     Numeric{Int: big.NewInt(-16), Valid: true},
			Upper:     Numeric{Int: big.NewInt(16), Valid: true},
			LowerType: Inclusive,
			UpperType: Inclusive,
			Valid:     true,
		}, result: `"[-16,16]"`},
		{src: Range[Numeric]{
			Upper:     Numeric{Int: big.NewInt(16), Valid: true},
			LowerType: Unbounded,
			UpperType: Exclusive,
			Valid:     true,
		}, result: `"(,16)"`},
		{src: Range[Numeric]{
			Lower:     Numeric{Int: big.NewInt(-16), Valid: true},
			LowerType: Inclusive,
			UpperType: Unbounded,
			Valid:     true,
		}, result: `"[-16,)"`},
		{src: Range[Numeric]{
			Lower:     Numeric{InfinityModifier: NegativeInfinity, NaN: true, Valid: true},
			Upper:     Numeric{Int: big.NewInt(16), Valid: true},
			LowerType: Exclusive,
			UpperType: Exclusive,
			Valid:     true,
		}, result: `"(-infinity,16)"`},
		{src: Range[Numeric]{
			Lower:     Numeric{Int: big.NewInt(-16), Valid: true},
			Upper:     Numeric{InfinityModifier: Infinity, NaN: true, Valid: true},
			LowerType: Inclusive,
			UpperType: Exclusive,
			Valid:     true,
		}, result: `"[-16,infinity)"`},
	}

	for i, tt := range tests {
		r, err := tt.src.MarshalJSON()
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}

		if string(r) != tt.result {
			t.Errorf("%d: expected %v to encode to %v, got %v", i, tt.src, tt.result, string(r))
		}
	}
}

func TestRangeDateUnmarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src    string
		result Range[Date]
	}{
		{src: "null", result: Range[Date]{}},
		{src: `"empty"`, result: Range[Date]{
			LowerType: Empty,
			UpperType: Empty,
			Valid:     true,
		}},
		{src: `"(2022-12-01,2022-12-31)"`, result: Range[Date]{
			Lower:     Date{Time: time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Exclusive,
			UpperType: Exclusive,
			Valid:     true,
		}},
		{src: `"(2022-12-01,2022-12-31]"`, result: Range[Date]{
			Lower:     Date{Time: time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Exclusive,
			UpperType: Inclusive,
			Valid:     true,
		}},
		{src: `"[2022-12-01,2022-12-31)"`, result: Range[Date]{
			Lower:     Date{Time: time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Inclusive,
			UpperType: Exclusive,
			Valid:     true,
		}},
		{src: `"[2022-12-01,2022-12-31]"`, result: Range[Date]{
			Lower:     Date{Time: time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Inclusive,
			UpperType: Inclusive,
			Valid:     true,
		}},
		{src: `"(,2022-12-31)"`, result: Range[Date]{
			Upper:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Unbounded,
			UpperType: Exclusive,
			Valid:     true,
		}},
		{src: `"[2022-12-01,)"`, result: Range[Date]{
			Lower:     Date{Time: time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Inclusive,
			UpperType: Unbounded,
			Valid:     true,
		}},
		{src: `"(-infinity,2022-12-31)"`, result: Range[Date]{
			Lower:     Date{InfinityModifier: NegativeInfinity, Valid: true},
			Upper:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			LowerType: Exclusive,
			UpperType: Exclusive,
			Valid:     true,
		}},
		{src: `"[2022-12-31,infinity)"`, result: Range[Date]{
			Lower:     Date{Time: time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true},
			Upper:     Date{InfinityModifier: Infinity, Valid: true},
			LowerType: Inclusive,
			UpperType: Exclusive,
			Valid:     true,
		}},
	}

	for i, tt := range tests {
		var r Range[Date]
		err := r.UnmarshalJSON([]byte(tt.src))
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}

		if r.Lower.Time.Year() != tt.result.Lower.Time.Year() ||
			r.Lower.Time.Month() != tt.result.Lower.Time.Month() ||
			r.Lower.Time.Day() != tt.result.Lower.Time.Day() ||
			r.Lower.InfinityModifier != tt.result.Lower.InfinityModifier ||
			r.LowerType != tt.result.LowerType ||
			r.Upper.Time.Year() != tt.result.Upper.Time.Year() ||
			r.Upper.Time.Month() != tt.result.Upper.Time.Month() ||
			r.Upper.Time.Day() != tt.result.Upper.Time.Day() ||
			r.Upper.InfinityModifier != tt.result.Upper.InfinityModifier ||
			r.UpperType != tt.result.UpperType ||
			r.Valid != tt.result.Valid {
			t.Errorf("%d: expected %v to decode to %v, got %v", i, tt.src, tt.result, r)
		}
	}
}

func TestRangeNumericUnmarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src    string
		result Range[Numeric]
	}{
		{src: "null", result: Range[Numeric]{}},
		{src: `"empty"`, result: Range[Numeric]{
			LowerType: Empty,
			UpperType: Empty,
			Valid:     true,
		}},
		{src: `"(-16,16)"`, result: Range[Numeric]{
			Lower:     Numeric{Int: big.NewInt(-16), Valid: true},
			Upper:     Numeric{Int: big.NewInt(16), Valid: true},
			LowerType: Exclusive,
			UpperType: Exclusive,
			Valid:     true,
		}},
		{src: `"(-16,16]"`, result: Range[Numeric]{
			Lower:     Numeric{Int: big.NewInt(-16), Valid: true},
			Upper:     Numeric{Int: big.NewInt(16), Valid: true},
			LowerType: Exclusive,
			UpperType: Inclusive,
			Valid:     true,
		}},
		{src: `"[-16,16)"`, result: Range[Numeric]{
			Lower:     Numeric{Int: big.NewInt(-16), Valid: true},
			Upper:     Numeric{Int: big.NewInt(16), Valid: true},
			LowerType: Inclusive,
			UpperType: Exclusive,
			Valid:     true,
		}},
		{src: `"[-16,16]"`, result: Range[Numeric]{
			Lower:     Numeric{Int: big.NewInt(-16), Valid: true},
			Upper:     Numeric{Int: big.NewInt(16), Valid: true},
			LowerType: Inclusive,
			UpperType: Inclusive,
			Valid:     true,
		}},
		{src: `"(,16)"`, result: Range[Numeric]{
			Upper:     Numeric{Int: big.NewInt(16), Valid: true},
			LowerType: Unbounded,
			UpperType: Exclusive,
			Valid:     true,
		}},
		{src: `"[-16,)"`, result: Range[Numeric]{
			Lower:     Numeric{Int: big.NewInt(-16), Valid: true},
			LowerType: Inclusive,
			UpperType: Unbounded,
			Valid:     true,
		}},
		{src: `"(-infinity,16)"`, result: Range[Numeric]{
			Lower:     Numeric{InfinityModifier: NegativeInfinity, NaN: true, Valid: true},
			Upper:     Numeric{Int: big.NewInt(16), Valid: true},
			LowerType: Exclusive,
			UpperType: Exclusive,
			Valid:     true,
		}},
		{src: `"[-16,infinity)"`, result: Range[Numeric]{
			Lower:     Numeric{Int: big.NewInt(-16), Valid: true},
			Upper:     Numeric{InfinityModifier: Infinity, NaN: true, Valid: true},
			LowerType: Inclusive,
			UpperType: Exclusive,
			Valid:     true,
		}},
	}

	for i, tt := range tests {
		var r Range[Numeric]
		err := r.UnmarshalJSON([]byte(tt.src))
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}

		if r.Lower.Int.Cmp(tt.result.Lower.Int) != 0 ||
			r.Lower.InfinityModifier != tt.result.Lower.InfinityModifier ||
			r.LowerType != tt.result.LowerType ||
			r.Upper.Int.Cmp(tt.result.Upper.Int) != 0 ||
			r.Upper.InfinityModifier != tt.result.Upper.InfinityModifier ||
			r.UpperType != tt.result.UpperType ||
			r.Valid != r.Valid {
			t.Errorf("%d: expected %s to decode to %v, got %v", i, tt.src, tt.result, r)
		}
	}
}
