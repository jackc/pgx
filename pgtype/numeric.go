package pgtype

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"

	"github.com/jackc/pgx/pgio"
)

type Numeric struct {
	Int    big.Int
	Exp    int16
	Status Status
}

func (dst *Numeric) Set(src interface{}) error {
	if src == nil {
		*dst = Numeric{Status: Null}
		return nil
	}

	return fmt.Errorf("todo")
}

func (dst *Numeric) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Numeric) AssignTo(dst interface{}) error {
	return fmt.Errorf("todo")
}

func (dst *Numeric) DecodeText(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Numeric{Status: Null}
		return nil
	}

	str := string(src)
	parts := strings.SplitN(str, ".", 2)

	var i big.Int
	if _, ok := i.SetString(strings.Join(parts, ""), 10); !ok {
		return fmt.Errorf("%s is not a number", str)
	}

	var e int16
	if len(parts) > 1 {
		e = int16(-len(parts[1]))
	}

	*dst = Numeric{Int: i, Exp: e, Status: Present}
	return nil
}

func (dst *Numeric) DecodeBinary(ci *ConnInfo, src []byte) error {
	if src == nil {
		*dst = Numeric{Status: Null}
		return nil
	}

	if len(src) < 8 {
		return fmt.Errorf("numeric incomplete %v", src)
	}

	fmt.Println("DECODE", src)

	rp := 0
	ndigits := int16(binary.BigEndian.Uint16(src[rp:]))
	rp += 2

	if ndigits == 0 {
		*dst = Numeric{Status: Present}
		return nil
	}

	// weight := int16(binary.BigEndian.Uint16(src[rp:]))
	rp += 2
	// sign := int16(binary.BigEndian.Uint16(src[rp:]))
	rp += 2
	// dscale := int16(binary.BigEndian.Uint16(src[rp:]))
	rp += 2

	if len(src[rp:]) < int(ndigits)*2 {
		return fmt.Errorf("numeric incomplete %v", src)
	}

	i64accum := int64(binary.BigEndian.Uint16(src[rp:]))
	rp += 2

	if ndigits > 1 {
		int64Digits := int64(ndigits - 1)
		if int64Digits > 3 {
			int64Digits = 3
		}

		for i := 0; i < int(int64Digits); i++ {
			i64accum *= 10000
			i64accum += int64(binary.BigEndian.Uint16(src[rp:]))
			rp += 2
		}
	}

	accum := big.NewInt(i64accum)

	// digitMult := big.NewInt(10000)

	// Still need to handle really big numbers

	*dst = Numeric{Int: *accum, Exp: 0, Status: Present}

	return nil

}

func (src Numeric) EncodeText(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	if _, err := io.WriteString(w, src.Int.String()); err != nil {
		return false, err
	}

	if err := pgio.WriteByte(w, 'e'); err != nil {
		return false, err
	}

	if _, err := io.WriteString(w, strconv.FormatInt(int64(src.Exp), 10)); err != nil {
		return false, err
	}

	return false, nil

}

func (src Numeric) EncodeBinary(ci *ConnInfo, w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	digitSize := big.NewInt(10000)
	num := src.Int
	var remainder big.Int
	digits := make([]int16, 0, 16)

	for num.Cmp(digitSize) >= 0 {
		num.DivMod(&num, digitSize, &remainder)
		digits = append(digits, int16(remainder.Int64()))
	}

	fmt.Println("ENCODE", digits)

	if _, err := pgio.WriteInt16(w, int16(len(digits))); err != nil {
		return false, err
	}

	// 0 weight for now
	if _, err := pgio.WriteInt16(w, int16(0)); err != nil {
		return false, err
	}

	// 0 sign for now
	if _, err := pgio.WriteInt16(w, int16(0)); err != nil {
		return false, err
	}

	// 0 dscale for now
	if _, err := pgio.WriteInt16(w, int16(0)); err != nil {
		return false, err
	}

	for i := len(digits); i > 0; i-- {
		if _, err := pgio.WriteInt16(w, digits[i]); err != nil {
			return false, err
		}
	}

	// pq_begintypsend(&buf);

	// pq_sendint(&buf, x.ndigits, sizeof(int16));
	// pq_sendint(&buf, x.weight, sizeof(int16));
	// pq_sendint(&buf, x.sign, sizeof(int16));
	// pq_sendint(&buf, x.dscale, sizeof(int16));
	// for (i = 0; i < x.ndigits; i++)
	// 	pq_sendint(&buf, x.digits[i], sizeof(NumericDigit));

	// PG_RETURN_BYTEA_P(pq_endtypsend(&buf));

	return false, nil

}

// Scan implements the database/sql Scanner interface.
func (dst *Numeric) Scan(src interface{}) error {
	if src == nil {
		*dst = Numeric{Status: Null}
		return nil
	}

	switch src := src.(type) {
	case float64:
		// TODO
		// *dst = Numeric{Float: src, Status: Present}
		return nil
	case string:
		return dst.DecodeText(nil, []byte(src))
	case []byte:
		return dst.DecodeText(nil, src)
	}

	return fmt.Errorf("todo")

}

// Value implements the database/sql/driver Valuer interface.
func (src Numeric) Value() (driver.Value, error) {
	switch src.Status {
	case Present:
		// TODO
		return nil, nil
	case Null:
		return nil, nil
	default:
		return nil, errUndefined
	}
}
