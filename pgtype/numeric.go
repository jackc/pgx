package pgtype

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"

	"github.com/jackc/pgx/pgio"
)

// PostgreSQL internal numeric storage uses 16-bit "digits" with base of 10,000
const nbase = 10000

var big0 *big.Int = big.NewInt(0)
var big10 *big.Int = big.NewInt(10)
var big100 *big.Int = big.NewInt(100)
var big1000 *big.Int = big.NewInt(1000)

var bigNBase *big.Int = big.NewInt(nbase)
var bigNBaseX2 *big.Int = big.NewInt(nbase * nbase)
var bigNBaseX3 *big.Int = big.NewInt(nbase * nbase * nbase)
var bigNBaseX4 *big.Int = big.NewInt(nbase * nbase * nbase * nbase)

type Numeric struct {
	Int    *big.Int
	Exp    int32
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
	digits := strings.Join(parts, "")

	accum := &big.Int{}
	if _, ok := accum.SetString(digits, 10); !ok {
		return fmt.Errorf("%s is not a number", str)
	}

	var exp int32
	if len(parts) > 1 {
		exp = int32(-len(parts[1]))
	} else {
		for i := len(digits) - 1; i > 0; i-- {
			if digits[i] == '0' {
				accum.Div(accum, big10)
				exp++
			} else {
				break
			}
		}
	}

	*dst = Numeric{Int: accum, Exp: exp, Status: Present}
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

	rp := 0
	ndigits := int16(binary.BigEndian.Uint16(src[rp:]))
	rp += 2

	if ndigits == 0 {
		*dst = Numeric{Int: big.NewInt(0), Status: Present}
		return nil
	}

	weight := int16(binary.BigEndian.Uint16(src[rp:]))
	rp += 2
	sign := int16(binary.BigEndian.Uint16(src[rp:]))
	rp += 2
	dscale := int16(binary.BigEndian.Uint16(src[rp:]))
	rp += 2

	if len(src[rp:]) < int(ndigits)*2 {
		return fmt.Errorf("numeric incomplete %v", src)
	}

	accum := &big.Int{}

	for i := 0; i < int(ndigits+3)/4; i++ {
		int64accum, bytesRead, digitsRead := nbaseDigitsToInt64(src[rp:])
		rp += bytesRead

		if i > 0 {
			var mul *big.Int
			switch digitsRead {
			case 1:
				mul = bigNBase
			case 2:
				mul = bigNBaseX2
			case 3:
				mul = bigNBaseX3
			case 4:
				mul = bigNBaseX4
			default:
				return fmt.Errorf("invalid digitsRead: %d (this can't happen)", digitsRead)
			}
			accum.Mul(accum, mul)
		}

		accum.Add(accum, big.NewInt(int64accum))
	}

	exp := (int32(weight) - int32(ndigits) + 1) * 4

	if dscale > 0 {
		fracNBaseDigits := ndigits - weight - 1
		fracDecimalDigits := fracNBaseDigits * 4

		if dscale > fracDecimalDigits {
			multCount := int(dscale - fracDecimalDigits)
			for i := 0; i < multCount; i++ {
				accum.Mul(accum, big10)
				exp--
			}
		} else if dscale < fracDecimalDigits {
			divCount := int(fracDecimalDigits - dscale)
			for i := 0; i < divCount; i++ {
				accum.Div(accum, big10)
				exp++
			}
		}
	}

	reduced := &big.Int{}
	remainder := &big.Int{}
	for exp > 0 {
		reduced.DivMod(accum, big10, remainder)
		if remainder.Cmp(big0) != 0 {
			break
		}
		accum.Set(reduced)
		exp++
	}

	if sign != 0 {
		accum.Neg(accum)
	}

	*dst = Numeric{Int: accum, Exp: exp, Status: Present}

	return nil

}

func nbaseDigitsToInt64(src []byte) (accum int64, bytesRead, digitsRead int) {
	digits := len(src) / 2
	if digits > 4 {
		digits = 4
	}

	rp := 0

	for i := 0; i < digits; i++ {
		if i > 0 {
			accum *= nbase
		}
		accum += int64(binary.BigEndian.Uint16(src[rp:]))
		rp += 2
	}

	return accum, rp, digits
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

	var sign int16
	if src.Int.Cmp(big0) < 0 {
		sign = 16384
	}

	absInt := &big.Int{}
	wholePart := &big.Int{}
	fracPart := &big.Int{}
	remainder := &big.Int{}
	absInt.Abs(src.Int)

	// Normalize absInt and exp to where exp is always a multiple of 4. This makes
	// converting to 16-bit base 10,000 digits easier.
	var exp int32
	switch src.Exp % 4 {
	case 1, -3:
		exp = src.Exp - 1
		absInt.Mul(absInt, big10)
	case 2, -2:
		exp = src.Exp - 2
		absInt.Mul(absInt, big100)
	case 3, -1:
		exp = src.Exp - 3
		absInt.Mul(absInt, big1000)
	default:
		exp = src.Exp
	}

	if exp < 0 {
		divisor := &big.Int{}
		divisor.Exp(big10, big.NewInt(int64(-exp)), nil)
		wholePart.DivMod(absInt, divisor, fracPart)
	} else {
		wholePart = absInt
	}

	var wholeDigits, fracDigits []int16

	for wholePart.Cmp(big0) != 0 {
		wholePart.DivMod(wholePart, bigNBase, remainder)
		wholeDigits = append(wholeDigits, int16(remainder.Int64()))
	}

	for fracPart.Cmp(big0) != 0 {
		fracPart.DivMod(fracPart, bigNBase, remainder)
		fracDigits = append(fracDigits, int16(remainder.Int64()))
	}

	if _, err := pgio.WriteInt16(w, int16(len(wholeDigits)+len(fracDigits))); err != nil {
		return false, err
	}

	var weight int16
	if len(wholeDigits) > 0 {
		weight = int16(len(wholeDigits) - 1)
		if exp > 0 {
			weight += int16(exp / 4)
		}
	} else {
		weight = int16(exp/4) - 1 + int16(len(fracDigits))
	}
	if _, err := pgio.WriteInt16(w, weight); err != nil {
		return false, err
	}

	if _, err := pgio.WriteInt16(w, sign); err != nil {
		return false, err
	}

	var dscale int16
	if src.Exp < 0 {
		dscale = int16(-src.Exp)
	}
	if _, err := pgio.WriteInt16(w, dscale); err != nil {
		return false, err
	}

	for i := len(wholeDigits) - 1; i >= 0; i-- {
		if _, err := pgio.WriteInt16(w, wholeDigits[i]); err != nil {
			return false, err
		}
	}

	for i := len(fracDigits) - 1; i >= 0; i-- {
		if _, err := pgio.WriteInt16(w, fracDigits[i]); err != nil {
			return false, err
		}
	}

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

	return fmt.Errorf("cannot scan %T", src)
}

// Value implements the database/sql/driver Valuer interface.
func (src Numeric) Value() (driver.Value, error) {
	switch src.Status {
	case Present:
		buf := &bytes.Buffer{}
		_, err := src.EncodeText(nil, buf)
		if err != nil {
			return nil, err
		}

		return buf.String(), nil
	case Null:
		return nil, nil
	default:
		return nil, errUndefined
	}
}
