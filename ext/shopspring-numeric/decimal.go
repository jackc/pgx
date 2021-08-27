package numeric

import (
	"database/sql/driver"
	"fmt"
	"strconv"

	"github.com/jackc/pgtype"
	"github.com/shopspring/decimal"
)

type Numeric struct {
	Decimal decimal.Decimal
	Valid   bool
}

func (dst *Numeric) Set(src interface{}) error {
	if src == nil {
		*dst = Numeric{}
		return nil
	}

	if value, ok := src.(interface{ Get() interface{} }); ok {
		value2 := value.Get()
		if value2 != value {
			return dst.Set(value2)
		}
	}

	switch value := src.(type) {
	case decimal.Decimal:
		*dst = Numeric{Decimal: value, Valid: true}
	case decimal.NullDecimal:
		if value.Valid {
			*dst = Numeric{Decimal: value.Decimal, Valid: true}
		} else {
			*dst = Numeric{}
		}
	case float32:
		*dst = Numeric{Decimal: decimal.NewFromFloat(float64(value)), Valid: true}
	case float64:
		*dst = Numeric{Decimal: decimal.NewFromFloat(value), Valid: true}
	case int8:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Valid: true}
	case uint8:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Valid: true}
	case int16:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Valid: true}
	case uint16:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Valid: true}
	case int32:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Valid: true}
	case uint32:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Valid: true}
	case int64:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Valid: true}
	case uint64:
		// uint64 could be greater than int64 so convert to string then to decimal
		dec, err := decimal.NewFromString(strconv.FormatUint(value, 10))
		if err != nil {
			return err
		}
		*dst = Numeric{Decimal: dec, Valid: true}
	case int:
		*dst = Numeric{Decimal: decimal.New(int64(value), 0), Valid: true}
	case uint:
		// uint could be greater than int64 so convert to string then to decimal
		dec, err := decimal.NewFromString(strconv.FormatUint(uint64(value), 10))
		if err != nil {
			return err
		}
		*dst = Numeric{Decimal: dec, Valid: true}
	case string:
		dec, err := decimal.NewFromString(value)
		if err != nil {
			return err
		}
		*dst = Numeric{Decimal: dec, Valid: true}
	default:
		// If all else fails see if pgtype.Numeric can handle it. If so, translate through that.
		num := &pgtype.Numeric{}
		if err := num.Set(value); err != nil {
			return fmt.Errorf("cannot convert %v to Numeric", value)
		}

		buf, err := num.EncodeText(nil, nil)
		if err != nil {
			return fmt.Errorf("cannot convert %v to Numeric", value)
		}

		dec, err := decimal.NewFromString(string(buf))
		if err != nil {
			return fmt.Errorf("cannot convert %v to Numeric", value)
		}
		*dst = Numeric{Decimal: dec, Valid: true}
	}

	return nil
}

func (dst Numeric) Get() interface{} {
	if !dst.Valid {
		return nil
	}
	return dst.Decimal
}

func (src *Numeric) AssignTo(dst interface{}) error {
	if !src.Valid {
		if v, ok := dst.(*decimal.NullDecimal); ok {
			(*v).Valid = false
			(*v).Decimal = src.Decimal
			return nil
		}
		return pgtype.NullAssignTo(dst)
	}

	switch v := dst.(type) {
	case *decimal.Decimal:
		*v = src.Decimal
	case *decimal.NullDecimal:
		(*v).Valid = true
		(*v).Decimal = src.Decimal
	case *float32:
		f, _ := src.Decimal.Float64()
		*v = float32(f)
	case *float64:
		f, _ := src.Decimal.Float64()
		*v = f
	case *int:
		if src.Decimal.Exponent() < 0 {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		n, err := strconv.ParseInt(src.Decimal.String(), 10, strconv.IntSize)
		if err != nil {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		*v = int(n)
	case *int8:
		if src.Decimal.Exponent() < 0 {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		n, err := strconv.ParseInt(src.Decimal.String(), 10, 8)
		if err != nil {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		*v = int8(n)
	case *int16:
		if src.Decimal.Exponent() < 0 {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		n, err := strconv.ParseInt(src.Decimal.String(), 10, 16)
		if err != nil {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		*v = int16(n)
	case *int32:
		if src.Decimal.Exponent() < 0 {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		n, err := strconv.ParseInt(src.Decimal.String(), 10, 32)
		if err != nil {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		*v = int32(n)
	case *int64:
		if src.Decimal.Exponent() < 0 {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		n, err := strconv.ParseInt(src.Decimal.String(), 10, 64)
		if err != nil {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		*v = int64(n)
	case *uint:
		if src.Decimal.Exponent() < 0 || src.Decimal.Sign() < 0 {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		n, err := strconv.ParseUint(src.Decimal.String(), 10, strconv.IntSize)
		if err != nil {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		*v = uint(n)
	case *uint8:
		if src.Decimal.Exponent() < 0 || src.Decimal.Sign() < 0 {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		n, err := strconv.ParseUint(src.Decimal.String(), 10, 8)
		if err != nil {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		*v = uint8(n)
	case *uint16:
		if src.Decimal.Exponent() < 0 || src.Decimal.Sign() < 0 {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		n, err := strconv.ParseUint(src.Decimal.String(), 10, 16)
		if err != nil {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		*v = uint16(n)
	case *uint32:
		if src.Decimal.Exponent() < 0 || src.Decimal.Sign() < 0 {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		n, err := strconv.ParseUint(src.Decimal.String(), 10, 32)
		if err != nil {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		*v = uint32(n)
	case *uint64:
		if src.Decimal.Exponent() < 0 || src.Decimal.Sign() < 0 {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		n, err := strconv.ParseUint(src.Decimal.String(), 10, 64)
		if err != nil {
			return fmt.Errorf("cannot convert %v to %T", dst, *v)
		}
		*v = uint64(n)
	default:
		if nextDst, retry := pgtype.GetAssignToDstType(dst); retry {
			return src.AssignTo(nextDst)
		}
		return fmt.Errorf("unable to assign to %T", dst)
	}

	return nil
}

func (dst *Numeric) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = Numeric{}
		return nil
	}

	dec, err := decimal.NewFromString(string(src))
	if err != nil {
		return err
	}

	*dst = Numeric{Decimal: dec, Valid: true}
	return nil
}

func (dst *Numeric) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = Numeric{}
		return nil
	}

	// For now at least, implement this in terms of pgtype.Numeric

	num := &pgtype.Numeric{}
	if err := num.DecodeBinary(ci, src); err != nil {
		return err
	}

	*dst = Numeric{Decimal: decimal.NewFromBigInt(num.Int, num.Exp), Valid: true}

	return nil
}

func (src Numeric) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}
	return append(buf, src.Decimal.String()...), nil
}

func (src Numeric) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if !src.Valid {
		return nil, nil
	}

	// For now at least, implement this in terms of pgtype.Numeric
	num := &pgtype.Numeric{}
	if err := num.DecodeText(ci, []byte(src.Decimal.String())); err != nil {
		return nil, err
	}

	return num.EncodeBinary(ci, buf)
}

// Scan implements the database/sql Scanner interface.
func (dst *Numeric) Scan(src interface{}) error {
	if src == nil {
		*dst = Numeric{}
		return nil
	}

	switch src := src.(type) {
	case float64:
		*dst = Numeric{Decimal: decimal.NewFromFloat(src), Valid: true}
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
	if !src.Valid {
		return nil, nil
	}
	return src.Decimal.Value()
}

func (src Numeric) MarshalJSON() ([]byte, error) {
	if !src.Valid {
		return []byte("null"), nil
	}
	return src.Decimal.MarshalJSON()
}

func (dst *Numeric) UnmarshalJSON(b []byte) error {
	d := decimal.NullDecimal{}
	err := d.UnmarshalJSON(b)
	if err != nil {
		return err
	}

	*dst = Numeric{Decimal: d.Decimal, Valid: d.Valid}

	return nil
}
