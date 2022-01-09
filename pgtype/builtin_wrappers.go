package pgtype

import (
	"fmt"
	"math"
	"strconv"
)

type int8Wrapper int8

func (n *int8Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *int8")
	}

	if v.Int < math.MinInt8 {
		return fmt.Errorf("%d is less than minimum value for int8", v.Int)
	}
	if v.Int > math.MaxInt8 {
		return fmt.Errorf("%d is greater than maximum value for int8", v.Int)
	}
	*n = int8Wrapper(v.Int)

	return nil
}

func (n int8Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(n), Valid: true}, nil
}

type int16Wrapper int16

func (n *int16Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *int16")
	}

	if v.Int < math.MinInt16 {
		return fmt.Errorf("%d is less than minimum value for int16", v.Int)
	}
	if v.Int > math.MaxInt16 {
		return fmt.Errorf("%d is greater than maximum value for int16", v.Int)
	}
	*n = int16Wrapper(v.Int)

	return nil
}

func (n int16Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(n), Valid: true}, nil
}

type int32Wrapper int32

func (n *int32Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *int32")
	}

	if v.Int < math.MinInt32 {
		return fmt.Errorf("%d is less than minimum value for int32", v.Int)
	}
	if v.Int > math.MaxInt32 {
		return fmt.Errorf("%d is greater than maximum value for int32", v.Int)
	}
	*n = int32Wrapper(v.Int)

	return nil
}

func (n int32Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(n), Valid: true}, nil
}

type int64Wrapper int64

func (n *int64Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *int64")
	}

	*n = int64Wrapper(v.Int)

	return nil
}

func (n int64Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(n), Valid: true}, nil
}

type intWrapper int

func (n *intWrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *int")
	}

	if v.Int < math.MinInt {
		return fmt.Errorf("%d is less than minimum value for int", v.Int)
	}
	if v.Int > math.MaxInt {
		return fmt.Errorf("%d is greater than maximum value for int", v.Int)
	}

	*n = intWrapper(v.Int)

	return nil
}

func (n intWrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(n), Valid: true}, nil
}

type uint8Wrapper uint8

func (n *uint8Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *uint8")
	}

	if v.Int < 0 {
		return fmt.Errorf("%d is less than minimum value for uint8", v.Int)
	}
	if v.Int > math.MaxUint8 {
		return fmt.Errorf("%d is greater than maximum value for uint8", v.Int)
	}
	*n = uint8Wrapper(v.Int)

	return nil
}

func (n uint8Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(n), Valid: true}, nil
}

type uint16Wrapper uint16

func (n *uint16Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *uint16")
	}

	if v.Int < 0 {
		return fmt.Errorf("%d is less than minimum value for uint16", v.Int)
	}
	if v.Int > math.MaxUint16 {
		return fmt.Errorf("%d is greater than maximum value for uint16", v.Int)
	}
	*n = uint16Wrapper(v.Int)

	return nil
}

func (n uint16Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(n), Valid: true}, nil
}

type uint32Wrapper uint32

func (n *uint32Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *uint32")
	}

	if v.Int < 0 {
		return fmt.Errorf("%d is less than minimum value for uint32", v.Int)
	}
	if v.Int > math.MaxUint32 {
		return fmt.Errorf("%d is greater than maximum value for uint32", v.Int)
	}
	*n = uint32Wrapper(v.Int)

	return nil
}

func (n uint32Wrapper) Int64Value() (Int8, error) {
	return Int8{Int: int64(n), Valid: true}, nil
}

type uint64Wrapper uint64

func (n *uint64Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *uint64")
	}

	if v.Int < 0 {
		return fmt.Errorf("%d is less than minimum value for uint64", v.Int)
	}

	*n = uint64Wrapper(v.Int)

	return nil
}

func (n uint64Wrapper) Int64Value() (Int8, error) {
	if uint64(n) > uint64(math.MaxInt64) {
		return Int8{}, fmt.Errorf("%d is greater than maximum value for int64", n)
	}

	return Int8{Int: int64(n), Valid: true}, nil
}

type uintWrapper uint

func (n *uintWrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *uint64")
	}

	if v.Int < 0 {
		return fmt.Errorf("%d is less than minimum value for uint64", v.Int)
	}

	if uint64(v.Int) > math.MaxUint {
		return fmt.Errorf("%d is greater than maximum value for uint", v.Int)
	}

	*n = uintWrapper(v.Int)

	return nil
}

func (n uintWrapper) Int64Value() (Int8, error) {
	if uint64(n) > uint64(math.MaxInt64) {
		return Int8{}, fmt.Errorf("%d is greater than maximum value for int64", n)
	}

	return Int8{Int: int64(n), Valid: true}, nil
}

type float32Wrapper float32

func (n *float32Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *float32")
	}

	*n = float32Wrapper(v.Int)

	return nil
}

func (n float32Wrapper) Int64Value() (Int8, error) {
	if n > math.MaxInt64 {
		return Int8{}, fmt.Errorf("%f is greater than maximum value for int64", n)
	}

	return Int8{Int: int64(n), Valid: true}, nil
}

type float64Wrapper float64

func (n *float64Wrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *float64")
	}

	*n = float64Wrapper(v.Int)

	return nil
}

func (n float64Wrapper) Int64Value() (Int8, error) {
	if n > math.MaxInt64 {
		return Int8{}, fmt.Errorf("%f is greater than maximum value for int64", n)
	}

	return Int8{Int: int64(n), Valid: true}, nil
}

type stringWrapper string

func (s *stringWrapper) ScanInt64(v Int8) error {
	if !v.Valid {
		return fmt.Errorf("cannot scan NULL into *string")
	}

	*s = stringWrapper(strconv.FormatInt(v.Int, 10))

	return nil
}

func (s stringWrapper) Int64Value() (Int8, error) {
	num, err := strconv.ParseInt(string(s), 10, 64)
	if err != nil {
		return Int8{}, err
	}

	return Int8{Int: int64(num), Valid: true}, nil
}
