package pgtype

import (
	"errors"
	"io"

	"github.com/jackc/pgx/pgio"
)

type Status byte

const (
	Undefined Status = iota
	Null
	Present
)

type InfinityModifier int8

const (
	Infinity         InfinityModifier = 1
	None             InfinityModifier = 0
	NegativeInfinity InfinityModifier = -Infinity
)

type Value interface {
	ConvertFrom(src interface{}) error
	AssignTo(dst interface{}) error
}

type BinaryDecoder interface {
	DecodeBinary(r io.Reader) error
}

type TextDecoder interface {
	DecodeText(r io.Reader) error
}

type BinaryEncoder interface {
	EncodeBinary(w io.Writer) error
}

type TextEncoder interface {
	EncodeText(w io.Writer) error
}

var errUndefined = errors.New("cannot encode status undefined")

func encodeNotPresent(w io.Writer, status Status) (done bool, err error) {
	switch status {
	case Undefined:
		return true, errUndefined
	case Null:
		_, err = pgio.WriteInt32(w, -1)
		return true, err
	}
	return false, nil
}
