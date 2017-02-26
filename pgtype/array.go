package pgtype

import (
	"io"

	"github.com/jackc/pgx/pgio"
)

// Information on the internals of PostgreSQL arrays can be found in
// src/include/utils/array.h and src/backend/utils/adt/arrayfuncs.c. Of
// particular interest is the array_send function.

type ArrayHeader struct {
	ContainsNull bool
	ElementOID   int32
	Dimensions   []ArrayDimension
}

type ArrayDimension struct {
	Length     int32
	LowerBound int32
}

func (ah *ArrayHeader) DecodeBinary(r io.Reader) error {
	numDims, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if numDims > 0 {
		ah.Dimensions = make([]ArrayDimension, numDims)
	}

	containsNull, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}
	ah.ContainsNull = containsNull == 1

	ah.ElementOID, err = pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	for i := range ah.Dimensions {
		ah.Dimensions[i].Length, err = pgio.ReadInt32(r)
		if err != nil {
			return err
		}

		ah.Dimensions[i].LowerBound, err = pgio.ReadInt32(r)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ah *ArrayHeader) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, int32(len(ah.Dimensions)))
	if err != nil {
		return err
	}

	var containsNull int32
	if ah.ContainsNull {
		containsNull = 1
	}
	_, err = pgio.WriteInt32(w, containsNull)
	if err != nil {
		return err
	}

	_, err = pgio.WriteInt32(w, ah.ElementOID)
	if err != nil {
		return err
	}

	for i := range ah.Dimensions {
		_, err = pgio.WriteInt32(w, ah.Dimensions[i].Length)
		if err != nil {
			return err
		}

		_, err = pgio.WriteInt32(w, ah.Dimensions[i].LowerBound)
		if err != nil {
			return err
		}
	}

	return nil
}
