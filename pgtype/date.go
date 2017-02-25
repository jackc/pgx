package pgtype

import (
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/pgio"
)

type Date struct {
	// TODO handling Infinity and -Infinity
	Time   time.Time
	Status Status
}

func (d *Date) ConvertFrom(src interface{}) error {
	switch value := src.(type) {
	case Date:
		*d = value
	case time.Time:
		*d = Date{Time: value}
	default:
		if originalSrc, ok := underlyingTimeType(src); ok {
			return d.ConvertFrom(originalSrc)
		}
		return fmt.Errorf("cannot convert %v to Date", value)
	}

	return nil
}

func (d *Date) AssignTo(dst interface{}) error {
	return nil
}

func (d *Date) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*d = Date{Status: Null}
		return nil
	}

	buf := make([]byte, int(size))
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	t, err := time.ParseInLocation("2006-01-02", string(buf), time.UTC)
	if err != nil {
		return err
	}

	*d = Date{Time: t}

	return nil
}

func (d *Date) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		*d = Date{Status: Null}
		return nil
	}

	if size != 4 {
		return fmt.Errorf("invalid length for date: %v", size)
	}

	dayOffset, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	t := time.Date(2000, 1, int(1+dayOffset), 0, 0, 0, 0, time.UTC)

	*d = Date{Time: t}

	return nil
}

func (d Date) EncodeText(w io.Writer) error {
	if done, err := encodeNotPresent(w, d.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, 10)
	if err != nil {
		return nil
	}

	_, err = w.Write([]byte(d.Time.Format("2006-01-02")))
	return err
}

func (d Date) EncodeBinary(w io.Writer) error {
	if done, err := encodeNotPresent(w, d.Status); done {
		return err
	}

	_, err := pgio.WriteInt32(w, 4)
	if err != nil {
		return err
	}

	tUnix := time.Date(d.Time.Year(), d.Time.Month(), d.Time.Day(), 0, 0, 0, 0, time.UTC).Unix()
	dateEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	secSinceDateEpoch := tUnix - dateEpoch
	daysSinceDateEpoch := secSinceDateEpoch / 86400

	_, err = pgio.WriteInt32(w, int32(daysSinceDateEpoch))
	return err
}
