package pgtype

import (
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/pgio"
)

type Date struct {
	// time.Time is embedded to hide internal implementation. Possibly do date
	// implementation at some point rather than simply delegating to time.Time.
	t time.Time
}

func (d *Date) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		return fmt.Errorf("invalid length for int8: %v", size)
	}

	buf := make([]byte, int(size))
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	d.t, err = time.Parse("2006-01-02", string(buf))
	if err != nil {
		return err
	}

	return nil
}

func (d *Date) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size != 4 {
		return fmt.Errorf("invalid length for date: %v", size)
	}

	dayOffset, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	d.t = time.Date(2000, 1, int(1+dayOffset), 0, 0, 0, 0, time.Local)

	return nil
}

func (d Date) EncodeText(w io.Writer) error {
	_, err := pgio.WriteInt32(w, 10)
	if err != nil {
		return nil
	}

	_, err = w.Write([]byte(d.t.Format("2006-01-02")))
	return err
}

func (d Date) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, 4)
	if err != nil {
		return err
	}

	tUnix := time.Date(d.t.Year(), d.t.Month(), d.t.Day(), 0, 0, 0, 0, time.UTC).Unix()
	dateEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	secSinceDateEpoch := tUnix - dateEpoch
	daysSinceDateEpoch := secSinceDateEpoch / 86400

	_, err = pgio.WriteInt32(w, int32(daysSinceDateEpoch))
	return err
}

func (d Date) Time() time.Time {
	return d.t
}

func DateFromTime(t time.Time) Date {
	return Date{t: t}
}
