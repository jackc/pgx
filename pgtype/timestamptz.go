package pgtype

import (
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/pgio"
)

const pgTimestamptzFormat = "2006-01-02 15:04:05.999999999Z07:00"
const microsecFromUnixEpochToY2K = 946684800 * 1000000

type Timestamptz struct {
	// time.Time is embedded to handle Infinity and -Infinity
	// TODO - infinity
	t time.Time
}

func (t *Timestamptz) DecodeText(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size == -1 {
		return fmt.Errorf("invalid length for timestamptz: %v", size)
	}

	buf := make([]byte, int(size))
	_, err = r.Read(buf)
	if err != nil {
		return err
	}

	t.t, err = time.Parse(pgTimestamptzFormat, string(buf))
	if err != nil {
		return err
	}

	return nil
}

func (t *Timestamptz) DecodeBinary(r io.Reader) error {
	size, err := pgio.ReadInt32(r)
	if err != nil {
		return err
	}

	if size != 8 {
		return fmt.Errorf("invalid length for timestamptz: %v", size)
	}

	microsecSinceY2K, err := pgio.ReadInt64(r)
	if err != nil {
		return err
	}

	microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
	t.t = time.Unix(microsecSinceUnixEpoch/1000000, (microsecSinceUnixEpoch%1000000)*1000)

	return nil
}

func (t Timestamptz) EncodeText(w io.Writer) error {
	buf := []byte(t.t.Format(pgTimestamptzFormat))

	_, err := pgio.WriteInt32(w, int32(len(buf)))
	if err != nil {
		return nil
	}

	_, err = w.Write(buf)
	return err
}

func (t Timestamptz) EncodeBinary(w io.Writer) error {
	_, err := pgio.WriteInt32(w, 8)
	if err != nil {
		return err
	}

	microsecSinceUnixEpoch := t.t.Unix()*1000000 + int64(t.t.Nanosecond())/1000
	microsecSinceY2K := microsecSinceUnixEpoch - microsecFromUnixEpochToY2K

	_, err = pgio.WriteInt64(w, microsecSinceY2K)
	return err
}

func (t Timestamptz) Time() time.Time {
	return t.t
}

func TimestamptzFromTime(t time.Time) Timestamptz {
	return Timestamptz{t: t}
}
