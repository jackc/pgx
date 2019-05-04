package pgx

import (
	"database/sql/driver"

	"github.com/jackc/pgio"
	"github.com/jackc/pgtype"
)

const (
	copyData = 'd'
	copyFail = 'f'
	copyDone = 'c'
)

func convertDriverValuers(args []interface{}) ([]interface{}, error) {
	for i, arg := range args {
		switch arg := arg.(type) {
		case pgtype.BinaryEncoder:
		case pgtype.TextEncoder:
		case driver.Valuer:
			v, err := callValuerValue(arg)
			if err != nil {
				return nil, err
			}
			args[i] = v
		}
	}
	return args, nil
}

// appendQuery appends a PostgreSQL wire protocol query message to buf and returns it.
func appendQuery(buf []byte, query string) []byte {
	buf = append(buf, 'Q')
	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)
	buf = append(buf, query...)
	buf = append(buf, 0)
	pgio.SetInt32(buf[sp:], int32(len(buf[sp:])))

	return buf
}
