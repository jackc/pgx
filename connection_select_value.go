package pgx

import (
	"errors"
	"strconv"
)

func (c *Connection) SelectString(sql string) (s string, err error) {
	onDataRow := func(r *messageReader, _ []fieldDescription) error {
		var null bool
		s, null = c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		return nil
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) selectInt(sql string, size int) (i int64, err error) {
	var s string
	s, err = c.SelectString(sql)
	if err != nil {
		return
	}

	i, err = strconv.ParseInt(s, 10, size)
	return
}

func (c *Connection) SelectInt64(sql string) (i int64, err error) {
	return c.selectInt(sql, 64)
}

func (c *Connection) SelectInt32(sql string) (i int32, err error) {
	var i64 int64
	i64, err = c.selectInt(sql, 32)
	i = int32(i64)
	return
}

func (c *Connection) SelectInt16(sql string) (i int16, err error) {
	var i64 int64
	i64, err = c.selectInt(sql, 16)
	i = int16(i64)
	return
}

func (c *Connection) selectFloat(sql string, size int) (f float64, err error) {
	var s string
	s, err = c.SelectString(sql)
	if err != nil {
		return
	}

	f, err = strconv.ParseFloat(s, size)
	return
}

func (c *Connection) SelectFloat64(sql string) (f float64, err error) {
	return c.selectFloat(sql, 64)
}

func (c *Connection) SelectFloat32(sql string) (f float32, err error) {
	var f64 float64
	f64, err = c.selectFloat(sql, 32)
	f = float32(f64)
	return
}
