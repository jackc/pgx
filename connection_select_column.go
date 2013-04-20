package pgx

import (
	"errors"
	"strconv"
)

func (c *Connection) SelectAllString(sql string) (strings []string, err error) {
	strings = make([]string, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) error {
		s, null := c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		strings = append(strings, s)
		return nil
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) SelectAllInt64(sql string) (ints []int64, err error) {
	ints = make([]int64, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) (parseError error) {
		s, null := c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		var i int64
		i, parseError = strconv.ParseInt(s, 10, 64)
		ints = append(ints, i)
		return
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) SelectAllInt32(sql string) (ints []int32, err error) {
	ints = make([]int32, 0, 8)
	onDataRow := func(r *messageReader, fields []fieldDescription) (parseError error) {
		s, null := c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		var i int64
		i, parseError = strconv.ParseInt(s, 10, 32)
		ints = append(ints, int32(i))
		return
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) SelectAllInt16(sql string) (ints []int16, err error) {
	ints = make([]int16, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) (parseError error) {
		s, null := c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		var i int64
		i, parseError = strconv.ParseInt(s, 10, 16)
		ints = append(ints, int16(i))
		return
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) SelectAllFloat64(sql string) (floats []float64, err error) {
	floats = make([]float64, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) (parseError error) {
		s, null := c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		var f float64
		f, parseError = strconv.ParseFloat(s, 64)
		floats = append(floats, f)
		return
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}

func (c *Connection) SelectAllFloat32(sql string) (floats []float32, err error) {
	floats = make([]float32, 0, 8)
	onDataRow := func(r *messageReader, _ []fieldDescription) (parseError error) {
		s, null := c.rxDataRowFirstValue(r)
		if null {
			return errors.New("Unexpected NULL")
		}
		var f float64
		f, parseError = strconv.ParseFloat(s, 32)
		floats = append(floats, float32(f))
		return
	}
	err = c.SelectFunc(sql, onDataRow)
	return
}
