package pgx

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/pgio"
	"github.com/jackc/pgx/pgproto3"
	"github.com/jackc/pgx/pgtype"
	"github.com/pkg/errors"
)

// CopyFromRows returns a CopyFromSource interface over the provided rows slice
// making it usable by *Conn.CopyFrom.
func CopyFromRows(rows [][]interface{}) CopyFromSource {
	return &copyFromRows{rows: rows, idx: -1}
}

type copyFromRows struct {
	rows [][]interface{}
	idx  int
}

func (ctr *copyFromRows) Next() bool {
	ctr.idx++
	return ctr.idx < len(ctr.rows)
}

func (ctr *copyFromRows) Values() ([]interface{}, error) {
	return ctr.rows[ctr.idx], nil
}

func (ctr *copyFromRows) Err() error {
	return nil
}

// CopyFromSource is the interface used by *Conn.CopyFrom as the source for copy data.
type CopyFromSource interface {
	// Next returns true if there is another row and makes the next row data
	// available to Values(). When there are no more rows available or an error
	// has occurred it returns false.
	Next() bool

	// Values returns the values for the current row.
	Values() ([]interface{}, error)

	// Err returns any error that has been encountered by the CopyFromSource. If
	// this is not nil *Conn.CopyFrom will abort the copy.
	Err() error
}

type copyFrom struct {
	conn          *Conn
	tableName     Identifier
	columnNames   []string
	rowSrc        CopyFromSource
	readerErrChan chan error
}

func (ct *copyFrom) readUntilReadyForQuery() {
	for {
		msg, err := ct.conn.rxMsg()
		if err != nil {
			ct.readerErrChan <- err
			close(ct.readerErrChan)
			return
		}

		switch msg := msg.(type) {
		case *pgproto3.ReadyForQuery:
			ct.conn.rxReadyForQuery(msg)
			close(ct.readerErrChan)
			return
		case *pgproto3.CommandComplete:
		case *pgproto3.ErrorResponse:
			ct.readerErrChan <- ct.conn.rxErrorResponse(msg)
		default:
			err = ct.conn.processContextFreeMsg(msg)
			if err != nil {
				ct.readerErrChan <- ct.conn.processContextFreeMsg(msg)
			}
		}
	}
}

func (ct *copyFrom) waitForReaderDone() error {
	var err error
	for err = range ct.readerErrChan {
	}
	return err
}

func (ct *copyFrom) run() (int, error) {
	quotedTableName := ct.tableName.Sanitize()
	cbuf := &bytes.Buffer{}
	for i, cn := range ct.columnNames {
		if i != 0 {
			cbuf.WriteString(", ")
		}
		cbuf.WriteString(quoteIdentifier(cn))
	}
	quotedColumnNames := cbuf.String()

	ps, err := ct.conn.Prepare("", fmt.Sprintf("select %s from %s", quotedColumnNames, quotedTableName))
	if err != nil {
		return 0, err
	}

	err = ct.conn.sendSimpleQuery(fmt.Sprintf("copy %s ( %s ) from stdin binary;", quotedTableName, quotedColumnNames))
	if err != nil {
		return 0, err
	}

	err = ct.conn.readUntilCopyInResponse()
	if err != nil {
		return 0, err
	}

	go ct.readUntilReadyForQuery()
	defer ct.waitForReaderDone()

	buf := ct.conn.wbuf
	buf = append(buf, copyData)
	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)

	buf = append(buf, "PGCOPY\n\377\r\n\000"...)
	buf = pgio.AppendInt32(buf, 0)
	buf = pgio.AppendInt32(buf, 0)

	var sentCount int

	for ct.rowSrc.Next() {
		select {
		case err = <-ct.readerErrChan:
			return 0, err
		default:
		}

		if len(buf) > 65536 {
			pgio.SetInt32(buf[sp:], int32(len(buf[sp:])))
			_, err = ct.conn.conn.Write(buf)
			if err != nil {
				ct.conn.die(err)
				return 0, err
			}

			// Directly manipulate wbuf to reset to reuse the same buffer
			buf = buf[0:5]
		}

		sentCount++

		values, err := ct.rowSrc.Values()
		if err != nil {
			ct.cancelCopyIn()
			return 0, err
		}
		if len(values) != len(ct.columnNames) {
			ct.cancelCopyIn()
			return 0, errors.Errorf("expected %d values, got %d values", len(ct.columnNames), len(values))
		}

		buf = pgio.AppendInt16(buf, int16(len(ct.columnNames)))
		for i, val := range values {
			buf, err = encodePreparedStatementArgument(ct.conn.ConnInfo, buf, ps.FieldDescriptions[i].DataType, val)
			if err != nil {
				ct.cancelCopyIn()
				return 0, err
			}

		}
	}

	if ct.rowSrc.Err() != nil {
		ct.cancelCopyIn()
		return 0, ct.rowSrc.Err()
	}

	buf = pgio.AppendInt16(buf, -1) // terminate the copy stream
	pgio.SetInt32(buf[sp:], int32(len(buf[sp:])))

	buf = append(buf, copyDone)
	buf = pgio.AppendInt32(buf, 4)

	_, err = ct.conn.conn.Write(buf)
	if err != nil {
		ct.conn.die(err)
		return 0, err
	}

	err = ct.waitForReaderDone()
	if err != nil {
		return 0, err
	}
	return sentCount, nil
}

func (c *Conn) readUntilCopyInResponse() error {
	for {
		msg, err := c.rxMsg()
		if err != nil {
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyInResponse:
			return nil
		default:
			err = c.processContextFreeMsg(msg)
			if err != nil {
				return err
			}
		}
	}
}

func (ct *copyFrom) cancelCopyIn() error {
	buf := ct.conn.wbuf
	buf = append(buf, copyFail)
	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)
	buf = append(buf, "client error: abort"...)
	buf = append(buf, 0)
	pgio.SetInt32(buf[sp:], int32(len(buf[sp:])))

	_, err := ct.conn.conn.Write(buf)
	if err != nil {
		ct.conn.die(err)
		return err
	}

	return nil
}

// CopyFrom uses the PostgreSQL copy protocol to perform bulk data insertion.
// It returns the number of rows copied and an error.
//
// CopyFrom requires all values use the binary format. Almost all types
// implemented by pgx use the binary format by default. Types implementing
// Encoder can only be used if they encode to the binary format.
func (c *Conn) CopyFrom(tableName Identifier, columnNames []string, rowSrc CopyFromSource) (int, error) {
	ct := &copyFrom{
		conn:          c,
		tableName:     tableName,
		columnNames:   columnNames,
		rowSrc:        rowSrc,
		readerErrChan: make(chan error),
	}

	return ct.run()
}

// CopyFromReader facilitates line-by-line bulk copy functionality from an io.Reader,
// such as a file or a network connection. It wraps a scanner over the supplied reader
// and returns an instance that satisfies the pgx.CopyFromSource interface.
// The lines are expected to be terminated by the '\n'. Any '\r' is automatically dropped.
//
// The expected format is a typical csv line format, with a user-defined separator rune.
// The values should be wrapped with or double quotes, but the parser allows unquoted
// values, as long as there are no commas inside. Whitespace outside double quotes is trimmed,
// so, in general, it is a mistake to pass text fields unquoted.
// The date type must be in the "YYYY-MM-DD" format
// The timestamp must be in the "YYYY-MM-DD HH:mm:ss MST" format
// The json fields must be in the official double quote format (e.g. {"myKey":"myVal"})
// The postgres interval type is (for now) restricted to the Go range and parseable string syntax.
//
// If a non-nil nullPlaceholder is specified, the db null value will be used
// for that column if that value is encountered (including the empty string).
//
//
// Example of a valid, parseable line with six columns (varchar, integer, jsonb, text, bool, date),
// "Hello, friend", 932, "{"age":34}", "No comments", false, "2010-10-20"
//
// Example of usage for a table with the above-mentioned columns
// imported from a comma-separated reader:
//
//  nullChar := "[!null]"
//	copySourceReader, err := CopyFromReader(r, pgx.CommaSeparator, &nullChar, "varchar", "integer", "jsonb","text","bool","date")
//	if err != nil {
//		return err
//	}
//	copyCount, err := currentDbHandle.CopyFrom(pgx.Identifier{"gaga_test_table"},
//		[]string{"greeting", "age", "meta_info", "comments", "is_enabled", "date_created"}, copySourceReader)
//	if err != nil {
//		return err
//	}
//	fmt.Println("Records copied:",copyCount)
func CopyFromReader(rdr io.Reader, separator rune, nullPlaceholder *string, dbtypes ...string) (CopyFromSource, error) {

	if len(dbtypes) == 0 {
		return nil, fmt.Errorf("CopyFromReader: Missing db types definitions")
	}
	c := &copyFromReader{
		lineReader:         csv.NewReader(rdr),
		separator:          separator,
		nullPlaceholder:    *nullPlaceholder,
		useNullPlaceholder: (nullPlaceholder != nil),
		idx:                -1,
		dbtypes:            dbtypes,
	}
	c.lineReader.Comma = separator
	c.lineReader.LazyQuotes = true
	c.lineReader.TrimLeadingSpace = true
	c.lineReader.FieldsPerRecord = len(dbtypes)

	return c, nil
}

// CommaSeparator is the rune constant for the comma character. It can be supplied
// to CopyFromReader as the separator rune for comma-separated values.
const CommaSeparator rune = ','

// TabSeparator is the rune constant for the tab character. It can be supplied
// to CopyFromReader as the separator rune for tab-separated values.
const TabSeparator rune = ','

var bEmptyStringDoubleQuotes = []byte("\"\"")
var bEmptyStringSingleQuotes = []byte("''")
var bDotComparatorSlice = []byte(".")

type copyFromReader struct {
	lineReader         *csv.Reader
	separator          rune
	useNullPlaceholder bool
	nullPlaceholder    string
	idx                int
	dbtypes            []string
	currRowErr         error
	currRow            []string
}

func (ctr *copyFromReader) Next() bool {
	ctr.idx++
	row, err := ctr.lineReader.Read()
	// end of file, return false
	if err == io.EOF {
		return false
	}
	if err != nil {
		ctr.currRowErr = err
		ctr.currRow = nil
	}
	ctr.currRowErr = nil
	ctr.currRow = row
	return true
}

func (ctr *copyFromReader) Values() ([]interface{}, error) {

	// Exit early if a csv parsing / splitting error occured inside Next()
	if ctr.currRowErr != nil {
		return nil, fmt.Errorf("copyFromReader.Value(row %d) error: %s", (ctr.idx + 1), ctr.currRowErr.Error())
	}

	outputValues := make([]interface{}, len(ctr.currRow))
	for i := range ctr.currRow {
		// Treat zero-length as null value is null placeholder happens to be the empty string
		if len(ctr.currRow[i]) == 0 && ctr.useNullPlaceholder && ctr.nullPlaceholder == "" {
			typeInstance := ctr.getPgTypeInstanceWithStatus(ctr.dbtypes[i], false)
			outputValues[i] = typeInstance
			continue
		}

		// Null placeholder match: treat it as null
		if ctr.useNullPlaceholder && ctr.nullPlaceholder == ctr.currRow[i] {
			typeInstance := ctr.getPgTypeInstanceWithStatus(ctr.dbtypes[i], false)
			outputValues[i] = typeInstance
			continue
		}

		// For non-text types, trim any right whitespace left and if the trimmed result
		// is zero-length, assume null
		if ctr.dbtypes[i] != "varchar" && ctr.dbtypes[i] != "text" {
			ctr.currRow[i] = strings.TrimRight(ctr.currRow[i], " ")
			if len(ctr.currRow[i]) == 0 {
				typeInstance := ctr.getPgTypeInstanceWithStatus(ctr.dbtypes[i], false)
				outputValues[i] = typeInstance
				continue
			}
		}

		val := []byte(ctr.currRow[i])

		typeInstance := ctr.getPgTypeInstanceWithStatus(ctr.dbtypes[i], true)
		if ctr.dbtypes[i] == "json" || ctr.dbtypes[i] == "jsonb" {
			if err := typeInstance.Set(val); err != nil {
				return nil, fmt.Errorf("copyFromReader.Value(row %d, column %d) error: %s", (ctr.idx + 1), (i + 1), err.Error())
			}
		} else if ctr.dbtypes[i] == "date" {
			t, err := time.Parse("2006-01-02", ctr.currRow[i])
			if err != nil {
				return nil, fmt.Errorf("copyFromReader.Value(row %d, column %d) error: %s", (ctr.idx + 1), (i + 1), err.Error())
			}
			if err := typeInstance.Set(t); err != nil {
				return nil, fmt.Errorf("copyFromReader.Value(row %d, column %d) error: %s", (ctr.idx + 1), (i + 1), err.Error())
			}
		} else if ctr.dbtypes[i] == "interval" {
			td, err := time.ParseDuration(ctr.currRow[i])
			if err != nil {
				return nil, fmt.Errorf("copyFromReader.Value(row %d, column %d) error: %s", (ctr.idx + 1), (i + 1), err.Error())
			}
			if err := typeInstance.Set(td); err != nil {
				return nil, fmt.Errorf("copyFromReader.Value(row %d, column %d) error: %s", (ctr.idx + 1), (i + 1), err.Error())
			}
		} else if ctr.dbtypes[i] == "timestamptz" {
			layout := "2006-01-02 15:04:05 MST"
			if bytes.Contains(val, bDotComparatorSlice) {
				layout = "2006-01-02 15:04:05.999999999 MST"
			}
			t, err := time.Parse(layout, ctr.currRow[i])
			if err != nil {
				return nil, fmt.Errorf("copyFromReader.Value(row %d, column %d) error: %s", (ctr.idx + 1), (i + 1), err.Error())
			}
			if err := typeInstance.Set(t); err != nil {
				return nil, fmt.Errorf("copyFromReader.Value(row %d, column %d) error: %s", (ctr.idx + 1), (i + 1), err.Error())
			}
		} else if ctr.dbtypes[i] == "timestamp" {
			layout := "2006-01-02 15:04:05"
			if bytes.Contains(val, bDotComparatorSlice) {
				layout = "2006-01-02 15:04:05.999999999"
			}
			t, err := time.Parse(layout, ctr.currRow[i])
			if err != nil {
				return nil, fmt.Errorf("copyFromReader.Value(row %d, column %d) error: %s", (ctr.idx + 1), (i + 1), err.Error())
			}
			if err := typeInstance.Set(t); err != nil {
				return nil, fmt.Errorf("copyFromReader.Value(row %d, column %d) error: %s", (ctr.idx + 1), (i + 1), err.Error())
			}
		} else {
			if err := typeInstance.Set(ctr.currRow[i]); err != nil {
				return nil, fmt.Errorf("copyFromReader.Value(row %d, column %d) error: %s", (ctr.idx + 1), (i + 1), err.Error())
			}
		}

		outputValues[i] = typeInstance
	}
	return outputValues, nil
}

func (ctr *copyFromReader) Err() error {
	return ctr.currRowErr
}

func (ctr *copyFromReader) getPgTypeInstanceWithStatus(dbtype string, present bool) pgtype.Value {
	if len(dbtype) == 0 {
		return nil
	}
	if v, ok := pgTypesFuncMap[dbtype]; ok {
		return v(present)
	}
	return nil
}

var pgTypesFuncMap map[string]func(present bool) pgtype.Value = map[string]func(present bool) pgtype.Value{
	"_aclitem": func(present bool) pgtype.Value {
		return &pgtype.ACLItemArray{Status: getStatusFromBool(present)}
	},
	"_bool": func(present bool) pgtype.Value {
		return &pgtype.BoolArray{Status: getStatusFromBool(present)}
	},
	"_bytea": func(present bool) pgtype.Value {
		return &pgtype.ByteaArray{Status: getStatusFromBool(present)}
	},
	"_cidr": func(present bool) pgtype.Value {
		return &pgtype.CIDRArray{Status: getStatusFromBool(present)}
	},
	"_date": func(present bool) pgtype.Value {
		return &pgtype.DateArray{Status: getStatusFromBool(present)}
	},
	"_float4": func(present bool) pgtype.Value {
		return &pgtype.Float4Array{Status: getStatusFromBool(present)}
	},
	"_float8": func(present bool) pgtype.Value {
		return &pgtype.Float8Array{Status: getStatusFromBool(present)}
	},
	"_inet": func(present bool) pgtype.Value {
		return &pgtype.InetArray{Status: getStatusFromBool(present)}
	},
	"_int2": func(present bool) pgtype.Value {
		return &pgtype.Int2Array{Status: getStatusFromBool(present)}
	},
	"_int4": func(present bool) pgtype.Value {
		return &pgtype.Int4Array{Status: getStatusFromBool(present)}
	},
	"_int8": func(present bool) pgtype.Value {
		return &pgtype.Int8Array{Status: getStatusFromBool(present)}
	},
	"_numeric": func(present bool) pgtype.Value {
		return &pgtype.NumericArray{Status: getStatusFromBool(present)}
	},
	"_text": func(present bool) pgtype.Value {
		return &pgtype.TextArray{Status: getStatusFromBool(present)}
	},
	"_timestamp": func(present bool) pgtype.Value {
		return &pgtype.TimestampArray{Status: getStatusFromBool(present)}
	},
	"_timestamptz": func(present bool) pgtype.Value {
		return &pgtype.TimestamptzArray{Status: getStatusFromBool(present)}
	},
	"_uuid": func(present bool) pgtype.Value {
		return &pgtype.UUIDArray{Status: getStatusFromBool(present)}
	},
	"_varchar": func(present bool) pgtype.Value {
		return &pgtype.VarcharArray{Status: getStatusFromBool(present)}
	},
	"aclitem": func(present bool) pgtype.Value {
		return &pgtype.ACLItem{Status: getStatusFromBool(present)}
	},
	"bigint": func(present bool) pgtype.Value {
		return &pgtype.Int8{Status: getStatusFromBool(present)}
	},
	"bool": func(present bool) pgtype.Value {
		return &pgtype.Bool{Status: getStatusFromBool(present)}
	},
	"box": func(present bool) pgtype.Value { return &pgtype.Box{Status: getStatusFromBool(present)} },
	"bytea": func(present bool) pgtype.Value {
		return &pgtype.Bytea{Status: getStatusFromBool(present)}
	},
	"char": func(present bool) pgtype.Value {
		return &pgtype.QChar{Status: getStatusFromBool(present)}
	},
	"cid": func(present bool) pgtype.Value { return &pgtype.CID{Status: getStatusFromBool(present)} },
	"cidr": func(present bool) pgtype.Value {
		return &pgtype.CIDR{Status: getStatusFromBool(present)}
	},
	"circle": func(present bool) pgtype.Value {
		return &pgtype.Circle{Status: getStatusFromBool(present)}
	},
	"date": func(present bool) pgtype.Value {
		return &pgtype.Date{Status: getStatusFromBool(present)}
	},
	"daterange": func(present bool) pgtype.Value {
		return &pgtype.Daterange{Status: getStatusFromBool(present)}
	},
	"decimal": func(present bool) pgtype.Value {
		return &pgtype.Decimal{Status: getStatusFromBool(present)}
	},
	"float4": func(present bool) pgtype.Value {
		return &pgtype.Float4{Status: getStatusFromBool(present)}
	},
	"float8": func(present bool) pgtype.Value {
		return &pgtype.Float8{Status: getStatusFromBool(present)}
	},
	"hstore": func(present bool) pgtype.Value {
		return &pgtype.Hstore{Status: getStatusFromBool(present)}
	},
	"inet": func(present bool) pgtype.Value {
		return &pgtype.Inet{Status: getStatusFromBool(present)}
	},
	"int2": func(present bool) pgtype.Value {
		return &pgtype.Int2{Status: getStatusFromBool(present)}
	},
	"int4": func(present bool) pgtype.Value {
		return &pgtype.Int4{Status: getStatusFromBool(present)}
	},
	"integer": func(present bool) pgtype.Value {
		return &pgtype.Int4{Status: getStatusFromBool(present)}
	},
	"int4range": func(present bool) pgtype.Value {
		return &pgtype.Int4range{Status: getStatusFromBool(present)}
	},
	"int8": func(present bool) pgtype.Value {
		return &pgtype.Int8{Status: getStatusFromBool(present)}
	},
	"int8range": func(present bool) pgtype.Value {
		return &pgtype.Int8range{Status: getStatusFromBool(present)}
	},
	"json": func(present bool) pgtype.Value {
		return &pgtype.JSON{Status: getStatusFromBool(present)}
	},
	"jsonb": func(present bool) pgtype.Value {
		return &pgtype.JSONB{Status: getStatusFromBool(present)}
	},
	"line": func(present bool) pgtype.Value {
		return &pgtype.Line{Status: getStatusFromBool(present)}
	},
	"lseg": func(present bool) pgtype.Value {
		return &pgtype.Lseg{Status: getStatusFromBool(present)}
	},
	"macaddr": func(present bool) pgtype.Value {
		return &pgtype.Macaddr{Status: getStatusFromBool(present)}
	},
	"name": func(present bool) pgtype.Value {
		return &pgtype.Name{Status: getStatusFromBool(present)}
	},
	"numeric": func(present bool) pgtype.Value {
		return &pgtype.Numeric{Status: getStatusFromBool(present)}
	},
	"numrange": func(present bool) pgtype.Value {
		return &pgtype.Numrange{Status: getStatusFromBool(present)}
	},
	"oid": func(present bool) pgtype.Value {
		return &pgtype.OIDValue{Status: getStatusFromBool(present)}
	},
	"path": func(present bool) pgtype.Value {
		return &pgtype.Path{Status: getStatusFromBool(present)}
	},
	"point": func(present bool) pgtype.Value {
		return &pgtype.Point{Status: getStatusFromBool(present)}
	},
	"polygon": func(present bool) pgtype.Value {
		return &pgtype.Polygon{Status: getStatusFromBool(present)}
	},
	"record": func(present bool) pgtype.Value {
		return &pgtype.Record{Status: getStatusFromBool(present)}
	},
	"text": func(present bool) pgtype.Value {
		return &pgtype.Text{Status: getStatusFromBool(present)}
	},
	"tid": func(present bool) pgtype.Value { return &pgtype.TID{Status: getStatusFromBool(present)} },
	"timestamp": func(present bool) pgtype.Value {
		return &pgtype.Timestamp{Status: getStatusFromBool(present)}
	},
	"timestamptz": func(present bool) pgtype.Value {
		return &pgtype.Timestamptz{Status: getStatusFromBool(present)}
	},
	"tsrange": func(present bool) pgtype.Value {
		return &pgtype.Tsrange{Status: getStatusFromBool(present)}
	},
	"tstzrange": func(present bool) pgtype.Value {
		return &pgtype.Tstzrange{Status: getStatusFromBool(present)}
	},
	"unknown": func(present bool) pgtype.Value {
		return &pgtype.Unknown{Status: getStatusFromBool(present)}
	},
	"uuid": func(present bool) pgtype.Value {
		return &pgtype.UUID{Status: getStatusFromBool(present)}
	},
	"varbit": func(present bool) pgtype.Value {
		return &pgtype.Varbit{Status: getStatusFromBool(present)}
	},
	"varchar": func(present bool) pgtype.Value {
		return &pgtype.Varchar{Status: getStatusFromBool(present)}
	},
	"xid": func(present bool) pgtype.Value { return &pgtype.XID{Status: getStatusFromBool(present)} },
}

func getStatusFromBool(present bool) pgtype.Status {
	if present {
		return pgtype.Present
	} else {
		return pgtype.Null
	}
}
