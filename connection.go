// Package pgx is a PostgreSQL database driver.
//
// It does not implement the standard database/sql interface.
package pgx

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
)

// ConnectionParameters contains all the options used to establish a connection.
type ConnectionParameters struct {
	Socket   string // path to unix domain socket (e.g. /private/tmp/.s.PGSQL.5432)
	Host     string // url (e.g. localhost)
	Port     uint16 // default: 5432
	Database string
	User     string
	Password string
}

// Connection is a PostgreSQL connection handle. It is not safe for concurrent usage.
// Use ConnectionPool to manage access to multiple database connections from multiple
// goroutines.
type Connection struct {
	conn               net.Conn             // the underlying TCP or unix domain socket connection
	buf                *bytes.Buffer        // work buffer to avoid constant alloc and dealloc
	pid                int32                // backend pid
	secretKey          int32                // key to use to send a cancel query message to the server
	runtimeParams      map[string]string    // parameters that have been reported by the server
	parameters         ConnectionParameters // parameters used when establishing this connection
	txStatus           byte
	preparedStatements map[string]*preparedStatement
}

type preparedStatement struct {
	Name              string
	FieldDescriptions []FieldDescription
	ParameterOids     []oid
}

type NotSingleRowError struct {
	RowCount int64
}

func (e NotSingleRowError) Error() string {
	return fmt.Sprintf("Expected to find 1 row exactly, instead found %d", e.RowCount)
}

type UnexpectedColumnCountError struct {
	ExpectedCount int16
	ActualCount   int16
}

func (e UnexpectedColumnCountError) Error() string {
	return fmt.Sprintf("Expected result to have %d column(s), instead it has %d", e.ExpectedCount, e.ActualCount)
}

const sharedBufferSize = 1024

func Connect(parameters ConnectionParameters) (c *Connection, err error) {
	c = new(Connection)

	c.parameters = parameters
	if c.parameters.Port == 0 {
		c.parameters.Port = 5432
	}

	if c.parameters.Socket != "" {
		c.conn, err = net.Dial("unix", c.parameters.Socket)
		if err != nil {
			return nil, err
		}
	} else if c.parameters.Host != "" {
		c.conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", c.parameters.Host, c.parameters.Port))
		if err != nil {
			return nil, err
		}
	}

	c.buf = bytes.NewBuffer(make([]byte, 0, sharedBufferSize))
	c.runtimeParams = make(map[string]string)
	c.preparedStatements = make(map[string]*preparedStatement)

	msg := newStartupMessage()
	msg.options["user"] = c.parameters.User
	if c.parameters.Database != "" {
		msg.options["database"] = c.parameters.Database
	}
	c.txStartupMessage(msg)

	for {
		var t byte
		var r *MessageReader
		if t, r, err = c.rxMsg(); err == nil {
			switch t {
			case backendKeyData:
				c.rxBackendKeyData(r)
			case authenticationX:
				if err = c.rxAuthenticationX(r); err != nil {
					return nil, err
				}
			case readyForQuery:
				c.rxReadyForQuery(r)
				return c, nil
			default:
				if err = c.processContextFreeMsg(t, r); err != nil {
					return nil, err
				}
			}
		} else {
			return nil, err
		}
	}
}

func (c *Connection) Close() (err error) {
	return c.txMsg('X', c.getBuf())
}

func (c *Connection) SelectFunc(sql string, onDataRow func(*DataRowReader) error, arguments ...interface{}) (err error) {
	var fields []FieldDescription

	if ps, present := c.preparedStatements[sql]; present {
		fields = ps.FieldDescriptions
		err = c.sendPreparedQuery(ps, arguments...)
	} else {
		err = c.sendSimpleQuery(sql, arguments...)
	}
	if err != nil {
		return
	}

	for {
		if t, r, rxErr := c.rxMsg(); rxErr == nil {
			switch t {
			case readyForQuery:
				c.rxReadyForQuery(r)
				return
			case rowDescription:
				fields = c.rxRowDescription(r)
			case dataRow:
				if err == nil {
					err = onDataRow(newDataRowReader(r, fields))
				}
			case commandComplete:
			case bindComplete:
			default:
				if e := c.processContextFreeMsg(t, r); e != nil && err == nil {
					err = e
				}
			}
		} else {
			return rxErr
		}
	}
}

func (c *Connection) SelectRows(sql string, arguments ...interface{}) (rows []map[string]interface{}, err error) {
	rows = make([]map[string]interface{}, 0, 8)
	onDataRow := func(r *DataRowReader) error {
		rows = append(rows, c.rxDataRow(r))
		return nil
	}
	err = c.SelectFunc(sql, onDataRow, arguments...)
	return
}

// Returns a NotSingleRowError if exactly one row is not found
func (c *Connection) SelectRow(sql string, arguments ...interface{}) (row map[string]interface{}, err error) {
	var numRowsFound int64

	onDataRow := func(r *DataRowReader) error {
		numRowsFound++
		row = c.rxDataRow(r)
		return nil
	}
	err = c.SelectFunc(sql, onDataRow, arguments...)
	if err == nil && numRowsFound != 1 {
		err = NotSingleRowError{RowCount: numRowsFound}
	}
	return
}

// Returns a UnexpectedColumnCountError if exactly one column is not found
// Returns a NotSingleRowError if exactly one row is not found
func (c *Connection) SelectValue(sql string, arguments ...interface{}) (v interface{}, err error) {
	var numRowsFound int64

	onDataRow := func(r *DataRowReader) error {
		if len(r.fields) != 1 {
			return UnexpectedColumnCountError{ExpectedCount: 1, ActualCount: int16(len(r.fields))}
		}

		numRowsFound++
		v = r.ReadValue()
		return nil
	}
	err = c.SelectFunc(sql, onDataRow, arguments...)
	if err == nil {
		if numRowsFound != 1 {
			err = NotSingleRowError{RowCount: numRowsFound}
		}
	}
	return
}

// Returns a UnexpectedColumnCountError if exactly one column is not found
func (c *Connection) SelectValues(sql string, arguments ...interface{}) (values []interface{}, err error) {
	values = make([]interface{}, 0, 8)
	onDataRow := func(r *DataRowReader) error {
		if len(r.fields) != 1 {
			return UnexpectedColumnCountError{ExpectedCount: 1, ActualCount: int16(len(r.fields))}
		}

		values = append(values, r.ReadValue())
		return nil
	}
	err = c.SelectFunc(sql, onDataRow, arguments...)
	return
}

func (c *Connection) Prepare(name, sql string) (err error) {
	// parse
	buf := c.getBuf()
	w := newMessageWriter(buf)
	w.writeCString(name)
	w.writeCString(sql)
	w.write(int16(0))
	if w.err != nil {
		return w.err
	}
	err = c.txMsg('P', buf)
	if err != nil {
		return
	}

	// describe
	buf = c.getBuf()
	w = newMessageWriter(buf)
	w.writeByte('S')
	w.writeCString(name)
	if w.err != nil {
		return w.err
	}

	err = c.txMsg('D', buf)
	if err != nil {
		return
	}

	// sync
	err = c.txMsg('S', c.getBuf())
	if err != nil {
		return err
	}

	ps := preparedStatement{Name: name}

	for {
		if t, r, rxErr := c.rxMsg(); rxErr == nil {
			switch t {
			case parseComplete:
			case parameterDescription:
				ps.ParameterOids = c.rxParameterDescription(r)
			case rowDescription:
				ps.FieldDescriptions = c.rxRowDescription(r)
				for i := range ps.FieldDescriptions {
					oid := ps.FieldDescriptions[i].DataType
					if valueTranscoders[oid] != nil && valueTranscoders[oid].DecodeBinary != nil {
						ps.FieldDescriptions[i].FormatCode = 1
					}
				}
			case readyForQuery:
				c.rxReadyForQuery(r)
				c.preparedStatements[name] = &ps
				return
			default:
				if e := c.processContextFreeMsg(t, r); e != nil && err == nil {
					err = e
				}
			}
		} else {
			return rxErr
		}
	}
}

func (c *Connection) Deallocate(name string) (err error) {
	delete(c.preparedStatements, name)
	_, err = c.Execute("deallocate " + c.QuoteIdentifier(name))
	return
}

func (c *Connection) sendQuery(sql string, arguments ...interface{}) (err error) {
	if ps, present := c.preparedStatements[sql]; present {
		return c.sendPreparedQuery(ps, arguments...)
	} else {
		return c.sendSimpleQuery(sql, arguments...)
	}
}

func (c *Connection) sendSimpleQuery(sql string, arguments ...interface{}) (err error) {
	if len(arguments) > 0 {
		sql = c.SanitizeSql(sql, arguments...)
	}

	buf := c.getBuf()

	_, err = buf.WriteString(sql)
	if err != nil {
		return
	}
	err = buf.WriteByte(0)
	if err != nil {
		return
	}

	return c.txMsg('Q', buf)
}

func (c *Connection) sendPreparedQuery(ps *preparedStatement, arguments ...interface{}) (err error) {
	if len(ps.ParameterOids) != len(arguments) {
		return fmt.Errorf("Prepared statement \"%v\" requires %d parameters, but %d were provided", ps.Name, len(ps.ParameterOids), len(arguments))
	}

	// bind
	buf := c.getBuf()
	w := newMessageWriter(buf)
	w.writeCString("")
	w.writeCString(ps.Name)
	w.write(int16(len(ps.ParameterOids)))
	for _, oid := range ps.ParameterOids {
		transcoder := valueTranscoders[oid]
		if transcoder == nil {
			transcoder = defaultTranscoder
		}
		w.write(transcoder.EncodeFormat)
	}

	w.write(int16(len(arguments)))
	for i, oid := range ps.ParameterOids {
		transcoder := valueTranscoders[oid]
		if transcoder == nil {
			transcoder = defaultTranscoder
		}
		transcoder.EncodeTo(w, arguments[i])
	}

	w.write(int16(len(ps.FieldDescriptions)))
	for _, fd := range ps.FieldDescriptions {
		transcoder := valueTranscoders[fd.DataType]
		if transcoder != nil && transcoder.DecodeBinary != nil {
			w.write(int16(1))
		} else {
			w.write(int16(0))
		}
	}
	if w.err != nil {
		return w.err
	}

	err = c.txMsg('B', buf)
	if err != nil {
		return err
	}

	// execute
	buf = c.getBuf()
	w = newMessageWriter(buf)
	w.writeCString("")
	w.write(int32(0))

	if w.err != nil {
		return w.err
	}

	err = c.txMsg('E', buf)
	if err != nil {
		return err
	}

	// sync
	err = c.txMsg('S', c.getBuf())
	if err != nil {
		return err
	}

	return

}

func (c *Connection) Execute(sql string, arguments ...interface{}) (commandTag string, err error) {
	if err = c.sendQuery(sql, arguments...); err != nil {
		return
	}

	for {
		if t, r, rxErr := c.rxMsg(); rxErr == nil {
			switch t {
			case readyForQuery:
				c.rxReadyForQuery(r)
				return
			case rowDescription:
			case dataRow:
			case bindComplete:
			case commandComplete:
				commandTag = r.ReadString()
			default:
				if e := c.processContextFreeMsg(t, r); e != nil && err == nil {
					err = e
				}
			}
		} else {
			return "", rxErr
		}
	}
}

func (c *Connection) Transaction(f func() bool) (committed bool, err error) {
	return c.transaction("", f)
}

func (c *Connection) TransactionIso(isoLevel string, f func() bool) (committed bool, err error) {
	return c.transaction(isoLevel, f)
}

func (c *Connection) transaction(isoLevel string, f func() bool) (committed bool, err error) {
	var beginSql string
	if isoLevel == "" {
		beginSql = "begin"
	} else {
		beginSql = fmt.Sprintf("begin isolation level %s", isoLevel)
	}

	if _, err = c.Execute(beginSql); err != nil {
		return
	}
	defer func() {
		if committed && c.txStatus == 'T' {
			_, err = c.Execute("commit")
			if err != nil {
				committed = false
			}
		} else {
			_, err = c.Execute("rollback")
			committed = false
		}
	}()

	committed = f()
	return
}

// Processes messages that are not exclusive to one context such as
// authentication or query response. The response to these messages
// is the same regardless of when they occur.
func (c *Connection) processContextFreeMsg(t byte, r *MessageReader) (err error) {
	switch t {
	case 'S':
		c.rxParameterStatus(r)
		return nil
	case errorResponse:
		return c.rxErrorResponse(r)
	case noticeResponse:
		return nil
	default:
		return fmt.Errorf("Received unknown message type: %c", t)
	}
}

func (c *Connection) rxMsg() (t byte, r *MessageReader, err error) {
	var bodySize int32
	t, bodySize, err = c.rxMsgHeader()
	if err != nil {
		return
	}

	var body *bytes.Buffer
	if body, err = c.rxMsgBody(bodySize); err != nil {
		return
	}

	r = newMessageReader(body)
	return
}

func (c *Connection) rxMsgHeader() (t byte, bodySize int32, err error) {
	buf := c.getBuf()
	if _, err = io.CopyN(buf, c.conn, 5); err != nil {
		return 0, 0, err
	}

	t, err = buf.ReadByte()
	if err != nil {
		return
	}
	err = binary.Read(buf, binary.BigEndian, &bodySize)
	bodySize -= 4 // remove self from size
	return
}

func (c *Connection) rxMsgBody(bodySize int32) (buf *bytes.Buffer, err error) {
	buf = c.getBuf()
	_, err = io.CopyN(buf, c.conn, int64(bodySize))
	return
}

func (c *Connection) rxAuthenticationX(r *MessageReader) (err error) {
	code := r.ReadInt32()
	switch code {
	case 0: // AuthenticationOk
	case 3: // AuthenticationCleartextPassword
		c.txPasswordMessage(c.parameters.Password)
	case 5: // AuthenticationMD5Password
		salt := r.ReadByteString(4)
		digestedPassword := "md5" + hexMD5(hexMD5(c.parameters.Password+c.parameters.User)+salt)
		c.txPasswordMessage(digestedPassword)
	default:
		err = errors.New("Received unknown authentication message")
	}

	return
}

func hexMD5(s string) string {
	hash := md5.New()
	io.WriteString(hash, s)
	return hex.EncodeToString(hash.Sum(nil))
}

func (c *Connection) rxParameterStatus(r *MessageReader) {
	key := r.ReadString()
	value := r.ReadString()
	c.runtimeParams[key] = value
}

func (c *Connection) rxErrorResponse(r *MessageReader) (err PgError) {
	for {
		switch r.ReadByte() {
		case 'S':
			err.Severity = r.ReadString()
		case 'C':
			err.Code = r.ReadString()
		case 'M':
			err.Message = r.ReadString()
		case 0: // End of error message
			return
		default: // Ignore other error fields
			r.ReadString()
		}
	}
}

func (c *Connection) rxBackendKeyData(r *MessageReader) {
	c.pid = r.ReadInt32()
	c.secretKey = r.ReadInt32()
}

func (c *Connection) rxReadyForQuery(r *MessageReader) {
	c.txStatus = r.ReadByte()
}

func (c *Connection) rxRowDescription(r *MessageReader) (fields []FieldDescription) {
	fieldCount := r.ReadInt16()
	fields = make([]FieldDescription, fieldCount)
	for i := int16(0); i < fieldCount; i++ {
		f := &fields[i]
		f.Name = r.ReadString()
		f.Table = r.ReadOid()
		f.AttributeNumber = r.ReadInt16()
		f.DataType = r.ReadOid()
		f.DataTypeSize = r.ReadInt16()
		f.Modifier = r.ReadInt32()
		f.FormatCode = r.ReadInt16()
	}
	return
}

func (c *Connection) rxParameterDescription(r *MessageReader) (parameters []oid) {
	parameterCount := r.ReadInt16()
	parameters = make([]oid, 0, parameterCount)
	for i := int16(0); i < parameterCount; i++ {
		parameters = append(parameters, r.ReadOid())
	}
	return
}

func (c *Connection) rxDataRow(r *DataRowReader) (row map[string]interface{}) {
	fieldCount := len(r.fields)

	row = make(map[string]interface{}, fieldCount)
	for i := 0; i < fieldCount; i++ {
		row[r.fields[i].Name] = r.ReadValue()
	}
	return
}

func (c *Connection) rxCommandComplete(r *MessageReader) string {
	return r.ReadString()
}

func (c *Connection) txStartupMessage(msg *startupMessage) (err error) {
	_, err = c.conn.Write(msg.Bytes())
	return
}

func (c *Connection) txMsg(identifier byte, buf *bytes.Buffer) (err error) {
	err = binary.Write(c.conn, binary.BigEndian, identifier)
	if err != nil {
		return
	}

	err = binary.Write(c.conn, binary.BigEndian, int32(buf.Len()+4))
	if err != nil {
		return
	}

	_, err = buf.WriteTo(c.conn)
	return
}

func (c *Connection) txPasswordMessage(password string) (err error) {
	buf := c.getBuf()

	_, err = buf.WriteString(password)
	if err != nil {
		return
	}
	buf.WriteByte(0)
	if err != nil {
		return
	}
	err = c.txMsg('p', buf)
	return
}

// Gets the shared connection buffer. Since bytes.Buffer never releases memory from
// its internal byte array, check on the size and create a new bytes.Buffer so the
// old one can get GC'ed
func (c *Connection) getBuf() *bytes.Buffer {
	c.buf.Reset()
	if cap(c.buf.Bytes()) > sharedBufferSize {
		c.buf = bytes.NewBuffer(make([]byte, 0, sharedBufferSize))
	}
	return c.buf
}
