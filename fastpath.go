package pgx

import (
	"encoding/binary"
)

type fastpathArg []byte

func newFastpath(cn *Conn) *fastpath {
	return &fastpath{cn: cn, fns: make(map[string]Oid)}
}

type fastpath struct {
	cn  *Conn
	fns map[string]Oid
}

func (f *fastpath) functionOID(name string) Oid {
	return f.fns[name]
}

func (f *fastpath) addFunction(name string, oid Oid) {
	f.fns[name] = oid
}

func (f *fastpath) addFunctions(rows *Rows) error {
	for rows.Next() {
		var name string
		var oid Oid
		if err := rows.Scan(&name, &oid); err != nil {
			return err
		}
		f.addFunction(name, oid)
	}
	return rows.Err()
}

type fpArg []byte

func fpIntArg(n int32) fpArg {
	res := make([]byte, 4)
	binary.BigEndian.PutUint32(res, uint32(n))
	return res
}

func fpInt64Arg(n int64) fpArg {
	res := make([]byte, 8)
	binary.BigEndian.PutUint64(res, uint64(n))
	return res
}

func (f *fastpath) Call(oid Oid, args []fpArg) (res []byte, err error) {
	wbuf := newWriteBuf(f.cn, 'F')    // function call
	wbuf.WriteInt32(int32(oid))       // function object id
	wbuf.WriteInt16(1)                // # of argument format codes
	wbuf.WriteInt16(1)                // format code: binary
	wbuf.WriteInt16(int16(len(args))) // # of arguments
	for _, arg := range args {
		wbuf.WriteInt32(int32(len(arg))) // length of argument
		wbuf.WriteBytes(arg)             // argument value
	}
	wbuf.WriteInt16(1) // response format code (binary)
	wbuf.closeMsg()

	if _, err := f.cn.conn.Write(wbuf.buf); err != nil {
		return nil, err
	}

	for {
		var t byte
		var r *msgReader
		t, r, err = f.cn.rxMsg()
		if err != nil {
			return nil, err
		}
		switch t {
		case 'V': // FunctionCallResponse
			data := r.readBytes(r.readInt32())
			res = make([]byte, len(data))
			copy(res, data)
		case 'Z': // Ready for query
			f.cn.rxReadyForQuery(r)
			// done
			return
		default:
			if err := f.cn.processContextFreeMsg(t, r); err != nil {
				return nil, err
			}
		}
	}
}

func (f *fastpath) CallFn(fn string, args []fpArg) ([]byte, error) {
	return f.Call(f.functionOID(fn), args)
}

func fpInt32(data []byte, err error) (int32, error) {
	if err != nil {
		return 0, err
	}
	n := int32(binary.BigEndian.Uint32(data))
	return n, nil
}

func fpInt64(data []byte, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}
