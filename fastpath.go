package pgx

import (
	"encoding/binary"

	"github.com/jackc/pgx/pgio"
	"github.com/jackc/pgx/pgproto3"
	"github.com/jackc/pgx/pgtype"
)

func newFastpath(cn *Conn) *fastpath {
	return &fastpath{cn: cn, fns: make(map[string]pgtype.Oid)}
}

type fastpath struct {
	cn  *Conn
	fns map[string]pgtype.Oid
}

func (f *fastpath) functionOid(name string) pgtype.Oid {
	return f.fns[name]
}

func (f *fastpath) addFunction(name string, oid pgtype.Oid) {
	f.fns[name] = oid
}

func (f *fastpath) addFunctions(rows *Rows) error {
	for rows.Next() {
		var name string
		var oid pgtype.Oid
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

func (f *fastpath) Call(oid pgtype.Oid, args []fpArg) (res []byte, err error) {
	if err := f.cn.ensureConnectionReadyForQuery(); err != nil {
		return nil, err
	}

	buf := f.cn.wbuf
	buf = append(buf, 'F') // function call
	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)

	buf = pgio.AppendInt32(buf, int32(oid))       // function object id
	buf = pgio.AppendInt16(buf, 1)                // # of argument format codes
	buf = pgio.AppendInt16(buf, 1)                // format code: binary
	buf = pgio.AppendInt16(buf, int16(len(args))) // # of arguments
	for _, arg := range args {
		buf = pgio.AppendInt32(buf, int32(len(arg))) // length of argument
		buf = append(buf, arg...)                    // argument value
	}
	buf = pgio.AppendInt16(buf, 1) // response format code (binary)
	pgio.SetInt32(buf[sp:], int32(len(buf[sp:])))

	if _, err := f.cn.conn.Write(buf); err != nil {
		return nil, err
	}

	for {
		msg, err := f.cn.rxMsg()
		if err != nil {
			return nil, err
		}
		switch msg := msg.(type) {
		case *pgproto3.FunctionCallResponse:
			res = make([]byte, len(msg.Result))
			copy(res, msg.Result)
		case *pgproto3.ReadyForQuery:
			f.cn.rxReadyForQuery(msg)
			// done
			return res, err
		default:
			if err := f.cn.processContextFreeMsg(msg); err != nil {
				return nil, err
			}
		}
	}
}

func (f *fastpath) CallFn(fn string, args []fpArg) ([]byte, error) {
	return f.Call(f.functionOid(fn), args)
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
