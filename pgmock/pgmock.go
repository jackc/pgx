package pgmock

import (
	"io"
	"net"
	"reflect"

	errors "golang.org/x/xerrors"

	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

type Server struct {
	ln         net.Listener
	controller Controller
}

func NewServer(controller Controller) (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		return nil, err
	}

	server := &Server{
		ln:         ln,
		controller: controller,
	}

	return server, nil
}

func (s *Server) Addr() net.Addr {
	return s.ln.Addr()
}

func (s *Server) ServeOne() error {
	conn, err := s.ln.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()

	s.Close()

	backend, err := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)
	if err != nil {
		conn.Close()
		return err
	}

	return s.controller.Serve(backend)
}

func (s *Server) Close() error {
	err := s.ln.Close()
	if err != nil {
		return err
	}

	return nil
}

type Controller interface {
	Serve(backend *pgproto3.Backend) error
}

type Step interface {
	Step(*pgproto3.Backend) error
}

type Script struct {
	Steps []Step
}

func (s *Script) Run(backend *pgproto3.Backend) error {
	for _, step := range s.Steps {
		err := step.Step(backend)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Script) Serve(backend *pgproto3.Backend) error {
	for _, step := range s.Steps {
		err := step.Step(backend)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Script) Step(backend *pgproto3.Backend) error {
	return s.Serve(backend)
}

type expectMessageStep struct {
	want pgproto3.FrontendMessage
	any  bool
}

func (e *expectMessageStep) Step(backend *pgproto3.Backend) error {
	msg, err := backend.Receive()
	if err != nil {
		return err
	}

	if e.any && reflect.TypeOf(msg) == reflect.TypeOf(e.want) {
		return nil
	}

	if !reflect.DeepEqual(msg, e.want) {
		return errors.Errorf("msg => %#v, e.want => %#v", msg, e.want)
	}

	return nil
}

type expectStartupMessageStep struct {
	want *pgproto3.StartupMessage
	any  bool
}

func (e *expectStartupMessageStep) Step(backend *pgproto3.Backend) error {
	msg, err := backend.ReceiveStartupMessage()
	if err != nil {
		return err
	}

	if e.any {
		return nil
	}

	if !reflect.DeepEqual(msg, e.want) {
		return errors.Errorf("msg => %#v, e.want => %#v", msg, e.want)
	}

	return nil
}

func ExpectMessage(want pgproto3.FrontendMessage) Step {
	return expectMessage(want, false)
}

func ExpectAnyMessage(want pgproto3.FrontendMessage) Step {
	return expectMessage(want, true)
}

func expectMessage(want pgproto3.FrontendMessage, any bool) Step {
	if want, ok := want.(*pgproto3.StartupMessage); ok {
		return &expectStartupMessageStep{want: want, any: any}
	}

	return &expectMessageStep{want: want, any: any}
}

type sendMessageStep struct {
	msg pgproto3.BackendMessage
}

func (e *sendMessageStep) Step(backend *pgproto3.Backend) error {
	return backend.Send(e.msg)
}

func SendMessage(msg pgproto3.BackendMessage) Step {
	return &sendMessageStep{msg: msg}
}

type waitForCloseMessageStep struct{}

func (e *waitForCloseMessageStep) Step(backend *pgproto3.Backend) error {
	for {
		msg, err := backend.Receive()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		if _, ok := msg.(*pgproto3.Terminate); ok {
			return nil
		}
	}
}

func WaitForClose() Step {
	return &waitForCloseMessageStep{}
}

func AcceptUnauthenticatedConnRequestSteps() []Step {
	return []Step{
		ExpectAnyMessage(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{}}),
		SendMessage(&pgproto3.Authentication{Type: pgproto3.AuthTypeOk}),
		SendMessage(&pgproto3.BackendKeyData{ProcessID: 0, SecretKey: 0}),
		SendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'}),
	}
}

type dataRowValue struct {
	Value      interface{}
	FormatCode int16
}

func mustBuildDataRow(values []interface{}, formatCodes []int16) *pgproto3.DataRow {
	dr, err := buildDataRow(values, formatCodes)
	if err != nil {
		panic(err)
	}

	return dr
}

func buildDataRow(values []interface{}, formatCodes []int16) (*pgproto3.DataRow, error) {
	dr := &pgproto3.DataRow{
		Values: make([][]byte, len(values)),
	}

	if len(formatCodes) == 1 {
		for i := 1; i < len(values); i++ {
			formatCodes = append(formatCodes, formatCodes[0])
		}
	}

	for i := range values {
		switch v := values[i].(type) {
		case string:
			values[i] = &pgtype.Text{String: v, Status: pgtype.Present}
		case int16:
			values[i] = &pgtype.Int2{Int: v, Status: pgtype.Present}
		case int32:
			values[i] = &pgtype.Int4{Int: v, Status: pgtype.Present}
		case int64:
			values[i] = &pgtype.Int8{Int: v, Status: pgtype.Present}
		}
	}

	for i := range values {
		switch formatCodes[i] {
		case pgproto3.TextFormat:
			if e, ok := values[i].(pgtype.TextEncoder); ok {
				buf, err := e.EncodeText(nil, nil)
				if err != nil {
					return nil, errors.Errorf("failed to encode values[%d]", i)
				}
				dr.Values[i] = buf
			} else {
				return nil, errors.Errorf("values[%d] does not implement TextExcoder", i)
			}

		case pgproto3.BinaryFormat:
			if e, ok := values[i].(pgtype.BinaryEncoder); ok {
				buf, err := e.EncodeBinary(nil, nil)
				if err != nil {
					return nil, errors.Errorf("failed to encode values[%d]", i)
				}
				dr.Values[i] = buf
			} else {
				return nil, errors.Errorf("values[%d] does not implement BinaryEncoder", i)
			}
		default:
			return nil, errors.New("unknown FormatCode")
		}
	}

	return dr, nil
}
