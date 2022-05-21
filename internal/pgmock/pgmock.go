// Package pgmock provides the ability to mock a PostgreSQL server.
package pgmock

import (
	"fmt"
	"io"
	"reflect"

	"github.com/jackc/pgx/v5/pgproto3"
)

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

func (s *Script) Step(backend *pgproto3.Backend) error {
	return s.Run(backend)
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
		return fmt.Errorf("msg => %#v, e.want => %#v", msg, e.want)
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
		return fmt.Errorf("msg => %#v, e.want => %#v", msg, e.want)
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
	backend.Send(e.msg)
	return backend.Flush()
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
		SendMessage(&pgproto3.AuthenticationOk{}),
		SendMessage(&pgproto3.BackendKeyData{ProcessID: 0, SecretKey: 0}),
		SendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'}),
	}
}
