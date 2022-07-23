package pgproto3

import (
	"encoding/hex"
	"errors"
	"fmt"
)

// Message is the interface implemented by an object that can decode and encode
// a particular PostgreSQL message.
type Message interface {
	// Decode is allowed and expected to retain a reference to data after
	// returning (unlike encoding.BinaryUnmarshaler).
	Decode(data []byte) error

	// Encode appends itself to dst and returns the new buffer.
	Encode(dst []byte) []byte
}

// FrontendMessage is a message sent by the frontend (i.e. the client).
type FrontendMessage interface {
	Message
	Frontend() // no-op method to distinguish frontend from backend methods
}

// BackendMessage is a message sent by the backend (i.e. the server).
type BackendMessage interface {
	Message
	Backend() // no-op method to distinguish frontend from backend methods
}

type AuthenticationResponseMessage interface {
	BackendMessage
	AuthenticationResponse() // no-op method to distinguish authentication responses
}

type invalidMessageLenErr struct {
	messageType string
	expectedLen int
	actualLen   int
}

func (e *invalidMessageLenErr) Error() string {
	return fmt.Sprintf("%s body must have length of %d, but it is %d", e.messageType, e.expectedLen, e.actualLen)
}

type invalidMessageFormatErr struct {
	messageType string
	details     string
}

func (e *invalidMessageFormatErr) Error() string {
	return fmt.Sprintf("%s body is invalid %s", e.messageType, e.details)
}

type writeError struct {
	err         error
	safeToRetry bool
}

func (e *writeError) Error() string {
	return fmt.Sprintf("write failed: %s", e.err.Error())
}

func (e *writeError) SafeToRetry() bool {
	return e.safeToRetry
}

func (e *writeError) Unwrap() error {
	return e.err
}

// getValueFromJSON gets the value from a protocol message representation in JSON.
func getValueFromJSON(v map[string]string) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	if text, ok := v["text"]; ok {
		return []byte(text), nil
	}
	if binary, ok := v["binary"]; ok {
		return hex.DecodeString(binary)
	}
	return nil, errors.New("unknown protocol representation")
}
