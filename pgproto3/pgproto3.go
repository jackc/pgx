package pgproto3

import "fmt"

type Message interface {
	UnmarshalBinary(data []byte) error
	MarshalBinary() (data []byte, err error)
}

type FrontendMessage interface {
	Message
	Frontend() // no-op method to distinguish frontend from backend methods
}

type BackendMessage interface {
	Message
	Backend() // no-op method to distinguish frontend from backend methods
}

// func ParseBackend(typeByte byte, body []byte) (BackendMessage, error) {
// 	switch typeByte {
// 	case '1':
// 		return ParseParseComplete(body)
// 	case '2':
// 		return ParseBindComplete(body)
// 	case 'C':
// 		return ParseCommandComplete(body)
// 	case 'D':
// 		return ParseDataRow(body)
// 	case 'E':
// 		return ParseErrorResponse(body)
// 	case 'K':
// 		return ParseBackendKeyData(body)
// 	case 'R':
// 		return ParseAuthentication(body)
// 	case 'S':
// 		return ParseParameterStatus(body)
// 	case 'T':
// 		return ParseRowDescription(body)
// 	case 't':
// 		return ParseParameterDescription(body)
// 	case 'Z':
// 		return ParseReadyForQuery(body)
// 	default:
// 		return ParseUnknownMessage(typeByte, body)
// 	}
// }

// func ParseFrontend(typeByte byte, body []byte) (FrontendMessage, error) {
// 	switch typeByte {
// 	case 'B':
// 		return ParseBind(body)
// 	case 'D':
// 		return ParseDescribe(body)
// 	case 'E':
// 		return ParseExecute(body)
// 	case 'P':
// 		return ParseParse(body)
// 	case 'p':
// 		return ParsePasswordMessage(body)
// 	case 'Q':
// 		return ParseQuery(body)
// 	case 'S':
// 		return ParseSync(body)
// 	case 'X':
// 		return ParseTerminate(body)
// 	default:
// 		return ParseUnknownMessage(typeByte, body)
// 	}
// }

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
}

func (e *invalidMessageFormatErr) Error() string {
	return fmt.Sprintf("%s body is invalid", e.messageType)
}
