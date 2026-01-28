package pgproto3

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"strconv"
	"sync"
	"time"
)

// tracer traces the messages send to and from a Backend or Frontend. The format it produces roughly mimics the
// format produced by the libpq C function PQtrace.
type tracer struct {
	TracerOptions

	mux sync.Mutex
	w   io.Writer
	buf bytes.Buffer
}

// TracerOptions controls tracing behavior. It is roughly equivalent to the libpq function PQsetTraceFlags.
type TracerOptions struct {
	// SuppressTimestamps prevents printing of timestamps.
	SuppressTimestamps bool

	// RegressMode redacts fields that may be vary between executions.
	RegressMode bool
}

const timestampFormat = "2006-01-02 15:04:05.000000"

var (
	errUnclosedDoubleQuote = errors.New("unclosed double quote")
	errUnclosedSingleQuote = errors.New("unclosed single quote")
	errExpectedSingleQuote = errors.New("expected single quote")
)

// Parse parses a single trace line into its components.
// Returns the timestamp (zero if SuppressTimestamps was true), actor ('F' or 'B'),
// message type name, encoded message size, and the args portion (may be empty).
func (opts TracerOptions) Parse(line []byte) (timestamp time.Time, actor byte, msgType string, size int32, args []byte, err error) {
	// Parse fields by scanning for tabs manually to avoid allocation from bytes.Split
	data := line

	// Parse timestamp if present
	if !opts.SuppressTimestamps {
		tabIdx := indexByte(data, '\t')
		if tabIdx < 0 {
			return time.Time{}, 0, "", 0, nil, errors.New("invalid trace line: not enough fields")
		}
		timestamp, err = time.Parse(timestampFormat, string(data[:tabIdx]))
		if err != nil {
			return time.Time{}, 0, "", 0, nil, fmt.Errorf("invalid timestamp: %w", err)
		}
		data = data[tabIdx+1:]
	}

	// Parse actor
	if len(data) < 1 {
		return time.Time{}, 0, "", 0, nil, errors.New("invalid trace line: not enough fields")
	}
	actor = data[0]
	if actor != 'F' && actor != 'B' {
		return time.Time{}, 0, "", 0, nil, fmt.Errorf("invalid actor: expected 'F' or 'B', got '%c'", actor)
	}
	data = data[1:]

	// Expect tab after actor
	if len(data) == 0 || data[0] != '\t' {
		return time.Time{}, 0, "", 0, nil, errors.New("invalid actor: expected single character")
	}
	data = data[1:]

	// Parse message type
	tabIdx := indexByte(data, '\t')
	if tabIdx < 0 {
		return time.Time{}, 0, "", 0, nil, errors.New("invalid trace line: not enough fields")
	}
	msgType = string(data[:tabIdx])
	data = data[tabIdx+1:]

	// Parse size
	tabIdx = indexByte(data, '\t')
	var sizeBytes []byte
	if tabIdx < 0 {
		sizeBytes = data
		data = nil
	} else {
		sizeBytes = data[:tabIdx]
		data = data[tabIdx+1:]
	}
	sizeVal, err := strconv.ParseInt(string(sizeBytes), 10, 32)
	if err != nil {
		return time.Time{}, 0, "", 0, nil, fmt.Errorf("invalid size: %w", err)
	}
	size = int32(sizeVal)

	// Remaining data is args
	args = data

	return timestamp, actor, msgType, size, args, nil
}

// indexByte returns the index of the first instance of c in s, or -1 if c is not present.
func indexByte(s []byte, c byte) int {
	return bytes.IndexByte(s, c)
}

// ParseArgs returns an iterator over space-separated arguments in the args portion.
// Each value is unquoted and unescaped:
//   - Double-quoted strings: "value" → value (quotes removed)
//   - Single-quoted strings with hex escapes: 'hello\x0aworld' → hello\nworld (unescaped)
//   - Unquoted values returned as-is
func (opts TracerOptions) ParseArgs(args []byte) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		data := args

		for len(data) > 0 {
			// Skip leading spaces
			for len(data) > 0 && data[0] == ' ' {
				data = data[1:]
			}
			if len(data) == 0 {
				break
			}

			var value []byte
			var err error

			switch data[0] {
			case '"':
				// Double-quoted string: find closing quote
				end := bytes.IndexByte(data[1:], '"')
				if end < 0 {
					if !yield(nil, errUnclosedDoubleQuote) {
						return
					}
					return
				}
				value = data[1 : end+1]
				data = data[end+2:]

			case '\'':
				// Single-quoted string with hex escapes
				value, data, err = parseSingleQuoted(data)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					return
				}

			default:
				// Unquoted value: read until space
				end := bytes.IndexByte(data, ' ')
				if end < 0 {
					value = data
					data = nil
				} else {
					value = data[:end]
					data = data[end:]
				}
			}

			if !yield(value, nil) {
				return
			}
		}
	}
}

// parseSingleQuoted parses a single-quoted string with hex escapes.
// Returns the unescaped value, remaining data, and any error.
// Optimized to avoid allocations when there are no escape sequences.
func parseSingleQuoted(data []byte) (value []byte, remaining []byte, err error) {
	if len(data) == 0 || data[0] != '\'' {
		return nil, data, errExpectedSingleQuote
	}
	data = data[1:]

	// First, scan to find the closing quote and check if any escapes exist
	hasEscape := false
	closeIdx := -1
	for i := range data {
		if data[i] == '\'' {
			closeIdx = i
			break
		}
		if data[i] == '\\' && i+1 < len(data) && data[i+1] == 'x' {
			hasEscape = true
		}
	}

	if closeIdx < 0 {
		return nil, nil, errUnclosedSingleQuote
	}

	content := data[:closeIdx]
	remaining = data[closeIdx+1:]

	// Fast path: no escapes, return a subslice directly
	if !hasEscape {
		return content, remaining, nil
	}

	// Slow path: need to unescape
	// Pre-calculate the result size to avoid reallocations
	resultLen := 0
	for i := 0; i < len(content); i++ {
		if len(content) >= i+4 && content[i] == '\\' && content[i+1] == 'x' {
			resultLen++
			i += 3 // skip \xNN, loop will add 1 more
		} else {
			resultLen++
		}
	}

	result := make([]byte, 0, resultLen)
	var decoded [1]byte
	for i := 0; i < len(content); i++ {
		if len(content) >= i+4 && content[i] == '\\' && content[i+1] == 'x' {
			_, err := hex.Decode(decoded[:], content[i+2:i+4])
			if err != nil {
				return nil, data, fmt.Errorf("invalid hex escape: %w", err)
			}
			result = append(result, decoded[0])
			i += 3
		} else {
			result = append(result, content[i])
		}
	}

	return result, remaining, nil
}

func (t *tracer) traceMessage(sender byte, encodedLen int32, msg Message) {
	switch msg := msg.(type) {
	case *AuthenticationCleartextPassword:
		t.traceAuthenticationCleartextPassword(sender, encodedLen, msg)
	case *AuthenticationGSS:
		t.traceAuthenticationGSS(sender, encodedLen, msg)
	case *AuthenticationGSSContinue:
		t.traceAuthenticationGSSContinue(sender, encodedLen, msg)
	case *AuthenticationMD5Password:
		t.traceAuthenticationMD5Password(sender, encodedLen, msg)
	case *AuthenticationOk:
		t.traceAuthenticationOk(sender, encodedLen, msg)
	case *AuthenticationSASL:
		t.traceAuthenticationSASL(sender, encodedLen, msg)
	case *AuthenticationSASLContinue:
		t.traceAuthenticationSASLContinue(sender, encodedLen, msg)
	case *AuthenticationSASLFinal:
		t.traceAuthenticationSASLFinal(sender, encodedLen, msg)
	case *BackendKeyData:
		t.traceBackendKeyData(sender, encodedLen, msg)
	case *Bind:
		t.traceBind(sender, encodedLen, msg)
	case *BindComplete:
		t.traceBindComplete(sender, encodedLen, msg)
	case *CancelRequest:
		t.traceCancelRequest(sender, encodedLen, msg)
	case *Close:
		t.traceClose(sender, encodedLen, msg)
	case *CloseComplete:
		t.traceCloseComplete(sender, encodedLen, msg)
	case *CommandComplete:
		t.traceCommandComplete(sender, encodedLen, msg)
	case *CopyBothResponse:
		t.traceCopyBothResponse(sender, encodedLen, msg)
	case *CopyData:
		t.traceCopyData(sender, encodedLen, msg)
	case *CopyDone:
		t.traceCopyDone(sender, encodedLen, msg)
	case *CopyFail:
		t.traceCopyFail(sender, encodedLen, msg)
	case *CopyInResponse:
		t.traceCopyInResponse(sender, encodedLen, msg)
	case *CopyOutResponse:
		t.traceCopyOutResponse(sender, encodedLen, msg)
	case *DataRow:
		t.traceDataRow(sender, encodedLen, msg)
	case *Describe:
		t.traceDescribe(sender, encodedLen, msg)
	case *EmptyQueryResponse:
		t.traceEmptyQueryResponse(sender, encodedLen, msg)
	case *ErrorResponse:
		t.traceErrorResponse(sender, encodedLen, msg)
	case *Execute:
		t.TraceQueryute(sender, encodedLen, msg)
	case *Flush:
		t.traceFlush(sender, encodedLen, msg)
	case *FunctionCall:
		t.traceFunctionCall(sender, encodedLen, msg)
	case *FunctionCallResponse:
		t.traceFunctionCallResponse(sender, encodedLen, msg)
	case *GSSEncRequest:
		t.traceGSSEncRequest(sender, encodedLen, msg)
	case *NoData:
		t.traceNoData(sender, encodedLen, msg)
	case *NoticeResponse:
		t.traceNoticeResponse(sender, encodedLen, msg)
	case *NotificationResponse:
		t.traceNotificationResponse(sender, encodedLen, msg)
	case *ParameterDescription:
		t.traceParameterDescription(sender, encodedLen, msg)
	case *ParameterStatus:
		t.traceParameterStatus(sender, encodedLen, msg)
	case *Parse:
		t.traceParse(sender, encodedLen, msg)
	case *ParseComplete:
		t.traceParseComplete(sender, encodedLen, msg)
	case *PortalSuspended:
		t.tracePortalSuspended(sender, encodedLen, msg)
	case *Query:
		t.traceQuery(sender, encodedLen, msg)
	case *ReadyForQuery:
		t.traceReadyForQuery(sender, encodedLen, msg)
	case *RowDescription:
		t.traceRowDescription(sender, encodedLen, msg)
	case *SSLRequest:
		t.traceSSLRequest(sender, encodedLen, msg)
	case *StartupMessage:
		t.traceStartupMessage(sender, encodedLen, msg)
	case *Sync:
		t.traceSync(sender, encodedLen, msg)
	case *Terminate:
		t.traceTerminate(sender, encodedLen, msg)
	default:
		t.writeTrace(sender, encodedLen, "Unknown", nil)
	}
}

func (t *tracer) traceAuthenticationCleartextPassword(sender byte, encodedLen int32, msg *AuthenticationCleartextPassword) {
	t.writeTrace(sender, encodedLen, "AuthenticationCleartextPassword", nil)
}

func (t *tracer) traceAuthenticationGSS(sender byte, encodedLen int32, msg *AuthenticationGSS) {
	t.writeTrace(sender, encodedLen, "AuthenticationGSS", nil)
}

func (t *tracer) traceAuthenticationGSSContinue(sender byte, encodedLen int32, msg *AuthenticationGSSContinue) {
	t.writeTrace(sender, encodedLen, "AuthenticationGSSContinue", nil)
}

func (t *tracer) traceAuthenticationMD5Password(sender byte, encodedLen int32, msg *AuthenticationMD5Password) {
	t.writeTrace(sender, encodedLen, "AuthenticationMD5Password", nil)
}

func (t *tracer) traceAuthenticationOk(sender byte, encodedLen int32, msg *AuthenticationOk) {
	t.writeTrace(sender, encodedLen, "AuthenticationOk", nil)
}

func (t *tracer) traceAuthenticationSASL(sender byte, encodedLen int32, msg *AuthenticationSASL) {
	t.writeTrace(sender, encodedLen, "AuthenticationSASL", nil)
}

func (t *tracer) traceAuthenticationSASLContinue(sender byte, encodedLen int32, msg *AuthenticationSASLContinue) {
	t.writeTrace(sender, encodedLen, "AuthenticationSASLContinue", nil)
}

func (t *tracer) traceAuthenticationSASLFinal(sender byte, encodedLen int32, msg *AuthenticationSASLFinal) {
	t.writeTrace(sender, encodedLen, "AuthenticationSASLFinal", nil)
}

func (t *tracer) traceBackendKeyData(sender byte, encodedLen int32, msg *BackendKeyData) {
	t.writeTrace(sender, encodedLen, "BackendKeyData", func() {
		if t.RegressMode {
			t.buf.WriteString("\t NNNN NNNN")
		} else {
			fmt.Fprintf(&t.buf, "\t %d %d", msg.ProcessID, msg.SecretKey)
		}
	})
}

func (t *tracer) traceBind(sender byte, encodedLen int32, msg *Bind) {
	t.writeTrace(sender, encodedLen, "Bind", func() {
		fmt.Fprintf(&t.buf, "\t %s %s %d", doubleQuotedString{&msg.DestinationPortal}, doubleQuotedString{&msg.PreparedStatement}, len(msg.ParameterFormatCodes))
		for _, fc := range msg.ParameterFormatCodes {
			fmt.Fprintf(&t.buf, " %d", fc)
		}
		fmt.Fprintf(&t.buf, " %d", len(msg.Parameters))
		for i := range msg.Parameters {
			fmt.Fprintf(&t.buf, " %s", singleQuotedEscaped{&msg.Parameters[i]})
		}
		fmt.Fprintf(&t.buf, " %d", len(msg.ResultFormatCodes))
		for _, fc := range msg.ResultFormatCodes {
			fmt.Fprintf(&t.buf, " %d", fc)
		}
	})
}

func (t *tracer) traceBindComplete(sender byte, encodedLen int32, msg *BindComplete) {
	t.writeTrace(sender, encodedLen, "BindComplete", nil)
}

func (t *tracer) traceCancelRequest(sender byte, encodedLen int32, msg *CancelRequest) {
	t.writeTrace(sender, encodedLen, "CancelRequest", nil)
}

func (t *tracer) traceClose(sender byte, encodedLen int32, msg *Close) {
	t.writeTrace(sender, encodedLen, "Close", nil)
}

func (t *tracer) traceCloseComplete(sender byte, encodedLen int32, msg *CloseComplete) {
	t.writeTrace(sender, encodedLen, "CloseComplete", nil)
}

func (t *tracer) traceCommandComplete(sender byte, encodedLen int32, msg *CommandComplete) {
	t.writeTrace(sender, encodedLen, "CommandComplete", func() {
		fmt.Fprintf(&t.buf, "\t %s", doubleQuotedBytes{&msg.CommandTag})
	})
}

func (t *tracer) traceCopyBothResponse(sender byte, encodedLen int32, msg *CopyBothResponse) {
	t.writeTrace(sender, encodedLen, "CopyBothResponse", nil)
}

func (t *tracer) traceCopyData(sender byte, encodedLen int32, msg *CopyData) {
	t.writeTrace(sender, encodedLen, "CopyData", nil)
}

func (t *tracer) traceCopyDone(sender byte, encodedLen int32, msg *CopyDone) {
	t.writeTrace(sender, encodedLen, "CopyDone", nil)
}

func (t *tracer) traceCopyFail(sender byte, encodedLen int32, msg *CopyFail) {
	t.writeTrace(sender, encodedLen, "CopyFail", func() {
		fmt.Fprintf(&t.buf, "\t %s", doubleQuotedString{&msg.Message})
	})
}

func (t *tracer) traceCopyInResponse(sender byte, encodedLen int32, msg *CopyInResponse) {
	t.writeTrace(sender, encodedLen, "CopyInResponse", nil)
}

func (t *tracer) traceCopyOutResponse(sender byte, encodedLen int32, msg *CopyOutResponse) {
	t.writeTrace(sender, encodedLen, "CopyOutResponse", nil)
}

func (t *tracer) traceDataRow(sender byte, encodedLen int32, msg *DataRow) {
	t.writeTrace(sender, encodedLen, "DataRow", func() {
		fmt.Fprintf(&t.buf, "\t %d", len(msg.Values))
		for i := range msg.Values {
			if msg.Values[i] == nil {
				t.buf.WriteString(" -1")
			} else {
				fmt.Fprintf(&t.buf, " %d %s", len(msg.Values[i]), singleQuotedEscaped{&msg.Values[i]})
			}
		}
	})
}

func (t *tracer) traceDescribe(sender byte, encodedLen int32, msg *Describe) {
	t.writeTrace(sender, encodedLen, "Describe", func() {
		fmt.Fprintf(&t.buf, "\t %c %s", msg.ObjectType, doubleQuotedString{&msg.Name})
	})
}

func (t *tracer) traceEmptyQueryResponse(sender byte, encodedLen int32, msg *EmptyQueryResponse) {
	t.writeTrace(sender, encodedLen, "EmptyQueryResponse", nil)
}

func (t *tracer) traceErrorResponse(sender byte, encodedLen int32, msg *ErrorResponse) {
	t.writeTrace(sender, encodedLen, "ErrorResponse", nil)
}

func (t *tracer) TraceQueryute(sender byte, encodedLen int32, msg *Execute) {
	t.writeTrace(sender, encodedLen, "Execute", func() {
		fmt.Fprintf(&t.buf, "\t %s %d", doubleQuotedString{&msg.Portal}, msg.MaxRows)
	})
}

func (t *tracer) traceFlush(sender byte, encodedLen int32, msg *Flush) {
	t.writeTrace(sender, encodedLen, "Flush", nil)
}

func (t *tracer) traceFunctionCall(sender byte, encodedLen int32, msg *FunctionCall) {
	t.writeTrace(sender, encodedLen, "FunctionCall", nil)
}

func (t *tracer) traceFunctionCallResponse(sender byte, encodedLen int32, msg *FunctionCallResponse) {
	t.writeTrace(sender, encodedLen, "FunctionCallResponse", nil)
}

func (t *tracer) traceGSSEncRequest(sender byte, encodedLen int32, msg *GSSEncRequest) {
	t.writeTrace(sender, encodedLen, "GSSEncRequest", nil)
}

func (t *tracer) traceNoData(sender byte, encodedLen int32, msg *NoData) {
	t.writeTrace(sender, encodedLen, "NoData", nil)
}

func (t *tracer) traceNoticeResponse(sender byte, encodedLen int32, msg *NoticeResponse) {
	t.writeTrace(sender, encodedLen, "NoticeResponse", nil)
}

func (t *tracer) traceNotificationResponse(sender byte, encodedLen int32, msg *NotificationResponse) {
	t.writeTrace(sender, encodedLen, "NotificationResponse", func() {
		fmt.Fprintf(&t.buf, "\t %d %s %s", msg.PID, doubleQuotedString{&msg.Channel}, doubleQuotedString{&msg.Payload})
	})
}

func (t *tracer) traceParameterDescription(sender byte, encodedLen int32, msg *ParameterDescription) {
	t.writeTrace(sender, encodedLen, "ParameterDescription", nil)
}

func (t *tracer) traceParameterStatus(sender byte, encodedLen int32, msg *ParameterStatus) {
	t.writeTrace(sender, encodedLen, "ParameterStatus", func() {
		fmt.Fprintf(&t.buf, "\t %s %s", doubleQuotedString{&msg.Name}, doubleQuotedString{&msg.Value})
	})
}

func (t *tracer) traceParse(sender byte, encodedLen int32, msg *Parse) {
	t.writeTrace(sender, encodedLen, "Parse", func() {
		fmt.Fprintf(&t.buf, "\t %s %s %d", doubleQuotedString{&msg.Name}, doubleQuotedString{&msg.Query}, len(msg.ParameterOIDs))
		for _, oid := range msg.ParameterOIDs {
			fmt.Fprintf(&t.buf, " %d", oid)
		}
	})
}

func (t *tracer) traceParseComplete(sender byte, encodedLen int32, msg *ParseComplete) {
	t.writeTrace(sender, encodedLen, "ParseComplete", nil)
}

func (t *tracer) tracePortalSuspended(sender byte, encodedLen int32, msg *PortalSuspended) {
	t.writeTrace(sender, encodedLen, "PortalSuspended", nil)
}

func (t *tracer) traceQuery(sender byte, encodedLen int32, msg *Query) {
	t.writeTrace(sender, encodedLen, "Query", func() {
		fmt.Fprintf(&t.buf, "\t %s", doubleQuotedString{&msg.String})
	})
}

func (t *tracer) traceReadyForQuery(sender byte, encodedLen int32, msg *ReadyForQuery) {
	t.writeTrace(sender, encodedLen, "ReadyForQuery", func() {
		fmt.Fprintf(&t.buf, "\t %c", msg.TxStatus)
	})
}

func (t *tracer) traceRowDescription(sender byte, encodedLen int32, msg *RowDescription) {
	t.writeTrace(sender, encodedLen, "RowDescription", func() {
		fmt.Fprintf(&t.buf, "\t %d", len(msg.Fields))
		for i := range msg.Fields {
			fmt.Fprintf(&t.buf, ` %s %d %d %d %d %d %d`, doubleQuotedBytes{&msg.Fields[i].Name}, msg.Fields[i].TableOID, msg.Fields[i].TableAttributeNumber, msg.Fields[i].DataTypeOID, msg.Fields[i].DataTypeSize, msg.Fields[i].TypeModifier, msg.Fields[i].Format)
		}
	})
}

func (t *tracer) traceSSLRequest(sender byte, encodedLen int32, msg *SSLRequest) {
	t.writeTrace(sender, encodedLen, "SSLRequest", nil)
}

func (t *tracer) traceStartupMessage(sender byte, encodedLen int32, msg *StartupMessage) {
	t.writeTrace(sender, encodedLen, "StartupMessage", nil)
}

func (t *tracer) traceSync(sender byte, encodedLen int32, msg *Sync) {
	t.writeTrace(sender, encodedLen, "Sync", nil)
}

func (t *tracer) traceTerminate(sender byte, encodedLen int32, msg *Terminate) {
	t.writeTrace(sender, encodedLen, "Terminate", nil)
}

func (t *tracer) writeTrace(sender byte, encodedLen int32, msgType string, writeDetails func()) {
	t.mux.Lock()
	defer t.mux.Unlock()
	defer func() {
		if t.buf.Cap() > 1024 {
			t.buf = bytes.Buffer{}
		} else {
			t.buf.Reset()
		}
	}()

	if !t.SuppressTimestamps {
		now := time.Now()
		t.buf.Write(now.AppendFormat(t.buf.AvailableBuffer(), timestampFormat))
		t.buf.WriteByte('\t')
	}

	t.buf.WriteByte(sender)
	t.buf.WriteByte('\t')
	t.buf.WriteString(msgType)
	t.buf.WriteByte('\t')
	t.buf.Write(strconv.AppendInt(t.buf.AvailableBuffer(), int64(encodedLen), 10))

	if writeDetails != nil {
		writeDetails()
	}

	t.buf.WriteByte('\n')
	t.buf.WriteTo(t.w)
}

// doubleQuotedString wraps a pointer to string for zero-copy double-quoted formatting.
// Using a pointer avoids copying the string header when passed to fmt.Fprintf.
type doubleQuotedString struct{ data *string }

func (dq doubleQuotedString) Format(f fmt.State, verb rune) {
	io.WriteString(f, `"`)
	io.WriteString(f, *dq.data)
	io.WriteString(f, `"`)
}

// doubleQuotedBytes wraps a pointer to []byte for zero-copy double-quoted formatting.
// Using a pointer avoids copying the slice header when passed to fmt.Fprintf.
type doubleQuotedBytes struct{ data *[]byte }

func (dq doubleQuotedBytes) Format(f fmt.State, verb rune) {
	io.WriteString(f, `"`)
	f.Write(*dq.data)
	io.WriteString(f, `"`)
}

// singleQuotedEscaped wraps a pointer to []byte for zero-copy single-quoted formatting
// with hex escaping for non-printable characters (libpq style).
type singleQuotedEscaped struct{ data *[]byte }

func (sq singleQuotedEscaped) Format(f fmt.State, verb rune) {
	io.WriteString(f, `'`)

	data := *sq.data
	for len(data) > 0 {
		i := 0
		for i < len(data) && data[i] >= 32 && data[i] <= 126 {
			i++
		}
		if i > 0 {
			f.Write(data[:i])
			data = data[i:]
		}
		if len(data) > 0 && (data[0] < 32 || data[0] > 126) {
			fmt.Fprintf(f, `\x%x`, data[0])
			data = data[1:]
		}
	}

	io.WriteString(f, `'`)
}
