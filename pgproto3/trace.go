package pgproto3

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"
)

// MessageTracer is an interface that traces the messages send to and from a Backend or Frontend.
type MessageTracer interface {
	// TraceMessage tracks the sending or receiving of a message. sender is either 'F' for frontend or 'B' for backend.
	TraceMessage(sender byte, encodedLen int32, msg Message)
}

// LibpqMessageTracer is a MessageTracer that roughly mimics the format produced by the libpq C function PQtrace.
type LibpqMessageTracer struct {
	Writer io.Writer

	// SuppressTimestamps prevents printing of timestamps.
	SuppressTimestamps bool

	// RegressMode redacts fields that may be vary between executions.
	RegressMode bool
}

func (t *LibpqMessageTracer) TraceMessage(sender byte, encodedLen int32, msg Message) {
	buf := &bytes.Buffer{}

	if !t.SuppressTimestamps {
		now := time.Now()
		buf.WriteString(now.Format("2006-01-02 15:04:05.000000"))
		buf.WriteByte('\t')
	}

	buf.WriteByte(sender)
	buf.WriteByte('\t')

	switch msg := msg.(type) {
	case *AuthenticationCleartextPassword:
		buf.WriteString("AuthenticationCleartextPassword")
	case *AuthenticationGSS:
		buf.WriteString("AuthenticationGSS")
	case *AuthenticationGSSContinue:
		buf.WriteString("AuthenticationGSSContinue")
	case *AuthenticationMD5Password:
		buf.WriteString("AuthenticationMD5Password")
	case *AuthenticationOk:
		buf.WriteString("AuthenticationOk")
	case *AuthenticationSASL:
		buf.WriteString("AuthenticationSASL")
	case *AuthenticationSASLContinue:
		buf.WriteString("AuthenticationSASLContinue")
	case *AuthenticationSASLFinal:
		buf.WriteString("AuthenticationSASLFinal")
	case *BackendKeyData:
		if t.RegressMode {
			buf.WriteString("BackendKeyData\t NNNN NNNN")
		} else {
			fmt.Fprintf(buf, "BackendKeyData\t %d %d", msg.ProcessID, msg.SecretKey)
		}
	case *Bind:
		fmt.Fprintf(buf, "Bind\t %s %s %d", traceDoubleQuotedString([]byte(msg.DestinationPortal)), traceDoubleQuotedString([]byte(msg.PreparedStatement)), len(msg.ParameterFormatCodes))
		for _, fc := range msg.ParameterFormatCodes {
			fmt.Fprintf(buf, " %d", fc)
		}
		fmt.Fprintf(buf, " %d", len(msg.Parameters))
		for _, p := range msg.Parameters {
			fmt.Fprintf(buf, " %s", traceSingleQuotedString(p))
		}
		fmt.Fprintf(buf, " %d", len(msg.ResultFormatCodes))
		for _, fc := range msg.ResultFormatCodes {
			fmt.Fprintf(buf, " %d", fc)
		}
	case *BindComplete:
		buf.WriteString("BindComplete")
	case *CancelRequest:
		buf.WriteString("CancelRequest")
	case *Close:
		buf.WriteString("Close")
	case *CloseComplete:
		buf.WriteString("CloseComplete")
	case *CommandComplete:
		fmt.Fprintf(buf, "CommandComplete\t %s", traceDoubleQuotedString(msg.CommandTag))
	case *CopyBothResponse:
		buf.WriteString("CopyBothResponse")
	case *CopyData:
		buf.WriteString("CopyData")
	case *CopyDone:
		buf.WriteString("CopyDone")
	case *CopyFail:
		fmt.Fprintf(buf, "CopyFail\t %s", traceDoubleQuotedString([]byte(msg.Message)))
	case *CopyInResponse:
		buf.WriteString("CopyInResponse")
	case *CopyOutResponse:
		buf.WriteString("CopyOutResponse")
	case *DataRow:
		fmt.Fprintf(buf, "DataRow\t %d", len(msg.Values))
		for _, v := range msg.Values {
			if v == nil {
				buf.WriteString(" -1")
			} else {
				fmt.Fprintf(buf, " %d %s", len(v), traceSingleQuotedString(v))
			}
		}
	case *Describe:
		fmt.Fprintf(buf, "Describe\t %c %s", msg.ObjectType, traceDoubleQuotedString([]byte(msg.Name)))
	case *EmptyQueryResponse:
		buf.WriteString("EmptyQueryResponse")
	case *ErrorResponse:
		buf.WriteString("ErrorResponse")
	case *Execute:
		fmt.Fprintf(buf, "Execute\t %s %d", traceDoubleQuotedString([]byte(msg.Portal)), msg.MaxRows)
	case *Flush:
		buf.WriteString("Flush")
	case *FunctionCall:
		buf.WriteString("FunctionCall")
	case *FunctionCallResponse:
		buf.WriteString("FunctionCallResponse")
	case *GSSEncRequest:
		buf.WriteString("GSSEncRequest")
	case *NoData:
		buf.WriteString("NoData")
	case *NoticeResponse:
		buf.WriteString("NoticeResponse")
	case *NotificationResponse:
		fmt.Fprintf(buf, "NotificationResponse\t %d %s %s", msg.PID, traceDoubleQuotedString([]byte(msg.Channel)), traceDoubleQuotedString([]byte(msg.Payload)))
	case *ParameterDescription:
		buf.WriteString("ParameterDescription")
	case *ParameterStatus:
		fmt.Fprintf(buf, "ParameterStatus\t %s %s", traceDoubleQuotedString([]byte(msg.Name)), traceDoubleQuotedString([]byte(msg.Value)))
	case *Parse:
		fmt.Fprintf(buf, "Parse\t %s %s %d", traceDoubleQuotedString([]byte(msg.Name)), traceDoubleQuotedString([]byte(msg.Query)), len(msg.ParameterOIDs))
		for _, oid := range msg.ParameterOIDs {
			fmt.Fprintf(buf, " %d", oid)
		}
	case *ParseComplete:
		buf.WriteString("ParseComplete")
	case *PortalSuspended:
		buf.WriteString("PortalSuspended")
	case *Query:
		buf.WriteString("Query\t")
		fmt.Fprintf(buf, ` "%s"`, msg.String)
	case *ReadyForQuery:
		fmt.Fprintf(buf, "ReadyForQuery\t %c", msg.TxStatus)
	case *RowDescription:
		buf.WriteString("RowDescription\t")
		fmt.Fprintf(buf, " %d", len(msg.Fields))
		for _, fd := range msg.Fields {
			fmt.Fprintf(buf, ` %s %d %d %d %d %d %d`, traceDoubleQuotedString(fd.Name), fd.TableOID, fd.TableAttributeNumber, fd.DataTypeOID, fd.DataTypeSize, fd.TypeModifier, fd.Format)
		}
	case *SSLRequest:
		buf.WriteString("SSLRequest")
	case *StartupMessage:
		buf.WriteString("StartupMessage")
	case *Sync:
		buf.WriteString("Sync")
	case *Terminate:
		buf.WriteString("Terminate")
	default:
		buf.WriteString("Unknown")
	}

	buf.WriteByte('\n')
	buf.WriteTo(t.Writer)
}

// traceDoubleQuotedString returns buf as a double-quoted string without any escaping. It is roughly equivalent to
// pqTraceOutputString in libpq.
func traceDoubleQuotedString(buf []byte) string {
	return `"` + string(buf) + `"`
}

// traceSingleQuotedString returns buf as a single-quoted string with non-printable characters hex-escaped. It is
// roughly equivalent to pqTraceOutputNchar in libpq.
func traceSingleQuotedString(buf []byte) string {
	sb := &strings.Builder{}

	sb.WriteByte('\'')
	for _, b := range buf {
		if b < 32 || b > 126 {
			fmt.Fprintf(sb, `\x%x`, b)
		} else {
			sb.WriteByte(b)
		}
	}
	sb.WriteByte('\'')

	return sb.String()
}
