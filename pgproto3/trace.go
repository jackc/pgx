package pgproto3

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// tracer traces the messages send to and from a Backend or Frontend. The format it produces roughly mimics the
// format produced by the libpq C function PQtrace.
type tracer struct {
	w   io.Writer
	buf *bytes.Buffer
	TracerOptions
}

// TracerOptions controls tracing behavior. It is roughly equivalent to the libpq function PQsetTraceFlags.
type TracerOptions struct {
	// SuppressTimestamps prevents printing of timestamps.
	SuppressTimestamps bool

	// RegressMode redacts fields that may be vary between executions.
	RegressMode bool
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
		t.beginTrace(sender, encodedLen, "Unknown")
		t.finishTrace()
	}
}

func (t *tracer) traceAuthenticationCleartextPassword(sender byte, encodedLen int32, msg *AuthenticationCleartextPassword) {
	t.beginTrace(sender, encodedLen, "AuthenticationCleartextPassword")
	t.finishTrace()
}

func (t *tracer) traceAuthenticationGSS(sender byte, encodedLen int32, msg *AuthenticationGSS) {
	t.beginTrace(sender, encodedLen, "AuthenticationGSS")
	t.finishTrace()
}

func (t *tracer) traceAuthenticationGSSContinue(sender byte, encodedLen int32, msg *AuthenticationGSSContinue) {
	t.beginTrace(sender, encodedLen, "AuthenticationGSSContinue")
	t.finishTrace()
}

func (t *tracer) traceAuthenticationMD5Password(sender byte, encodedLen int32, msg *AuthenticationMD5Password) {
	t.beginTrace(sender, encodedLen, "AuthenticationMD5Password")
	t.finishTrace()
}

func (t *tracer) traceAuthenticationOk(sender byte, encodedLen int32, msg *AuthenticationOk) {
	t.beginTrace(sender, encodedLen, "AuthenticationOk")
	t.finishTrace()
}

func (t *tracer) traceAuthenticationSASL(sender byte, encodedLen int32, msg *AuthenticationSASL) {
	t.beginTrace(sender, encodedLen, "AuthenticationSASL")
	t.finishTrace()
}

func (t *tracer) traceAuthenticationSASLContinue(sender byte, encodedLen int32, msg *AuthenticationSASLContinue) {
	t.beginTrace(sender, encodedLen, "AuthenticationSASLContinue")
	t.finishTrace()
}

func (t *tracer) traceAuthenticationSASLFinal(sender byte, encodedLen int32, msg *AuthenticationSASLFinal) {
	t.beginTrace(sender, encodedLen, "AuthenticationSASLFinal")
	t.finishTrace()
}

func (t *tracer) traceBackendKeyData(sender byte, encodedLen int32, msg *BackendKeyData) {
	t.beginTrace(sender, encodedLen, "BackendKeyData")
	if t.RegressMode {
		t.buf.WriteString("\t NNNN NNNN")
	} else {
		fmt.Fprintf(t.buf, "\t %d %d", msg.ProcessID, msg.SecretKey)
	}
	t.finishTrace()
}

func (t *tracer) traceBind(sender byte, encodedLen int32, msg *Bind) {
	t.beginTrace(sender, encodedLen, "Bind")
	fmt.Fprintf(t.buf, "\t %s %s %d", traceDoubleQuotedString([]byte(msg.DestinationPortal)), traceDoubleQuotedString([]byte(msg.PreparedStatement)), len(msg.ParameterFormatCodes))
	for _, fc := range msg.ParameterFormatCodes {
		fmt.Fprintf(t.buf, " %d", fc)
	}
	fmt.Fprintf(t.buf, " %d", len(msg.Parameters))
	for _, p := range msg.Parameters {
		fmt.Fprintf(t.buf, " %s", traceSingleQuotedString(p))
	}
	fmt.Fprintf(t.buf, " %d", len(msg.ResultFormatCodes))
	for _, fc := range msg.ResultFormatCodes {
		fmt.Fprintf(t.buf, " %d", fc)
	}
	t.finishTrace()
}

func (t *tracer) traceBindComplete(sender byte, encodedLen int32, msg *BindComplete) {
	t.beginTrace(sender, encodedLen, "BindComplete")
	t.finishTrace()
}

func (t *tracer) traceCancelRequest(sender byte, encodedLen int32, msg *CancelRequest) {
	t.beginTrace(sender, encodedLen, "CancelRequest")
	t.finishTrace()
}

func (t *tracer) traceClose(sender byte, encodedLen int32, msg *Close) {
	t.beginTrace(sender, encodedLen, "Close")
	t.finishTrace()
}

func (t *tracer) traceCloseComplete(sender byte, encodedLen int32, msg *CloseComplete) {
	t.beginTrace(sender, encodedLen, "CloseComplete")
	t.finishTrace()
}

func (t *tracer) traceCommandComplete(sender byte, encodedLen int32, msg *CommandComplete) {
	t.beginTrace(sender, encodedLen, "CommandComplete")
	fmt.Fprintf(t.buf, "\t %s", traceDoubleQuotedString(msg.CommandTag))
	t.finishTrace()
}

func (t *tracer) traceCopyBothResponse(sender byte, encodedLen int32, msg *CopyBothResponse) {
	t.beginTrace(sender, encodedLen, "CopyBothResponse")
	t.finishTrace()
}

func (t *tracer) traceCopyData(sender byte, encodedLen int32, msg *CopyData) {
	t.beginTrace(sender, encodedLen, "CopyData")
	t.finishTrace()
}

func (t *tracer) traceCopyDone(sender byte, encodedLen int32, msg *CopyDone) {
	t.beginTrace(sender, encodedLen, "CopyDone")
	t.finishTrace()
}

func (t *tracer) traceCopyFail(sender byte, encodedLen int32, msg *CopyFail) {
	t.beginTrace(sender, encodedLen, "CopyFail")
	fmt.Fprintf(t.buf, "\t %s", traceDoubleQuotedString([]byte(msg.Message)))
	t.finishTrace()
}

func (t *tracer) traceCopyInResponse(sender byte, encodedLen int32, msg *CopyInResponse) {
	t.beginTrace(sender, encodedLen, "CopyInResponse")
	t.finishTrace()
}

func (t *tracer) traceCopyOutResponse(sender byte, encodedLen int32, msg *CopyOutResponse) {
	t.beginTrace(sender, encodedLen, "CopyOutResponse")
	t.finishTrace()
}

func (t *tracer) traceDataRow(sender byte, encodedLen int32, msg *DataRow) {
	t.beginTrace(sender, encodedLen, "DataRow")
	fmt.Fprintf(t.buf, "\t %d", len(msg.Values))
	for _, v := range msg.Values {
		if v == nil {
			t.buf.WriteString(" -1")
		} else {
			fmt.Fprintf(t.buf, " %d %s", len(v), traceSingleQuotedString(v))
		}
	}
	t.finishTrace()
}

func (t *tracer) traceDescribe(sender byte, encodedLen int32, msg *Describe) {
	t.beginTrace(sender, encodedLen, "Describe")
	fmt.Fprintf(t.buf, "\t %c %s", msg.ObjectType, traceDoubleQuotedString([]byte(msg.Name)))
	t.finishTrace()
}

func (t *tracer) traceEmptyQueryResponse(sender byte, encodedLen int32, msg *EmptyQueryResponse) {
	t.beginTrace(sender, encodedLen, "EmptyQueryResponse")
	t.finishTrace()
}

func (t *tracer) traceErrorResponse(sender byte, encodedLen int32, msg *ErrorResponse) {
	t.beginTrace(sender, encodedLen, "ErrorResponse")
	t.finishTrace()
}

func (t *tracer) TraceQueryute(sender byte, encodedLen int32, msg *Execute) {
	t.beginTrace(sender, encodedLen, "Execute")
	fmt.Fprintf(t.buf, "\t %s %d", traceDoubleQuotedString([]byte(msg.Portal)), msg.MaxRows)
	t.finishTrace()
}

func (t *tracer) traceFlush(sender byte, encodedLen int32, msg *Flush) {
	t.beginTrace(sender, encodedLen, "Flush")
	t.finishTrace()
}

func (t *tracer) traceFunctionCall(sender byte, encodedLen int32, msg *FunctionCall) {
	t.beginTrace(sender, encodedLen, "FunctionCall")
	t.finishTrace()
}

func (t *tracer) traceFunctionCallResponse(sender byte, encodedLen int32, msg *FunctionCallResponse) {
	t.beginTrace(sender, encodedLen, "FunctionCallResponse")
	t.finishTrace()
}

func (t *tracer) traceGSSEncRequest(sender byte, encodedLen int32, msg *GSSEncRequest) {
	t.beginTrace(sender, encodedLen, "GSSEncRequest")
	t.finishTrace()
}

func (t *tracer) traceNoData(sender byte, encodedLen int32, msg *NoData) {
	t.beginTrace(sender, encodedLen, "NoData")
	t.finishTrace()
}

func (t *tracer) traceNoticeResponse(sender byte, encodedLen int32, msg *NoticeResponse) {
	t.beginTrace(sender, encodedLen, "NoticeResponse")
	t.finishTrace()
}

func (t *tracer) traceNotificationResponse(sender byte, encodedLen int32, msg *NotificationResponse) {
	t.beginTrace(sender, encodedLen, "NotificationResponse")
	fmt.Fprintf(t.buf, "\t %d %s %s", msg.PID, traceDoubleQuotedString([]byte(msg.Channel)), traceDoubleQuotedString([]byte(msg.Payload)))
	t.finishTrace()
}

func (t *tracer) traceParameterDescription(sender byte, encodedLen int32, msg *ParameterDescription) {
	t.beginTrace(sender, encodedLen, "ParameterDescription")
	t.finishTrace()
}

func (t *tracer) traceParameterStatus(sender byte, encodedLen int32, msg *ParameterStatus) {
	t.beginTrace(sender, encodedLen, "ParameterStatus")
	fmt.Fprintf(t.buf, "\t %s %s", traceDoubleQuotedString([]byte(msg.Name)), traceDoubleQuotedString([]byte(msg.Value)))
	t.finishTrace()
}

func (t *tracer) traceParse(sender byte, encodedLen int32, msg *Parse) {
	t.beginTrace(sender, encodedLen, "Parse")
	fmt.Fprintf(t.buf, "\t %s %s %d", traceDoubleQuotedString([]byte(msg.Name)), traceDoubleQuotedString([]byte(msg.Query)), len(msg.ParameterOIDs))
	for _, oid := range msg.ParameterOIDs {
		fmt.Fprintf(t.buf, " %d", oid)
	}
	t.finishTrace()
}

func (t *tracer) traceParseComplete(sender byte, encodedLen int32, msg *ParseComplete) {
	t.beginTrace(sender, encodedLen, "ParseComplete")
	t.finishTrace()
}

func (t *tracer) tracePortalSuspended(sender byte, encodedLen int32, msg *PortalSuspended) {
	t.beginTrace(sender, encodedLen, "PortalSuspended")
	t.finishTrace()
}

func (t *tracer) traceQuery(sender byte, encodedLen int32, msg *Query) {
	t.beginTrace(sender, encodedLen, "Query")
	fmt.Fprintf(t.buf, "\t %s", traceDoubleQuotedString([]byte(msg.String)))
	t.finishTrace()
}

func (t *tracer) traceReadyForQuery(sender byte, encodedLen int32, msg *ReadyForQuery) {
	t.beginTrace(sender, encodedLen, "ReadyForQuery")
	fmt.Fprintf(t.buf, "\t %c", msg.TxStatus)
	t.finishTrace()
}

func (t *tracer) traceRowDescription(sender byte, encodedLen int32, msg *RowDescription) {
	t.beginTrace(sender, encodedLen, "RowDescription")
	fmt.Fprintf(t.buf, "\t %d", len(msg.Fields))
	for _, fd := range msg.Fields {
		fmt.Fprintf(t.buf, ` %s %d %d %d %d %d %d`, traceDoubleQuotedString(fd.Name), fd.TableOID, fd.TableAttributeNumber, fd.DataTypeOID, fd.DataTypeSize, fd.TypeModifier, fd.Format)
	}
	t.finishTrace()
}

func (t *tracer) traceSSLRequest(sender byte, encodedLen int32, msg *SSLRequest) {
	t.beginTrace(sender, encodedLen, "SSLRequest")
	t.finishTrace()
}

func (t *tracer) traceStartupMessage(sender byte, encodedLen int32, msg *StartupMessage) {
	t.beginTrace(sender, encodedLen, "StartupMessage")
	t.finishTrace()
}

func (t *tracer) traceSync(sender byte, encodedLen int32, msg *Sync) {
	t.beginTrace(sender, encodedLen, "Sync")
	t.finishTrace()
}

func (t *tracer) traceTerminate(sender byte, encodedLen int32, msg *Terminate) {
	t.beginTrace(sender, encodedLen, "Terminate")
	t.finishTrace()
}

func (t *tracer) beginTrace(sender byte, encodedLen int32, msgType string) {
	if !t.SuppressTimestamps {
		now := time.Now()
		t.buf.WriteString(now.Format("2006-01-02 15:04:05.000000"))
		t.buf.WriteByte('\t')
	}

	t.buf.WriteByte(sender)
	t.buf.WriteByte('\t')
	t.buf.WriteString(msgType)
	t.buf.WriteByte('\t')
	t.buf.WriteString(strconv.FormatInt(int64(encodedLen), 10))
}

func (t *tracer) finishTrace() {
	t.buf.WriteByte('\n')
	t.buf.WriteTo(t.w)

	if t.buf.Cap() > 1024 {
		t.buf = &bytes.Buffer{}
	} else {
		t.buf.Reset()
	}
}

// traceDoubleQuotedString returns t.buf as a double-quoted string without any escaping. It is roughly equivalent to
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
