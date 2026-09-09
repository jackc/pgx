package pgx

import (
	"github.com/jackc/pgx/v5/internal/pgio"
	"github.com/jackc/pgx/v5/pgtype"
)

// PostgreSQL format codes
const (
	TextFormatCode   = 0
	BinaryFormatCode = 1
)

func convertSimpleArgument(m *pgtype.Map, arg any) (any, error) {
	buf, err := m.Encode(0, TextFormatCode, arg, []byte{})
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}
	return string(buf), nil
}

func encodeCopyValue(m *pgtype.Map, buf []byte, oid uint32, arg any) ([]byte, error) {
	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)
	argBuf, err := m.Encode(oid, BinaryFormatCode, arg, buf)
	if err != nil {
		return nil, err
	}

	if argBuf != nil {
		buf = argBuf
		pgio.SetInt32(buf[sp:], int32(len(buf[sp:])-4))
	}
	return buf, nil
}

func encodeCopyValueText(m *pgtype.Map, buf []byte, oid uint32, arg any) ([]byte, error) {
	// Encode into a separate buffer to distinguish NULL (nil return) from empty string (empty non-nil return). Using a
	// non-nil empty slice ensures that encoding an empty value (e.g. empty string) returns non-nil. This also avoids
	// aliasing issues when the subsequent escaping step expands special characters in-place.
	textBuf, err := m.Encode(oid, TextFormatCode, arg, []byte{})
	if err != nil {
		return nil, err
	}
	if textBuf == nil {
		// NULL is represented as \N in text COPY format.
		return append(buf, '\\', 'N'), nil
	}
	return appendTextCopyEscaped(buf, textBuf), nil
}

// appendTextCopyEscaped appends src to buf, escaping characters that are special in PostgreSQL text COPY format.
func appendTextCopyEscaped(buf []byte, src []byte) []byte {
	for _, b := range src {
		switch b {
		case '\\':
			buf = append(buf, '\\', '\\')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			buf = append(buf, b)
		}
	}
	return buf
}
