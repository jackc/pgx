package pgx

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type SerializationError string

func (e SerializationError) Error() string {
	return string(e)
}

// TextEncoder is an interface used to encode values in text format for
// transmission to the PostgreSQL server. It is used by unprepared
// queries and for prepared queries when the type does not implement
// BinaryEncoder
type TextEncoder interface {
	// EncodeText MUST sanitize (and quote, if necessary) the returned string.
	// It will be interpolated directly into the SQL string.
	EncodeText() (string, error)
}

var literalPattern *regexp.Regexp = regexp.MustCompile(`\$\d+`)

// QuoteString escapes and quotes a string making it safe for interpolation
// into an SQL string.
func QuoteString(input string) (output string) {
	output = "'" + strings.Replace(input, "'", "''", -1) + "'"
	return
}

// QuoteIdentifier escapes and quotes an identifier making it safe for
// interpolation into an SQL string
func QuoteIdentifier(input string) (output string) {
	output = `"` + strings.Replace(input, `"`, `""`, -1) + `"`
	return
}

// SanitizeSql substitutely args positionaly into sql. Placeholder values are
// $ prefixed integers like $1, $2, $3, etc. args are sanitized and quoted as
// appropriate.
func SanitizeSql(sql string, args ...interface{}) (output string, err error) {
	replacer := func(match string) (replacement string) {
		if err != nil {
			return ""
		}

		n, _ := strconv.ParseInt(match[1:], 10, 0)
		if int(n-1) >= len(args) {
			err = fmt.Errorf("Cannot interpolate %v, only %d arguments provided", match, len(args))
			return
		}

		switch arg := args[n-1].(type) {
		case string:
			return QuoteString(arg)
		case int:
			return strconv.FormatInt(int64(arg), 10)
		case int8:
			return strconv.FormatInt(int64(arg), 10)
		case int16:
			return strconv.FormatInt(int64(arg), 10)
		case int32:
			return strconv.FormatInt(int64(arg), 10)
		case int64:
			return strconv.FormatInt(int64(arg), 10)
		case time.Time:
			return QuoteString(arg.Format("2006-01-02 15:04:05.999999 -0700"))
		case uint:
			return strconv.FormatUint(uint64(arg), 10)
		case uint8:
			return strconv.FormatUint(uint64(arg), 10)
		case uint16:
			return strconv.FormatUint(uint64(arg), 10)
		case uint32:
			return strconv.FormatUint(uint64(arg), 10)
		case uint64:
			return strconv.FormatUint(uint64(arg), 10)
		case float32:
			return strconv.FormatFloat(float64(arg), 'f', -1, 32)
		case float64:
			return strconv.FormatFloat(arg, 'f', -1, 64)
		case bool:
			return strconv.FormatBool(arg)
		case []byte:
			return `E'\\x` + hex.EncodeToString(arg) + `'`
		case nil:
			return "null"
		case TextEncoder:
			var s string
			s, err = arg.EncodeText()
			return s
		default:
			err = SerializationError(fmt.Sprintf("%T is not a core type and it does not implement TextEncoder", arg))
			return ""
		}
	}

	output = literalPattern.ReplaceAllStringFunc(sql, replacer)
	return
}
