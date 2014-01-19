package pgx

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var literalPattern *regexp.Regexp = regexp.MustCompile(`\$\d+`)

// QuoteString escapes and quotes a string making it safe for interpolation
// into an SQL string.
func (c *Connection) QuoteString(input string) (output string) {
	output = "'" + strings.Replace(input, "'", "''", -1) + "'"
	return
}

// QuoteIdentifier escapes and quotes an identifier making it safe for
// interpolation into an SQL string
func (c *Connection) QuoteIdentifier(input string) (output string) {
	output = `"` + strings.Replace(input, `"`, `""`, -1) + `"`
	return
}

// SanitizeSql substitutely args positionaly into sql. Placeholder values are
// $ prefixed integers like $1, $2, $3, etc. args are sanitized and quoted as
// appropriate.
func (c *Connection) SanitizeSql(sql string, args ...interface{}) (output string, err error) {
	replacer := func(match string) (replacement string) {
		n, _ := strconv.ParseInt(match[1:], 10, 0)
		switch arg := args[n-1].(type) {
		case string:
			return c.QuoteString(arg)
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
			return c.QuoteString(arg.Format("2006-01-02 15:04:05.999999 -0700"))
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
		case []int16:
			var s string
			s, err = int16SliceToArrayString(arg)
			return c.QuoteString(s)
		case []int32:
			var s string
			s, err = int32SliceToArrayString(arg)
			return c.QuoteString(s)
		case []int64:
			var s string
			s, err = int64SliceToArrayString(arg)
			return c.QuoteString(s)
		case nil:
			return "null"
		default:
			err = fmt.Errorf("Unable to sanitize type: %T", arg)
			return ""
		}
	}

	output = literalPattern.ReplaceAllStringFunc(sql, replacer)
	return
}
