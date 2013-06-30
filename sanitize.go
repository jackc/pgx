package pgx

import (
	"encoding/hex"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var literalPattern *regexp.Regexp = regexp.MustCompile(`\$\d+`)

func (c *Connection) QuoteString(input string) (output string) {
	output = "'" + strings.Replace(input, "'", "''", -1) + "'"
	return
}

func (c *Connection) SanitizeSql(sql string, args ...interface{}) (output string) {
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
		case []byte:
			return `E'\\x` + hex.EncodeToString(arg) + `'`
		default:
			panic("Unable to sanitize type: " + reflect.TypeOf(arg).String())
		}
	}

	output = literalPattern.ReplaceAllStringFunc(sql, replacer)
	return
}
