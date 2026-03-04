package pgx

import (
	"fmt"
	"strconv"
	"strings"
)

// findInsertValuesTupleRange finds the byte range of the first VALUES row tuple in an INSERT
// query. Returns (start, end) where start is the index of '(' and end is the index of the
// matching ')'. Handles single/double-quoted strings and line/block comments.
func findInsertValuesTupleRange(sql string) (start, end int, err error) {
	n := len(sql)
	i := 0

	// Phase 1: scan for the VALUES keyword, skipping strings and comments.
	valuesFound := false
	for i < n && !valuesFound {
		switch {
		case sql[i] == '\'':
			i = skipInsertSingleQuote(sql, i+1)
		case sql[i] == '"':
			i = skipInsertDoubleQuote(sql, i+1)
		case sql[i] == '-' && i+1 < n && sql[i+1] == '-':
			i = skipInsertLineComment(sql, i+2)
		case sql[i] == '/' && i+1 < n && sql[i+1] == '*':
			i = skipInsertBlockComment(sql, i+2)
		case (sql[i] == 'V' || sql[i] == 'v') && i+6 <= n && strings.EqualFold(sql[i:i+6], "VALUES"):
			prevOK := i == 0 || !isInsertIDChar(sql[i-1])
			nextOK := i+6 >= n || !isInsertIDChar(sql[i+6])
			if prevOK && nextOK {
				i += 6
				valuesFound = true
			} else {
				i++
			}
		default:
			i++
		}
	}

	if !valuesFound {
		return 0, 0, fmt.Errorf("VALUES clause not found in INSERT query")
	}

	// Phase 2: skip whitespace and find '('.
	for i < n && isInsertSpace(sql[i]) {
		i++
	}
	if i >= n || sql[i] != '(' {
		return 0, 0, fmt.Errorf("expected '(' after VALUES in INSERT query")
	}
	start = i
	i++

	// Phase 3: balance parentheses to find the matching ')'.
	depth := 1
	for i < n {
		switch {
		case sql[i] == '(':
			depth++
			i++
		case sql[i] == ')':
			depth--
			if depth == 0 {
				return start, i, nil
			}
			i++
		case sql[i] == '\'':
			i = skipInsertSingleQuote(sql, i+1)
		case sql[i] == '"':
			i = skipInsertDoubleQuote(sql, i+1)
		default:
			i++
		}
	}

	return 0, 0, fmt.Errorf("unbalanced parentheses in VALUES clause")
}

// splitInsertTemplate splits an INSERT...VALUES query template into three parts around the first
// VALUES row tuple.
//
// For "INSERT INTO t (a, b) VALUES ($1, $2) ON CONFLICT DO NOTHING":
//
//	prefix = "INSERT INTO t (a, b) VALUES ("
//	inner  = "$1, $2"
//	suffix = " ON CONFLICT DO NOTHING"
func splitInsertTemplate(sql string) (prefix, inner, suffix string, err error) {
	tupleStart, tupleEnd, err := findInsertValuesTupleRange(sql)
	if err != nil {
		return "", "", "", err
	}
	prefix = sql[:tupleStart+1]          // up to and including '('
	inner = sql[tupleStart+1 : tupleEnd] // content inside ()
	suffix = sql[tupleEnd+1:]            // after ')'
	return prefix, inner, suffix, nil
}

// countInsertParams returns the maximum $N placeholder index found in the SQL fragment.
// String literals within the fragment are skipped.
func countInsertParams(fragment string) int {
	max := 0
	n := len(fragment)
	for i := 0; i < n; i++ {
		switch {
		case fragment[i] == '\'':
			i = skipInsertSingleQuote(fragment, i+1) - 1
		case fragment[i] == '"':
			i = skipInsertDoubleQuote(fragment, i+1) - 1
		case fragment[i] == '$' && i+1 < n && fragment[i+1] >= '1' && fragment[i+1] <= '9':
			j := i + 1
			for j < n && fragment[j] >= '0' && fragment[j] <= '9' {
				j++
			}
			if num, e := strconv.Atoi(fragment[i+1 : j]); e == nil && num > max {
				max = num
			}
			i = j - 1
		}
	}
	return max
}

// renumberPlaceholders adds offset to every $N placeholder in template.
// Single-quoted string literals are copied verbatim without modification.
//
//	renumberPlaceholders("$1, $2", 2)  → "$3, $4"
//	renumberPlaceholders("$1, 'x'", 4) → "$5, 'x'"
func renumberPlaceholders(template string, offset int) string {
	var sb strings.Builder
	i := 0
	n := len(template)
	for i < n {
		switch {
		case template[i] == '\'':
			// Copy single-quoted string verbatim.
			sb.WriteByte('\'')
			i++
			for i < n {
				ch := template[i]
				sb.WriteByte(ch)
				i++
				if ch == '\'' {
					if i < n && template[i] == '\'' {
						sb.WriteByte(template[i]) // escaped ''
						i++
					} else {
						break
					}
				}
			}
		case template[i] == '$' && i+1 < n && template[i+1] >= '1' && template[i+1] <= '9':
			j := i + 1
			for j < n && template[j] >= '0' && template[j] <= '9' {
				j++
			}
			num, _ := strconv.Atoi(template[i+1 : j])
			sb.WriteByte('$')
			sb.WriteString(strconv.Itoa(num + offset))
			i = j
		default:
			sb.WriteByte(template[i])
			i++
		}
	}
	return sb.String()
}

// buildBulkExpandedSQL generates a multi-row INSERT SQL from a split template.
//
//	prefix = "INSERT INTO t (a, b) VALUES ("
//	inner  = "$1, $2"
//	suffix = ""
//	rowCount=3, paramsPerRow=2
//	→ "INSERT INTO t (a, b) VALUES ($1, $2), ($3, $4), ($5, $6)"
func buildBulkExpandedSQL(prefix, inner, suffix string, rowCount, paramsPerRow int) string {
	var sb strings.Builder
	sb.WriteString(prefix) // ends with "("
	for row := 0; row < rowCount; row++ {
		if row > 0 {
			sb.WriteString(", (")
			sb.WriteString(renumberPlaceholders(inner, row*paramsPerRow))
		} else {
			sb.WriteString(inner)
		}
		sb.WriteByte(')')
	}
	sb.WriteString(suffix)
	return sb.String()
}

func skipInsertSingleQuote(sql string, i int) int {
	for i < len(sql) {
		if sql[i] == '\'' {
			i++
			if i < len(sql) && sql[i] == '\'' {
				i++ // escaped ''
			} else {
				return i
			}
		} else {
			i++
		}
	}
	return i
}

func skipInsertDoubleQuote(sql string, i int) int {
	for i < len(sql) {
		if sql[i] == '"' {
			i++
			if i < len(sql) && sql[i] == '"' {
				i++ // escaped ""
			} else {
				return i
			}
		} else {
			i++
		}
	}
	return i
}

func skipInsertLineComment(sql string, i int) int {
	for i < len(sql) && sql[i] != '\n' {
		i++
	}
	return i
}

func skipInsertBlockComment(sql string, i int) int {
	for i+1 < len(sql) {
		if sql[i] == '*' && sql[i+1] == '/' {
			return i + 2
		}
		i++
	}
	return len(sql)
}

func isInsertIDChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

func isInsertSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
