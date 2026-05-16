package pgconn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommandTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		commandTag   CommandTag
		rowsAffected int64
		isInsert     bool
		isUpdate     bool
		isDelete     bool
		isSelect     bool
	}{
		{commandTag: CommandTag{s: "INSERT 0 5"}, rowsAffected: 5, isInsert: true},
		{commandTag: CommandTag{s: "UPDATE 0"}, rowsAffected: 0, isUpdate: true},
		{commandTag: CommandTag{s: "UPDATE 1"}, rowsAffected: 1, isUpdate: true},
		{commandTag: CommandTag{s: "DELETE 0"}, rowsAffected: 0, isDelete: true},
		{commandTag: CommandTag{s: "DELETE 1"}, rowsAffected: 1, isDelete: true},
		{commandTag: CommandTag{s: "DELETE 1234567890"}, rowsAffected: 1234567890, isDelete: true},
		{commandTag: CommandTag{s: "SELECT 1"}, rowsAffected: 1, isSelect: true},
		{commandTag: CommandTag{s: "SELECT 99999999999"}, rowsAffected: 99999999999, isSelect: true},
		{commandTag: CommandTag{s: "CREATE TABLE"}, rowsAffected: 0},
		{commandTag: CommandTag{s: "ALTER TABLE"}, rowsAffected: 0},
		{commandTag: CommandTag{s: "DROP TABLE"}, rowsAffected: 0},
	}

	for i, tt := range tests {
		ct := tt.commandTag
		assert.Equalf(t, tt.rowsAffected, ct.RowsAffected(), "%d. %v", i, tt.commandTag)
		assert.Equalf(t, tt.isInsert, ct.Insert(), "%d. %v", i, tt.commandTag)
		assert.Equalf(t, tt.isUpdate, ct.Update(), "%d. %v", i, tt.commandTag)
		assert.Equalf(t, tt.isDelete, ct.Delete(), "%d. %v", i, tt.commandTag)
		assert.Equalf(t, tt.isSelect, ct.Select(), "%d. %v", i, tt.commandTag)
	}
}

// timeoutNetError is a net.Error that always reports Timeout() == true.
type timeoutNetError struct{ msg string }

func (e *timeoutNetError) Error() string   { return e.msg }
func (e *timeoutNetError) Timeout() bool   { return true }
func (e *timeoutNetError) Temporary() bool { return false }

// wrappedDialError simulates a DialError wrapping a net.Error, like the error
// produced when a dial deadline is exceeded on a custom dialer.
type wrappedDialError struct {
	addr string
	err  error
}

func (e *wrappedDialError) Error() string { return fmt.Sprintf("dial %s: %s", e.addr, e.err) }
func (e *wrappedDialError) Unwrap() error { return e.err }

func TestNormalizeTimeoutError_PreservesErrorChain(t *testing.T) {
	t.Parallel()

	inner := &timeoutNetError{msg: "i/o timeout"}
	outer := &wrappedDialError{addr: "192.0.2.1:5432", err: inner}

	// Sanity check: errors.As finds the net.Error through the wrapper.
	var netErr net.Error
	require.True(t, errors.As(outer, &netErr))

	result := normalizeTimeoutError(context.Background(), outer)

	// The result should be an errTimeout and should still unwrap to the
	// original wrappedDialError so callers can inspect dial context (address, etc.).
	var te *errTimeout
	require.True(t, errors.As(result, &te))

	var dial *wrappedDialError
	assert.True(t, errors.As(result, &dial), "original dial error should be preserved in the chain")
	assert.Equal(t, "192.0.2.1:5432", dial.addr)
}
