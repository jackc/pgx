package pgconn

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommandTag(t *testing.T) {
	t.Parallel()

	var tests = []struct {
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

func mustConnectWithNoClosingMode(t *testing.T) *PgConn {
	t.Helper()

	cfg, err := ParseConfig(os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)

	cfg.NoClosingConnMode = true

	conn, err := ConnectConfig(context.Background(), cfg)
	require.NoError(t, err)

	return conn
}

func TestCleanup(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		name     string
		testCase func(t *testing.T, conn *PgConn)
	}{
		{
			name: "success",
			testCase: func(t *testing.T, conn *PgConn) {
				ctx := context.Background()

				execCtx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
				defer cancel()

				// expecting error because timeout is less than execution time
				rr := conn.Exec(execCtx, `select pg_sleep(0.5)`)
				err := rr.Close()
				require.Error(t, err)

				// enough timeout for cleanup
				cleanupCtx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()

				var cleanupSucceeded bool
				// we expect that connection is in status `need cleanup` because previous request failed
				launched := conn.LaunchCleanup(cleanupCtx, `select 'hello wold'`, func() { cleanupSucceeded = true }, nil)
				require.True(t, launched)

				// enough time for cleanup
				time.Sleep(time.Second)
				require.True(t, conn.status == connStatusIdle)
				require.True(t, cleanupSucceeded)

				execCtx, cancel = context.WithTimeout(ctx, time.Millisecond*200)
				defer cancel()

				// checking that socket is clean, and we are reading data from our request, not from the previous one
				rr = conn.Exec(execCtx, `select 'goodbye world'`)
				res, err := rr.ReadAll()
				require.NoError(t, err)

				require.True(t, string(res[0].Rows[0][0]) == "goodbye world")
			},
		},
		{
			name: "failed cleanup timepout",
			testCase: func(t *testing.T, conn *PgConn) {
				ctx := context.Background()

				execCtx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
				defer cancel()

				// expecting error because timeout is less than execution time
				rr := conn.Exec(execCtx, `select pg_sleep(0.5)`)
				err := rr.Close()
				require.Error(t, err)

				// enough timeout for cleanup
				cleanupCtx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
				defer cancel()

				var launchErr error
				// we expect that connection is in status `need cleanup` because previous request failed
				launched := conn.LaunchCleanup(cleanupCtx, `select pg_sleep(0.5)`, nil, func(err error) {
					launchErr = err
				})
				require.True(t, launched)

				// enough time for cleanup
				time.Sleep(time.Second)

				// we expect error as we failed to cleanup socket
				require.Error(t, launchErr)
			},
		},
		{
			name: "failed to launch cleanup",
			testCase: func(t *testing.T, conn *PgConn) {
				ctx := context.Background()

				execCtx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
				defer cancel()

				// expecting error because timeout is less than execution time
				rr := conn.Exec(execCtx, `select pg_sleep(0.5)`)

				// we expect not to launch cleanup when connection is not in `need cleanup` state
				launched := conn.LaunchCleanup(ctx, "", nil, nil)
				require.False(t, launched)

				_ = rr.Close()
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conn := mustConnectWithNoClosingMode(t)
			tt.testCase(t, conn)
		})
	}
}

var _ net.Error = &mockNetError{}

type mockNetError struct {
	safeToRetry bool
	timeout     bool
}

func (m *mockNetError) Error() string {
	return "mock error"
}

func (m *mockNetError) Timeout() bool {
	return m.timeout
}

func (m *mockNetError) Temporary() bool {
	return true
}

func (m *mockNetError) SafeToRetry() bool {
	return m.safeToRetry
}

func TestCheckIfCleanupNeeded(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		name                        string
		reason                      error
		expected                    bool
		expectedStatus              byte
		expectedCleanupWithoutReset bool
		builder                     func() *PgConn
	}{
		{
			name:                        "need to cleanup",
			reason:                      &mockNetError{timeout: true},
			expected:                    true,
			expectedStatus:              connStatusNeedCleanup,
			expectedCleanupWithoutReset: false,
			builder: func() *PgConn {
				return &PgConn{status: connStatusBusy}
			},
		},
		{
			name:                        "need to cleanup without recoverSocket",
			reason:                      &mockNetError{timeout: true, safeToRetry: true},
			expected:                    true,
			expectedStatus:              connStatusNeedCleanup,
			expectedCleanupWithoutReset: true,
			builder: func() *PgConn {
				return &PgConn{status: connStatusBusy}
			},
		},
		{
			name:                        "wrong status no need to cleanup",
			reason:                      &mockNetError{},
			expected:                    false,
			expectedStatus:              connStatusClosed,
			expectedCleanupWithoutReset: false,
			builder: func() *PgConn {
				return &PgConn{status: connStatusClosed}
			},
		},
		{
			name:                        "no timeout error",
			reason:                      &mockNetError{timeout: false},
			expected:                    false,
			expectedStatus:              connStatusBusy,
			expectedCleanupWithoutReset: false,
			builder: func() *PgConn {
				return &PgConn{status: connStatusBusy}
			},
		},
		{
			name:                        "not net error",
			reason:                      fmt.Errorf("some error"),
			expected:                    false,
			expectedStatus:              connStatusBusy,
			expectedCleanupWithoutReset: false,
			builder: func() *PgConn {
				return &PgConn{status: connStatusBusy}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conn := tt.builder()
			actual := conn.setCleanupNeeded(tt.reason)

			assert.Equal(t, tt.expected, actual)
			assert.Equal(t, tt.expectedStatus, conn.status)
			assert.Equal(t, tt.expectedCleanupWithoutReset, conn.cleanupWithoutReset)
		})
	}
}
