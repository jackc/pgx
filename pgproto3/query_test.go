package pgproto3_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestQueryBiggerThanMaxMessageBodyLen(t *testing.T) {
	t.Parallel()

	// Maximum allowed size. 4 bytes for size and 1 byte for 0 terminated string.
	_, err := (&pgproto3.Query{String: string(make([]byte, pgproto3.MaxMessageBodyLen-5))}).Encode(nil)
	require.NoError(t, err)

	// 1 byte too big
	_, err = (&pgproto3.Query{String: string(make([]byte, pgproto3.MaxMessageBodyLen-4))}).Encode(nil)
	require.Error(t, err)
}
