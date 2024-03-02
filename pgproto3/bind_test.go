package pgproto3_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestBindBiggerThanMaxMessageBodyLen(t *testing.T) {
	t.Parallel()

	// Maximum allowed size.
	_, err := (&pgproto3.Bind{Parameters: [][]byte{make([]byte, pgproto3.MaxMessageBodyLen-16)}}).Encode(nil)
	require.NoError(t, err)

	// 1 byte too big
	_, err = (&pgproto3.Bind{Parameters: [][]byte{make([]byte, pgproto3.MaxMessageBodyLen-15)}}).Encode(nil)
	require.Error(t, err)
}
