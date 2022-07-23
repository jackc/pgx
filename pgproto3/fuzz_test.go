package pgproto3_test

import (
	"bytes"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func FuzzFrontend(f *testing.F) {
	testcases := [][]byte{
		{'Z', 0, 0, 0, 5},
	}
	for _, tc := range testcases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, encodedMsg []byte) {
		r := &bytes.Buffer{}
		w := &bytes.Buffer{}
		fe := pgproto3.NewFrontend(r, w)

		_, err := r.Write(encodedMsg)
		require.NoError(t, err)

		// Not checking anything other than no panic.
		fe.Receive()
	})
}
