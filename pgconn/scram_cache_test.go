package pgconn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testScramClient(password string, salt []byte, iterations uint64) *scramClient {
	return &scramClient{
		password:   password,
		salt:       salt,
		iterations: iterations,
	}
}

func TestScramDeriveFingerprintStable(t *testing.T) {
	t.Parallel()
	sc := testScramClient("pw", []byte{1, 2, 3}, 4096)
	fp1 := sc.deriveFingerprint()
	fp2 := sc.deriveFingerprint()
	assert.Equal(t, fp1, fp2)
}

func TestScramDeriveFingerprintDistinct(t *testing.T) {
	t.Parallel()
	a := testScramClient("a", []byte{1}, 1).deriveFingerprint()
	b := testScramClient("b", []byte{1}, 1).deriveFingerprint()
	assert.NotEqual(t, a, b)
}

func TestSimpleScramDeriveCache(t *testing.T) {
	t.Parallel()

	c := NewSimpleScramDeriveCache()
	fp := testScramClient("pw", []byte{1, 2, 3}, 4096).deriveFingerprint()

	var sp ScramSaltedPassword
	for i := range sp {
		sp[i] = byte(i)
	}

	_, ok := c.Get(fp)
	assert.False(t, ok)

	c.Put(fp, sp)
	got, ok := c.Get(fp)
	require.True(t, ok)
	assert.Equal(t, sp, got)
	got[0] ^= 0xff
	got2, ok := c.Get(fp)
	require.True(t, ok)
	assert.Equal(t, sp, got2)

	c.Delete(fp)
	_, ok = c.Get(fp)
	assert.False(t, ok)
}
