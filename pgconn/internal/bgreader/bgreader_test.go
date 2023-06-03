package bgreader_test

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn/internal/bgreader"
	"github.com/stretchr/testify/require"
)

func TestBGReaderReadWhenStopped(t *testing.T) {
	r := bytes.NewReader([]byte("foo bar baz"))
	bgr := bgreader.New(r)
	buf, err := io.ReadAll(bgr)
	require.NoError(t, err)
	require.Equal(t, []byte("foo bar baz"), buf)
}

func TestBGReaderReadWhenStarted(t *testing.T) {
	r := bytes.NewReader([]byte("foo bar baz"))
	bgr := bgreader.New(r)
	bgr.Start()
	buf, err := io.ReadAll(bgr)
	require.NoError(t, err)
	require.Equal(t, []byte("foo bar baz"), buf)
}

type mockReadFunc func(p []byte) (int, error)

type mockReader struct {
	readFuncs []mockReadFunc
}

func (r *mockReader) Read(p []byte) (int, error) {
	if len(r.readFuncs) == 0 {
		return 0, io.EOF
	}

	fn := r.readFuncs[0]
	r.readFuncs = r.readFuncs[1:]

	return fn(p)
}

func TestBGReaderReadWaitsForBackgroundRead(t *testing.T) {
	rr := &mockReader{
		readFuncs: []mockReadFunc{
			func(p []byte) (int, error) { time.Sleep(1 * time.Second); return copy(p, []byte("foo")), nil },
			func(p []byte) (int, error) { return copy(p, []byte("bar")), nil },
			func(p []byte) (int, error) { return copy(p, []byte("baz")), nil },
		},
	}
	bgr := bgreader.New(rr)
	bgr.Start()
	buf := make([]byte, 3)
	n, err := bgr.Read(buf)
	require.NoError(t, err)
	require.EqualValues(t, 3, n)
	require.Equal(t, []byte("foo"), buf)
}

func TestBGReaderErrorWhenStarted(t *testing.T) {
	rr := &mockReader{
		readFuncs: []mockReadFunc{
			func(p []byte) (int, error) { return copy(p, []byte("foo")), nil },
			func(p []byte) (int, error) { return copy(p, []byte("bar")), nil },
			func(p []byte) (int, error) { return copy(p, []byte("baz")), errors.New("oops") },
		},
	}

	bgr := bgreader.New(rr)
	bgr.Start()
	buf, err := io.ReadAll(bgr)
	require.Equal(t, []byte("foobarbaz"), buf)
	require.EqualError(t, err, "oops")
}

func TestBGReaderErrorWhenStopped(t *testing.T) {
	rr := &mockReader{
		readFuncs: []mockReadFunc{
			func(p []byte) (int, error) { return copy(p, []byte("foo")), nil },
			func(p []byte) (int, error) { return copy(p, []byte("bar")), nil },
			func(p []byte) (int, error) { return copy(p, []byte("baz")), errors.New("oops") },
		},
	}

	bgr := bgreader.New(rr)
	buf, err := io.ReadAll(bgr)
	require.Equal(t, []byte("foobarbaz"), buf)
	require.EqualError(t, err, "oops")
}

type numberReader struct {
	v   uint8
	rng *rand.Rand
}

func (nr *numberReader) Read(p []byte) (int, error) {
	n := nr.rng.Intn(len(p))
	for i := 0; i < n; i++ {
		p[i] = nr.v
		nr.v++
	}

	return n, nil
}

// TestBGReaderStress stress tests BGReader by reading a lot of bytes in random sizes while randomly starting and
// stopping the background worker from other goroutines.
func TestBGReaderStress(t *testing.T) {
	nr := &numberReader{rng: rand.New(rand.NewSource(0))}
	bgr := bgreader.New(nr)

	bytesRead := 0
	var expected uint8
	buf := make([]byte, 10_000)
	rng := rand.New(rand.NewSource(0))

	for bytesRead < 1_000_000 {
		randomNumber := rng.Intn(100)
		switch {
		case randomNumber < 10:
			go bgr.Start()
		case randomNumber < 20:
			go bgr.Stop()
		default:
			n, err := bgr.Read(buf)
			require.NoError(t, err)
			for i := 0; i < n; i++ {
				require.Equal(t, expected, buf[i])
				expected++
			}
			bytesRead += n
		}
	}
}
