package pgproto3

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestChunkReaderNextDoesNotReadIfAlreadyBuffered(t *testing.T) {
	server := &bytes.Buffer{}
	r := newChunkReader(server, 4)

	src := []byte{1, 2, 3, 4}
	server.Write(src)

	n1, err := r.Next(2)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(n1, src[0:2]) != 0 {
		t.Fatalf("Expected read bytes to be %v, but they were %v", src[0:2], n1)
	}

	n2, err := r.Next(2)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(n2, src[2:4]) != 0 {
		t.Fatalf("Expected read bytes to be %v, but they were %v", src[2:4], n2)
	}

	if bytes.Compare(r.buf, src) != 0 {
		t.Fatalf("Expected r.buf to be %v, but it was %v", src, r.buf)
	}

	_, err = r.Next(0) // Trigger the buffer reset.
	if err != nil {
		t.Fatal(err)
	}

	if r.rp != 0 {
		t.Fatalf("Expected r.rp to be %v, but it was %v", 0, r.rp)
	}
	if r.wp != 0 {
		t.Fatalf("Expected r.wp to be %v, but it was %v", 0, r.wp)
	}
}

func TestChunkReaderNextGetsBiggerBufAsNeededFromBigBufPools(t *testing.T) {
	server := &bytes.Buffer{}
	r := newChunkReader(server, 4)

	src := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	server.Write(src)

	n1, err := r.Next(5)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(n1, src[0:5]) != 0 {
		t.Fatalf("Expected read bytes to be %v, but they were %v", src[0:5], n1)
	}
	if len(r.buf) != bigBufPools[0].byteSize {
		t.Fatalf("Expected len(r.buf) to be %v, but it was %v", bigBufPools[0].byteSize, len(r.buf))
	}
}

func TestChunkReaderReusesBuf(t *testing.T) {
	server := &bytes.Buffer{}
	r := newChunkReader(server, 4)

	src := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	server.Write(src)

	n1, err := r.Next(4)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(n1, src[0:4]) != 0 {
		t.Fatalf("Expected read bytes to be %v, but they were %v", src[0:4], n1)
	}

	n2, err := r.Next(4)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(n2, src[4:8]) != 0 {
		t.Fatalf("Expected read bytes to be %v, but they were %v", src[4:8], n2)
	}

	if bytes.Compare(n1, src[4:8]) != 0 {
		t.Fatalf("Expected slice to be reused, expected %v but it was %v", src[4:8], n1)
	}
}

type randomReader struct {
	rnd *rand.Rand
}

// Read reads a random number of random bytes.
func (r *randomReader) Read(p []byte) (n int, err error) {
	n = r.rnd.Intn(len(p) + 1)
	return r.rnd.Read(p[:n])
}

func TestChunkReaderNextFuzz(t *testing.T) {
	rr := &randomReader{rnd: rand.New(rand.NewSource(1))}
	r := newChunkReader(rr, 8192)

	randomSizes := rand.New(rand.NewSource(0))

	for i := 0; i < 100000; i++ {
		size := randomSizes.Intn(16384) + 1
		buf, err := r.Next(size)
		if err != nil {
			t.Fatal(err)
		}
		if len(buf) != size {
			t.Fatalf("Expected to get %v bytes but got %v bytes", size, len(buf))
		}
	}
}
