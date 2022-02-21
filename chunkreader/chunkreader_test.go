package chunkreader

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestChunkReaderNextDoesNotReadIfAlreadyBuffered(t *testing.T) {
	server := &bytes.Buffer{}
	r, err := NewConfig(server, Config{MinBufLen: 4})
	if err != nil {
		t.Fatal(err)
	}

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
	if r.rp != 4 {
		t.Fatalf("Expected r.rp to be %v, but it was %v", 4, r.rp)
	}
	if r.wp != 4 {
		t.Fatalf("Expected r.wp to be %v, but it was %v", 4, r.wp)
	}
}

func TestChunkReaderNextExpandsBufAsNeeded(t *testing.T) {
	server := &bytes.Buffer{}
	r, err := NewConfig(server, Config{MinBufLen: 4})
	if err != nil {
		t.Fatal(err)
	}

	src := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	server.Write(src)

	n1, err := r.Next(5)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(n1, src[0:5]) != 0 {
		t.Fatalf("Expected read bytes to be %v, but they were %v", src[0:5], n1)
	}
	if len(r.buf) != 5 {
		t.Fatalf("Expected len(r.buf) to be %v, but it was %v", 5, len(r.buf))
	}
}

func TestChunkReaderDoesNotReuseBuf(t *testing.T) {
	server := &bytes.Buffer{}
	r, err := NewConfig(server, Config{MinBufLen: 4})
	if err != nil {
		t.Fatal(err)
	}

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

	if bytes.Compare(n1, src[0:4]) != 0 {
		t.Fatalf("Expected KeepLast to prevent Next from overwriting buf, expected %v but it was %v", src[0:4], n1)
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
	r, err := NewConfig(rr, Config{MinBufLen: 8192})
	if err != nil {
		t.Fatal(err)
	}

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
