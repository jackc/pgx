package pgx

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgmock/pgmsg"
)

func TestMsgReaderPrebuffersWhenPossible(t *testing.T) {
	t.Parallel()

	tests := []struct {
		msgType     byte
		payloadSize int32
		buffered    bool
	}{
		{1, 50, true},
		{2, 0, true},
		{3, 500, true},
		{4, 1050, true},
		{5, 1500, true},
		{6, 1500, true},
		{7, 4000, true},
		{8, 24000, false},
		{9, 4000, true},
		{1, 1500, true},
		{2, 0, true},
		{3, 500, true},
		{4, 1050, true},
		{5, 1500, true},
		{6, 1500, true},
		{7, 4000, true},
		{8, 14000, false},
		{9, 0, true},
		{1, 500, true},
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		var bigEndian pgmsg.BigEndianBuf

		conn, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		for _, tt := range tests {
			_, err = conn.Write([]byte{tt.msgType})
			if err != nil {
				t.Fatal(err)
			}

			_, err = conn.Write(bigEndian.Int32(tt.payloadSize + 4))
			if err != nil {
				t.Fatal(err)
			}

			payload := make([]byte, int(tt.payloadSize))
			_, err = conn.Write(payload)
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	mr := &msgReader{
		reader:    bufio.NewReader(conn),
		shouldLog: func(int) bool { return false },
	}

	for i, tt := range tests {
		msgType, err := mr.rxMsg()
		if err != nil {
			t.Fatalf("%d. Unexpected error: %v", i, err)
		}

		if msgType != tt.msgType {
			t.Fatalf("%d. Expected %v, got %v", 1, i, tt.msgType, msgType)
		}

		if mr.reader.Buffered() < int(tt.payloadSize) && tt.buffered {
			t.Fatalf("%d. Expected message to be buffered with at least %d bytes, but only %v bytes buffered", i, tt.payloadSize, mr.reader.Buffered())
		}
	}
}

func TestMsgReaderDeadlineNeverInterruptsNormalSizedMessages(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	testCount := 10000

	go func() {
		var bigEndian pgmsg.BigEndianBuf

		conn, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		for i := 0; i < testCount; i++ {
			msgType := byte(i)

			_, err = conn.Write([]byte{msgType})
			if err != nil {
				t.Fatal(err)
			}

			msgSize := i % 4000

			_, err = conn.Write(bigEndian.Int32(int32(msgSize + 4)))
			if err != nil {
				t.Fatal(err)
			}

			payload := make([]byte, msgSize)
			_, err = conn.Write(payload)
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	mr := &msgReader{
		reader:    bufio.NewReader(conn),
		shouldLog: func(int) bool { return false },
	}

	conn.SetReadDeadline(time.Now().Add(time.Millisecond))

	i := 0
	for {
		msgType, err := mr.rxMsg()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				conn.SetReadDeadline(time.Now().Add(time.Millisecond))
				continue
			} else {
				t.Fatalf("%d. Unexpected error: %v", i, err)
			}
		}

		expectedMsgType := byte(i)
		if msgType != expectedMsgType {
			t.Fatalf("%d. Expected %v, got %v", i, expectedMsgType, msgType)
		}

		expectedMsgSize := i % 4000
		payload := mr.readBytes(mr.msgBytesRemaining)
		if mr.err != nil {
			t.Fatalf("%d. readBytes killed msgReader: %v", i, mr.err)
		}
		if len(payload) != expectedMsgSize {
			t.Fatalf("%d. Expected %v, got %v", i, expectedMsgSize, len(payload))
		}

		i++
		if i == testCount {
			break
		}
	}
}
