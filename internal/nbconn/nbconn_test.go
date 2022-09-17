package nbconn_test

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/internal/nbconn"
	"github.com/stretchr/testify/require"
)

// Test keys generated with:
//
// $ openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -sha256 -nodes -days 20000 -subj '/CN=localhost'

var testTLSPublicKey = []byte(`-----BEGIN CERTIFICATE-----
MIICpjCCAY4CCQCjQKYdUDQzKDANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAls
b2NhbGhvc3QwIBcNMjIwNjA0MTY1MzE2WhgPMjA3NzAzMDcxNjUzMTZaMBQxEjAQ
BgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
ALHbOu80cfSPufKTZsKf3E5rCXHeIHjaIbgHEXA2SW/n77U8oZX518s+27FO0sK5
yA0WnEIwY34PU359sNR5KelARGnaeh3HdaGm1nuyyxBtwwAqIuM0UxGAMF/mQ4lT
caZPxG+7WlYDqnE3eVXUtG4c+T7t5qKAB3MtfbzKFSjczkWkroi6cTypmHArGghT
0VWWVu0s9oNp5q8iWchY2o9f0aIjmKv6FgtilO+geev+4U+QvtvrziR5BO3/3EgW
c5TUVcf+lwkvp8ziXvargmjjnNTyeF37y4KpFcex0v7z7hSrUK4zU0+xRn7Bp17v
7gzj0xN+HCsUW1cjPFNezX0CAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAbEBzewzg
Z5F+BqMSxP3HkMCkLLH0N9q0/DkZaVyZ38vrjcjaDYuabq28kA2d5dc5jxsQpvTw
HTGqSv1ZxJP3pBFv6jLSh8xaM6tUkk482Q6DnZGh97CD4yup/yJzkn5nv9OHtZ9g
TnaQeeXgOz0o5Zq9IpzHJb19ysya3UCIK8oKXbSO4Qd168seCq75V2BFHDpmejjk
D92eT6WODlzzvZbhzA1F3/cUilZdhbQtJMqdecKvD+yrBpzGVqzhWQsXwsRAU1fB
hShx+D14zUGM2l4wlVzOAuGh4ZL7x3AjJsc86TsCavTspS0Xl51j+mRbiULq7G7Y
E7ZYmaKTMOhvkg==
-----END CERTIFICATE-----`)

// The strings.ReplaceAll is used to placate any secret scanners that would squawk if they saw a private key embedded in
// source code.
var testTLSPrivateKey = []byte(strings.ReplaceAll(`-----BEGIN TESTING KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCx2zrvNHH0j7ny
k2bCn9xOawlx3iB42iG4BxFwNklv5++1PKGV+dfLPtuxTtLCucgNFpxCMGN+D1N+
fbDUeSnpQERp2nodx3WhptZ7sssQbcMAKiLjNFMRgDBf5kOJU3GmT8Rvu1pWA6px
N3lV1LRuHPk+7eaigAdzLX28yhUo3M5FpK6IunE8qZhwKxoIU9FVllbtLPaDaeav
IlnIWNqPX9GiI5ir+hYLYpTvoHnr/uFPkL7b684keQTt/9xIFnOU1FXH/pcJL6fM
4l72q4Jo45zU8nhd+8uCqRXHsdL+8+4Uq1CuM1NPsUZ+wade7+4M49MTfhwrFFtX
IzxTXs19AgMBAAECggEBAJcHt5ARVQN8WUbobMawwX/F3QtYuPJnKWMAfYpwTwQ8
TI32orCcrObmxeBXMxowcPTMUnzSYmpV0W0EhvimuzRbYr0Qzcoj6nwPFOuN9GpL
CuBE58NQV4nw9SM6gfdHaKb17bWDvz5zdnUVym9cZKts5yrNEqDDX5Aq/S8n27gJ
/qheXwSxwETVO6kMEW1ndNIWDP8DPQ0E4O//RuMZwxpnZdnjGKkdVNy8I1BpgDgn
lwgkE3H3IciASki1GYXoyvrIiRwMQVzvYD2zcgwK9OZSjZe0TGwAGa+eQdbs3A9I
Ir1kYn6ZMGMRFJA2XHJW3hMZdWB/t2xMBGy75Uv9sAECgYEA1o+oRUYwwQ1MwBo9
YA6c00KjhFgrjdzyKPQrN14Q0dw5ErqRkhp2cs7BRdCDTDrjAegPc3Otg7uMa1vp
RgU/C72jwzFLYATvn+RLGRYRyqIE+bQ22/lLnXTrp4DCfdMrqWuQbIYouGHqfQrq
MfdtSUpQ6VZCi9zHehXOYwBMvQECgYEA1DTQFpe+tndIFmguxxaBwDltoPh5omzd
3vA7iFct2+UYk5W9shfAekAaZk2WufKmmC3OfBWYyIaJ7QwQpuGDS3zwjy6WFMTE
Otp2CypFCVahwHcvn2jYHmDMT0k0Pt6X2S3GAyWTyEPv7mAfKR1OWUYi7ZgdXpt0
TtL3Z3JyhH0CgYEAwveHUGuXodUUCPvPCZo9pzrGm1wDN8WtxskY/Bbd8dTLh9lA
riKdv3Vg6q+un3ZjETht0dsrsKib0HKUZqwdve11AcmpVHcnx4MLOqBzSk4vdzfr
IbhGna3A9VRrZyqcYjb75aGDHwjaqwVgCkdrZ03AeEeJ8M2N9cIa6Js9IAECgYBu
nlU24cVdspJWc9qml3ntrUITnlMxs1R5KXuvF9rk/OixzmYDV1RTpeTdHWcL6Yyk
WYSAtHVfWpq9ggOQKpBZonh3+w3rJ6MvFsBgE5nHQ2ywOrENhQbb1xPJ5NwiRcCc
Srsk2srNo3SIK30y3n8AFIqSljABKEIZ8Olc+JDvtQKBgQCiKz43zI6a0HscgZ77
DCBduWP4nk8BM7QTFxs9VypjrylMDGGtTKHc5BLA5fNZw97Hb7pcicN7/IbUnQUD
pz01y53wMSTJs0ocAxkYvUc5laF+vMsLpG2vp8f35w8uKuO7+vm5LAjUsPd099jG
2qWm8jTPeDC3sq+67s2oojHf+Q==
-----END TESTING KEY-----`, "TESTING KEY", "PRIVATE KEY"))

func testVariants(t *testing.T, f func(t *testing.T, local nbconn.Conn, remote net.Conn)) {
	for _, tt := range []struct {
		name              string
		makeConns         func(t *testing.T) (local, remote net.Conn)
		useTLS            bool
		fakeNonBlockingIO bool
	}{
		{
			name:              "Pipe",
			makeConns:         makePipeConns,
			useTLS:            false,
			fakeNonBlockingIO: true,
		},
		{
			name:              "TCP with Fake Non-blocking IO",
			makeConns:         makeTCPConns,
			useTLS:            false,
			fakeNonBlockingIO: true,
		},
		{
			name:              "TLS over TCP with Fake Non-blocking IO",
			makeConns:         makeTCPConns,
			useTLS:            true,
			fakeNonBlockingIO: true,
		},
		{
			name:              "TCP with Real Non-blocking IO",
			makeConns:         makeTCPConns,
			useTLS:            false,
			fakeNonBlockingIO: false,
		},
		{
			name:              "TLS over TCP with Real Non-blocking IO",
			makeConns:         makeTCPConns,
			useTLS:            true,
			fakeNonBlockingIO: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			local, remote := tt.makeConns(t)

			// Just to be sure both ends get closed. Also, it retains a reference so one side of the connection doesn't get
			// garbage collected. This could happen when a test is testing against a non-responsive remote. Since it never
			// uses remote it may be garbage collected leading to the connection being closed.
			defer local.Close()
			defer remote.Close()

			var conn nbconn.Conn
			netConn := nbconn.NewNetConn(local, tt.fakeNonBlockingIO)

			if tt.useTLS {
				cert, err := tls.X509KeyPair(testTLSPublicKey, testTLSPrivateKey)
				require.NoError(t, err)

				tlsServer := tls.Server(remote, &tls.Config{
					Certificates: []tls.Certificate{cert},
				})
				serverTLSHandshakeChan := make(chan error)
				go func() {
					err := tlsServer.Handshake()
					serverTLSHandshakeChan <- err
				}()

				tlsConn, err := nbconn.TLSClient(netConn, &tls.Config{InsecureSkipVerify: true})
				require.NoError(t, err)
				conn = tlsConn

				err = <-serverTLSHandshakeChan
				require.NoError(t, err)
				remote = tlsServer
			} else {
				conn = netConn
			}

			f(t, conn, remote)
		})
	}
}

// makePipeConns returns a connected pair of net.Conns created with net.Pipe(). It is entirely synchronous so it is
// useful for testing an exact sequence of reads and writes with the underlying connection blocking.
func makePipeConns(t *testing.T) (local, remote net.Conn) {
	local, remote = net.Pipe()
	t.Cleanup(func() {
		local.Close()
		remote.Close()
	})

	return local, remote
}

// makeTCPConns returns a connected pair of net.Conns running over TCP on localhost.
func makeTCPConns(t *testing.T) (local, remote net.Conn) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	type acceptResultT struct {
		conn net.Conn
		err  error
	}
	acceptChan := make(chan acceptResultT)

	go func() {
		conn, err := ln.Accept()
		acceptChan <- acceptResultT{conn: conn, err: err}
	}()

	local, err = net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)

	acceptResult := <-acceptChan
	require.NoError(t, acceptResult.err)

	remote = acceptResult.conn

	return local, remote
}

func TestWriteIsBuffered(t *testing.T) {
	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {
		// net.Pipe is synchronous so the Write would block if not buffered.
		writeBuf := []byte("test")
		n, err := conn.Write(writeBuf)
		require.NoError(t, err)
		require.EqualValues(t, 4, n)

		errChan := make(chan error, 1)
		go func() {
			err := conn.Flush()
			errChan <- err
		}()

		readBuf := make([]byte, len(writeBuf))
		_, err = remote.Read(readBuf)
		require.NoError(t, err)

		require.NoError(t, <-errChan)
	})
}

func TestSetWriteDeadlineDoesNotBlockWrite(t *testing.T) {
	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {
		err := conn.SetWriteDeadline(time.Now())
		require.NoError(t, err)

		writeBuf := []byte("test")
		n, err := conn.Write(writeBuf)
		require.NoError(t, err)
		require.EqualValues(t, 4, n)
	})
}

func TestReadFlushesWriteBuffer(t *testing.T) {
	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {
		writeBuf := []byte("test")
		n, err := conn.Write(writeBuf)
		require.NoError(t, err)
		require.EqualValues(t, 4, n)

		errChan := make(chan error, 2)
		go func() {
			readBuf := make([]byte, len(writeBuf))
			_, err := remote.Read(readBuf)
			errChan <- err

			_, err = remote.Write([]byte("okay"))
			errChan <- err
		}()

		readBuf := make([]byte, 4)
		_, err = conn.Read(readBuf)
		require.NoError(t, err)
		require.Equal(t, []byte("okay"), readBuf)

		require.NoError(t, <-errChan)
		require.NoError(t, <-errChan)
	})
}

func TestCloseFlushesWriteBuffer(t *testing.T) {
	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {
		writeBuf := []byte("test")
		n, err := conn.Write(writeBuf)
		require.NoError(t, err)
		require.EqualValues(t, 4, n)

		errChan := make(chan error, 1)
		go func() {
			readBuf := make([]byte, len(writeBuf))
			_, err := remote.Read(readBuf)
			errChan <- err
		}()

		err = conn.Close()
		require.NoError(t, err)

		require.NoError(t, <-errChan)
	})
}

// This test exercises the non-blocking write path. Because writes are buffered it is difficult trigger this with
// certainty and visibility. So this test tries to trigger what would otherwise be a deadlock by both sides writing
// large values.
func TestInternalNonBlockingWrite(t *testing.T) {
	const deadlockSize = 4 * 1024 * 1024

	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {
		writeBuf := make([]byte, deadlockSize)
		n, err := conn.Write(writeBuf)
		require.NoError(t, err)
		require.EqualValues(t, deadlockSize, n)

		errChan := make(chan error, 1)
		go func() {
			remoteWriteBuf := make([]byte, deadlockSize)
			_, err := remote.Write(remoteWriteBuf)
			if err != nil {
				errChan <- err
				return
			}

			readBuf := make([]byte, deadlockSize)
			_, err = io.ReadFull(remote, readBuf)
			errChan <- err
		}()

		readBuf := make([]byte, deadlockSize)
		_, err = io.ReadFull(conn, readBuf)
		require.NoError(t, err)

		err = conn.Close()
		require.NoError(t, err)

		require.NoError(t, <-errChan)
	})
}

func TestInternalNonBlockingWriteWithDeadline(t *testing.T) {
	const deadlockSize = 4 * 1024 * 1024

	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {
		writeBuf := make([]byte, deadlockSize)
		n, err := conn.Write(writeBuf)
		require.NoError(t, err)
		require.EqualValues(t, deadlockSize, n)

		err = conn.SetDeadline(time.Now().Add(100 * time.Millisecond))
		require.NoError(t, err)

		err = conn.Flush()
		require.Error(t, err)
		require.Contains(t, err.Error(), "i/o timeout")
	})
}

func TestNonBlockingRead(t *testing.T) {
	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {
		err := conn.SetReadDeadline(nbconn.NonBlockingDeadline)
		require.NoError(t, err)

		buf := make([]byte, 4)
		n, err := conn.Read(buf)
		require.ErrorIs(t, err, nbconn.ErrWouldBlock)
		require.EqualValues(t, 0, n)

		errChan := make(chan error, 1)
		go func() {
			_, err := remote.Write([]byte("okay"))
			errChan <- err
		}()

		err = conn.SetReadDeadline(time.Time{})
		require.NoError(t, err)

		n, err = conn.Read(buf)
		require.NoError(t, err)
		require.EqualValues(t, 4, n)
	})
}

func TestBufferNonBlockingRead(t *testing.T) {
	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {
		err := conn.BufferReadUntilBlock()
		require.NoError(t, err)

		errChan := make(chan error, 1)
		go func() {
			_, err := remote.Write([]byte("okay"))
			errChan <- err
		}()

		for i := 0; i < 1000; i++ {
			err = conn.BufferReadUntilBlock()
			if !errors.Is(err, nbconn.ErrWouldBlock) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		require.NoError(t, err)

		buf := make([]byte, 4)
		n, err := conn.Read(buf)
		require.NoError(t, err)
		require.EqualValues(t, 4, n)
		require.Equal(t, []byte("okay"), buf)
	})
}

func TestReadPreviouslyBuffered(t *testing.T) {
	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {

		errChan := make(chan error, 1)
		go func() {
			err := func() error {
				_, err := remote.Write([]byte("alpha"))
				if err != nil {
					return err
				}

				readBuf := make([]byte, 4)
				_, err = remote.Read(readBuf)
				if err != nil {
					return err
				}

				return nil
			}()
			errChan <- err
		}()

		_, err := conn.Write([]byte("test"))
		require.NoError(t, err)

		// Because net.Pipe() is synchronous conn.Flush must buffer a read.
		err = conn.Flush()
		require.NoError(t, err)

		readBuf := make([]byte, 5)
		n, err := conn.Read(readBuf)
		require.NoError(t, err)
		require.EqualValues(t, 5, n)
		require.Equal(t, []byte("alpha"), readBuf)
	})
}

func TestReadMoreThanPreviouslyBufferedDoesNotBlock(t *testing.T) {
	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {
		errChan := make(chan error, 1)
		go func() {
			err := func() error {
				_, err := remote.Write([]byte("alpha"))
				if err != nil {
					return err
				}

				readBuf := make([]byte, 4)
				_, err = remote.Read(readBuf)
				if err != nil {
					return err
				}

				return nil
			}()
			errChan <- err
		}()

		_, err := conn.Write([]byte("test"))
		require.NoError(t, err)

		// Because net.Pipe() is synchronous conn.Flush must buffer a read.
		err = conn.Flush()
		require.NoError(t, err)

		readBuf := make([]byte, 10)
		n, err := conn.Read(readBuf)
		require.NoError(t, err)
		require.EqualValues(t, 5, n)
		require.Equal(t, []byte("alpha"), readBuf[:n])
	})
}

func TestReadPreviouslyBufferedPartialRead(t *testing.T) {
	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {

		errChan := make(chan error, 1)
		go func() {
			err := func() error {
				_, err := remote.Write([]byte("alpha"))
				if err != nil {
					return err
				}

				readBuf := make([]byte, 4)
				_, err = remote.Read(readBuf)
				if err != nil {
					return err
				}

				return nil
			}()
			errChan <- err
		}()

		_, err := conn.Write([]byte("test"))
		require.NoError(t, err)

		// Because net.Pipe() is synchronous conn.Flush must buffer a read.
		err = conn.Flush()
		require.NoError(t, err)

		readBuf := make([]byte, 2)
		n, err := conn.Read(readBuf)
		require.NoError(t, err)
		require.EqualValues(t, 2, n)
		require.Equal(t, []byte("al"), readBuf)

		readBuf = make([]byte, 3)
		n, err = conn.Read(readBuf)
		require.NoError(t, err)
		require.EqualValues(t, 3, n)
		require.Equal(t, []byte("pha"), readBuf)
	})
}

func TestReadMultiplePreviouslyBuffered(t *testing.T) {
	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {
		errChan := make(chan error, 1)
		go func() {
			err := func() error {
				_, err := remote.Write([]byte("alpha"))
				if err != nil {
					return err
				}

				_, err = remote.Write([]byte("beta"))
				if err != nil {
					return err
				}

				readBuf := make([]byte, 4)
				_, err = remote.Read(readBuf)
				if err != nil {
					return err
				}

				return nil
			}()
			errChan <- err
		}()

		_, err := conn.Write([]byte("test"))
		require.NoError(t, err)

		// Because net.Pipe() is synchronous conn.Flush must buffer a read.
		err = conn.Flush()
		require.NoError(t, err)

		readBuf := make([]byte, 9)
		n, err := io.ReadFull(conn, readBuf)
		require.NoError(t, err)
		require.EqualValues(t, 9, n)
		require.Equal(t, []byte("alphabeta"), readBuf)
	})
}

func TestReadPreviouslyBufferedAndReadMore(t *testing.T) {
	testVariants(t, func(t *testing.T, conn nbconn.Conn, remote net.Conn) {

		flushCompleteChan := make(chan struct{})
		errChan := make(chan error, 1)
		go func() {
			err := func() error {
				_, err := remote.Write([]byte("alpha"))
				if err != nil {
					return err
				}

				readBuf := make([]byte, 4)
				_, err = remote.Read(readBuf)
				if err != nil {
					return err
				}

				<-flushCompleteChan

				_, err = remote.Write([]byte("beta"))
				if err != nil {
					return err
				}

				return nil
			}()
			errChan <- err
		}()

		_, err := conn.Write([]byte("test"))
		require.NoError(t, err)

		// Because net.Pipe() is synchronous conn.Flush must buffer a read.
		err = conn.Flush()
		require.NoError(t, err)

		close(flushCompleteChan)

		readBuf := make([]byte, 9)

		n, err := io.ReadFull(conn, readBuf)
		require.NoError(t, err)
		require.EqualValues(t, 9, n)
		require.Equal(t, []byte("alphabeta"), readBuf)

		err = <-errChan
		require.NoError(t, err)
	})
}
