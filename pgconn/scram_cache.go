package pgconn

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"
)

// ScramCacheFingerprint is an opaque cache entry identifier for [ScramDeriveCache].
// pgx computes it while authenticating; implementations must use only the values
// passed to Get, Put, and Delete (do not construct fingerprints yourself).
type ScramCacheFingerprint [32]byte

// ScramSaltedPassword is the 32-byte PBKDF2 output cached by [ScramDeriveCache].
type ScramSaltedPassword [32]byte

func (sc *scramClient) deriveFingerprint() ScramCacheFingerprint {
	h := sha256.New()
	h.Write([]byte(sc.password))
	h.Write([]byte{0})
	h.Write(sc.salt)
	h.Write([]byte{0})
	var iter [8]byte
	binary.BigEndian.PutUint64(iter[:], sc.iterations)
	h.Write(iter[:])
	var fp ScramCacheFingerprint
	h.Sum(fp[:0])
	return fp
}

// ScramDeriveCache stores the 32-byte SCRAM salted password (PBKDF2 output) so new connections
// can skip PBKDF2 when the server verifier is unchanged.
//
// Method arguments never include passwords or raw salts—only opaque [ScramCacheFingerprint]
// values produced by pgx during SCRAM.
type ScramDeriveCache interface {
	Get(ScramCacheFingerprint) (ScramSaltedPassword, bool)
	Put(ScramCacheFingerprint, ScramSaltedPassword)
	Delete(ScramCacheFingerprint)
}

// SimpleScramDeriveCache is a small mutex-backed map implementation of [ScramDeriveCache]
// for tests and applications that do not need LRU eviction.
type SimpleScramDeriveCache struct {
	mu sync.Mutex
	m  map[ScramCacheFingerprint]ScramSaltedPassword
}

// NewSimpleScramDeriveCache returns an empty [SimpleScramDeriveCache].
func NewSimpleScramDeriveCache() *SimpleScramDeriveCache {
	return &SimpleScramDeriveCache{
		m: make(map[ScramCacheFingerprint]ScramSaltedPassword),
	}
}

// Get implements [ScramDeriveCache].
func (s *SimpleScramDeriveCache) Get(fp ScramCacheFingerprint) (ScramSaltedPassword, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[fp]
	return v, ok
}

// Put implements [ScramDeriveCache].
func (s *SimpleScramDeriveCache) Put(fp ScramCacheFingerprint, sp ScramSaltedPassword) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[fp] = sp
}

// Delete implements [ScramDeriveCache].
func (s *SimpleScramDeriveCache) Delete(fp ScramCacheFingerprint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, fp)
}
