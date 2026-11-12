package pgmock

import (
	"context"
	"errors"
	"net"
	"sync"
)

// ConnectionScript drives one socket of a connection attempt. Run must finish
// when the peer closes; a fixture's Close also interrupts any remaining scripts.
type ConnectionScript struct {
	Network, Address string
	Run              func(net.Conn) error
}

// Connections is a deterministic, in-process transport simulator. It records
// every attempted socket and script error without requiring PostgreSQL or libpq.
type Connections struct {
	mu      sync.Mutex
	Scripts []ConnectionScript
	Calls   []ConnectionScript
	Errors  []error
	conns   []net.Conn
	wg      sync.WaitGroup
}

func (s *Connections) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	index := len(s.Calls)
	s.Calls = append(s.Calls, ConnectionScript{Network: network, Address: address})
	if index >= len(s.Scripts) {
		s.mu.Unlock()
		return nil, errors.New("unexpected connection attempt")
	}
	script := s.Scripts[index]
	if script.Network != "" && script.Network != network || script.Address != "" && script.Address != address {
		s.mu.Unlock()
		return nil, errors.New("unexpected destination")
	}
	if script.Run == nil {
		s.mu.Unlock()
		return nil, errors.New("simulated dial failure")
	}
	client, server := net.Pipe()
	s.conns = append(s.conns, server)
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.wg.Done()
		defer server.Close()
		if err := script.Run(server); err != nil {
			s.mu.Lock()
			s.Errors = append(s.Errors, err)
			s.mu.Unlock()
		}
	}()
	return client, nil
}

func (s *Connections) Close() {
	s.mu.Lock()
	for _, c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}
