package pool

import (
	"time"

	"github.com/jackc/puddle"
)

type Stat struct {
	s *puddle.Stat
}

func (s *Stat) AcquireCount() int64 {
	return s.s.AcquireCount()
}

func (s *Stat) AcquireDuration() time.Duration {
	return s.s.AcquireDuration()
}

func (s *Stat) AcquiredConns() int {
	return s.s.AcquiredResources()
}

func (s *Stat) CanceledAcquireCount() int64 {
	return s.s.CanceledAcquireCount()
}

func (s *Stat) ConstructingConns() int {
	return s.s.ConstructingResources()
}

func (s *Stat) EmptyAcquireCount() int64 {
	return s.s.EmptyAcquireCount()
}

func (s *Stat) IdleConns() int {
	return s.s.IdleResources()
}

func (s *Stat) MaxConns() int {
	return s.s.MaxResources()
}

func (s *Stat) TotalConns() int {
	return s.s.TotalResources()
}
