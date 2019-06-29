package pgxpool

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

func (s *Stat) AcquiredConns() int32 {
	return s.s.AcquiredResources()
}

func (s *Stat) CanceledAcquireCount() int64 {
	return s.s.CanceledAcquireCount()
}

func (s *Stat) ConstructingConns() int32 {
	return s.s.ConstructingResources()
}

func (s *Stat) EmptyAcquireCount() int64 {
	return s.s.EmptyAcquireCount()
}

func (s *Stat) IdleConns() int32 {
	return s.s.IdleResources()
}

func (s *Stat) MaxConns() int32 {
	return s.s.MaxResources()
}

func (s *Stat) TotalConns() int32 {
	return s.s.TotalResources()
}
