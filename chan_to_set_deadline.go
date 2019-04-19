package pgconn

import (
	"time"
)

var deadlineTime = time.Date(1, 1, 1, 1, 1, 1, 1, time.UTC)

type setDeadliner interface {
	SetDeadline(time.Time) error
}

type chanToSetDeadline struct {
	cleanupChan     chan struct{}
	conn            setDeadliner
	deadlineWasSet  bool
	cleanupComplete bool
}

func (this *chanToSetDeadline) start(doneChan <-chan struct{}, conn setDeadliner) {
	if this.cleanupChan == nil {
		this.cleanupChan = make(chan struct{})
	}
	this.conn = conn
	this.deadlineWasSet = false
	this.cleanupComplete = false

	if doneChan != nil {
		go func() {
			select {
			case <-doneChan:
				conn.SetDeadline(deadlineTime)
				this.deadlineWasSet = true
				<-this.cleanupChan
			case <-this.cleanupChan:
			}
		}()
	} else {
		this.cleanupComplete = true
	}
}

func (this *chanToSetDeadline) cleanup() {
	if !this.cleanupComplete {
		this.cleanupChan <- struct{}{}
		if this.deadlineWasSet {
			this.conn.SetDeadline(time.Time{})
		}
		this.cleanupComplete = true
	}
}
