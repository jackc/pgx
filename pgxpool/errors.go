package pgxpool

import "errors"

var ErrInfiniteAcquireLoop = errors.New("pgxpool: detected infinite loop acquiring connection; likely bug in PrepareConn or BeforeAcquire hook")
