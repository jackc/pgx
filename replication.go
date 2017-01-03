package pgx

import (
	"errors"
	"fmt"
	"net"
	"time"
)

const (
	copyBothResponse    = 'W'
	walData             = 'w'
	senderKeepalive     = 'k'
	standbyStatusUpdate = 'r'
	initialReplicationResponseTimeout = 5 * time.Second
)

var epochNano int64

func init() {
	epochNano = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
}

// Format the given 64bit LSN value into the XXX/XXX format,
// which is the format reported by postgres.
func FormatLSN(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}

// Parse the given XXX/XXX format LSN as reported by postgres,
// into a 64 bit integer as used internally by the wire procotols
func ParseLSN(lsn string) (outputLsn uint64, err error) {
	var upperHalf uint64
	var lowerHalf uint64
	var nparsed int
	nparsed, err = fmt.Sscanf(lsn, "%X/%X", &upperHalf, &lowerHalf)
	if err != nil {
		return
	}

	if nparsed != 2 {
		err = errors.New(fmt.Sprintf("Failed to parsed LSN: %s", lsn))
		return
	}

	outputLsn = (upperHalf << 32) + lowerHalf
	return
}

// The WAL message contains WAL payload entry data
type WalMessage struct {
	// The WAL start position of this data. This
	// is the WAL position we need to track.
	WalStart uint64
	// The server wal end and server time are
	// documented to track the end position and current
	// time of the server, both of which appear to be
	// unimplemented in pg 9.5.
	ServerWalEnd uint64
	ServerTime   uint64
	// The WAL data is the raw unparsed binary WAL entry.
	// The contents of this are determined by the output
	// logical encoding plugin.
	WalData []byte
}

func (w *WalMessage) Time() time.Time {
	return time.Unix(0, (int64(w.ServerTime)*1000)+epochNano)
}

func (w *WalMessage) ByteLag() uint64 {
	return (w.ServerWalEnd - w.WalStart)
}

func (w *WalMessage) String() string {
	return fmt.Sprintf("Wal: %s Time: %s Lag: %d", FormatLSN(w.WalStart), w.Time(), w.ByteLag())
}

// The server heartbeat is sent periodically from the server,
// including server status, and a reply request field
type ServerHeartbeat struct {
	// The current max wal position on the server,
	// used for lag tracking
	ServerWalEnd uint64
	// The server time, in microseconds since jan 1 2000
	ServerTime uint64
	// If 1, the server is requesting a standby status message
	// to be sent immediately.
	ReplyRequested byte
}

func (s *ServerHeartbeat) Time() time.Time {
	return time.Unix(0, (int64(s.ServerTime)*1000)+epochNano)
}

func (s *ServerHeartbeat) String() string {
	return fmt.Sprintf("WalEnd: %s ReplyRequested: %d T: %s", FormatLSN(s.ServerWalEnd), s.ReplyRequested, s.Time())
}

// The replication message wraps all possible messages from the
// server received during replication. At most one of the wal message
// or server heartbeat will be non-nil
type ReplicationMessage struct {
	WalMessage      *WalMessage
	ServerHeartbeat *ServerHeartbeat
}

// The standby status is the client side heartbeat sent to the postgresql
// server to track the client wal positions. For practical purposes,
// all wal positions are typically set to the same value.
type StandbyStatus struct {
	// The WAL position that's been locally written
	WalWritePosition uint64
	// The WAL position that's been locally flushed
	WalFlushPosition uint64
	// The WAL position that's been locally applied
	WalApplyPosition uint64
	// The client time in microseconds since jan 1 2000
	ClientTime uint64
	// If 1, requests the server to immediately send a
	// server heartbeat
	ReplyRequested byte
}

// Create a standby status struct, which sets all the WAL positions
// to the given wal position, and the client time to the current time.
// The wal positions are, in order:
// WalFlushPosition
// WalApplyPosition
// WalWritePosition
//
// If only one position is provided, it will be used as the value for all 3
// status fields. Note you must provide either 1 wal position, or all 3
// in order to initialize the standby status.
func NewStandbyStatus(walPositions ...uint64) (status *StandbyStatus, err error) {
	if len(walPositions) == 1 {
		status = new(StandbyStatus)
		status.WalFlushPosition = walPositions[0]
		status.WalApplyPosition = walPositions[0]
		status.WalWritePosition = walPositions[0]
	} else if len(walPositions) == 3 {
		status = new(StandbyStatus)
		status.WalFlushPosition = walPositions[0]
		status.WalApplyPosition = walPositions[1]
		status.WalWritePosition = walPositions[2]
	} else {
		err = errors.New(fmt.Sprintf("Invalid number of wal positions provided, need 1 or 3, got %d", len(walPositions)))
		return
	}
	status.ClientTime = uint64((time.Now().UnixNano() - epochNano) / 1000)
	return
}

// Send standby status to the server, which both acts as a keepalive
// message to the server, as well as carries the WAL position of the
// client, which then updates the server's replication slot position.
func (c *Conn) SendStandbyStatus(k *StandbyStatus) (err error) {
	writeBuf := newWriteBuf(c, copyData)
	writeBuf.WriteByte(standbyStatusUpdate)
	writeBuf.WriteInt64(int64(k.WalWritePosition))
	writeBuf.WriteInt64(int64(k.WalFlushPosition))
	writeBuf.WriteInt64(int64(k.WalApplyPosition))
	writeBuf.WriteInt64(int64(k.ClientTime))
	writeBuf.WriteByte(k.ReplyRequested)

	writeBuf.closeMsg()

	_, err = c.conn.Write(writeBuf.buf)
	if err != nil {
		c.die(err)
	}

	return
}

// Send the message to formally stop the replication stream. This
// is done before calling Close() during a clean shutdown.
func (c *Conn) StopReplication() (err error) {
	writeBuf := newWriteBuf(c, copyDone)

	writeBuf.closeMsg()

	_, err = c.conn.Write(writeBuf.buf)
	if err != nil {
		c.die(err)
	}
	return
}


func (c *Conn) readReplicationMessage() (r *ReplicationMessage, err error) {
	var t byte
	var reader *msgReader
	t, reader, err = c.rxMsg()
	if err != nil {
		return
	}

	switch t {
	case noticeResponse:
		pgError := c.rxErrorResponse(reader)
		if c.shouldLog(LogLevelInfo) {
			c.log(LogLevelInfo, pgError.Error())
		}
	case errorResponse:
		err = c.rxErrorResponse(reader)
		if c.shouldLog(LogLevelError) {
			c.log(LogLevelError, err.Error())
		}
		return
	case copyBothResponse:
		// This is the tail end of the replication process start,
		// and can be safely ignored
		return
	case copyData:
		var msgType byte
		msgType = reader.readByte()
		switch msgType {
		case walData:
			walStart := reader.readInt64()
			serverWalEnd := reader.readInt64()
			serverTime := reader.readInt64()
			walData := reader.readBytes(reader.msgBytesRemaining)
			walMessage := WalMessage{WalStart: uint64(walStart),
				ServerWalEnd: uint64(serverWalEnd),
				ServerTime:   uint64(serverTime),
				WalData:      walData,
			}

			return &ReplicationMessage{WalMessage: &walMessage}, nil
		case senderKeepalive:
			serverWalEnd := reader.readInt64()
			serverTime := reader.readInt64()
			replyNow := reader.readByte()
			h := &ServerHeartbeat{ServerWalEnd: uint64(serverWalEnd), ServerTime: uint64(serverTime), ReplyRequested: replyNow}
			return &ReplicationMessage{ServerHeartbeat: h}, nil
		default:
			if c.shouldLog(LogLevelError) {
				c.log(LogLevelError,"Unexpected data playload message type %v", t)
			}
		}
	default:
		if c.shouldLog(LogLevelError) {
			c.log(LogLevelError,"Unexpected replication message type %v", t)
		}
	}
	return
}

// Wait for a single replication message up to timeout time.
//
// Properly using this requires some knowledge of the postgres replication mechanisms,
// as the client can receive both WAL data (the ultimate payload) and server heartbeat
// updates. The caller also must send standby status updates in order to keep the connection
// alive and working.
//
// This returns pgx.ErrNotificationTimeout when there is no replication message by the specified
// duration.
func (c *Conn) WaitForReplicationMessage(timeout time.Duration) (r *ReplicationMessage, err error) {
	var zeroTime time.Time

	deadline := time.Now().Add(timeout)

	// Use SetReadDeadline to implement the timeout. SetReadDeadline will
	// cause operations to fail with a *net.OpError that has a Timeout()
	// of true. Because the normal pgx rxMsg path considers any error to
	// have potentially corrupted the state of the connection, it dies
	// on any errors. So to avoid timeout errors in rxMsg we set the
	// deadline and peek into the reader. If a timeout error occurs there
	// we don't break the pgx connection. If the Peek returns that data
	// is available then we turn off the read deadline before the rxMsg.
	err = c.conn.SetReadDeadline(deadline)
	if err != nil {
		return nil, err
	}

	// Wait until there is a byte available before continuing onto the normal msg reading path
	_, err = c.reader.Peek(1)
	if err != nil {
		c.conn.SetReadDeadline(zeroTime) // we can only return one error and we already have one -- so ignore possiple error from SetReadDeadline
		if err, ok := err.(*net.OpError); ok && err.Timeout() {
			return nil, ErrNotificationTimeout
		}
		return nil, err
	}

	err = c.conn.SetReadDeadline(zeroTime)
	if err != nil {
		return nil, err
	}

	return c.readReplicationMessage()
}

// Start a replication connection, sending WAL data to the given replication
// receiver. This function wraps a START_REPLICATION command as documented
// here:
// https://www.postgresql.org/docs/9.5/static/protocol-replication.html
//
// Once started, the client needs to invoke WaitForReplicationMessage() in order
// to fetch the WAL and standby status. Also, it is the responsibility of the caller
// to periodically send StandbyStatus messages to update the replication slot position.
//
// This function assumes that slotName has already been created. In order to omit the timeline argument
// pass a -1 for the timeline to get the server default behavior.
func (c *Conn) StartReplication(slotName string, startLsn uint64, timeline int64, pluginArguments ...string) (err error) {
	var queryString string
	if timeline >= 0 {
		queryString = fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s TIMELINE %d", slotName, FormatLSN(startLsn), timeline)
	} else {
		queryString = fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s", slotName, FormatLSN(startLsn))
	}

	for _, arg := range pluginArguments {
		queryString += fmt.Sprintf(" %s", arg)
	}

	if err = c.sendQuery(queryString); err != nil {
		return
	}

	// The first replication message that comes back here will be (in a success case)
	// a empty CopyBoth that is (apparently) sent as the confirmation that the replication has
	// started. This call will either return nil, nil or if it returns an error
	// that indicates the start replication command failed
	var r *ReplicationMessage
	r, err = c.WaitForReplicationMessage(initialReplicationResponseTimeout)
	if err != nil && r != nil {
		if c.shouldLog(LogLevelError) {
			c.log(LogLevelError, "Unxpected replication message %v", r)
		}
	}

	return
}
