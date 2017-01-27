package pgx_test

import (
	"fmt"
	"github.com/jackc/pgx"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This function uses a postgresql 9.6 specific column
func getConfirmedFlushLsnFor(t *testing.T, conn *pgx.Conn, slot string) string {
	// Fetch the restart LSN of the slot, to establish a starting point
	rows, err := conn.Query(fmt.Sprintf("select confirmed_flush_lsn from pg_replication_slots where slot_name='%s'", slot))
	if err != nil {
		t.Fatalf("conn.Query failed: %v", err)
	}
	defer rows.Close()

	var restartLsn string
	for rows.Next() {
		rows.Scan(&restartLsn)
	}
	return restartLsn
}

// This battleship test (at least somewhat by necessity) does
// several things all at once in a single run. It:
// - Establishes a replication connection & slot
// - Does a series of operations to create some known WAL entries
// - Replicates the entries down, and checks that the rows it
//   created come down in order
// - Sends a standby status message to update the server with the
//   wal position of the slot
// - Checks the wal position of the slot on the server to make sure
//   the update succeeded
func TestSimpleReplicationConnection(t *testing.T) {
	t.Parallel()

	var err error

	if replicationConnConfig == nil {
		t.Skip("Skipping due to undefined replicationConnConfig")
	}

	conn := mustConnect(t, *replicationConnConfig)
	defer closeConn(t, conn)

	replicationConn := mustReplicationConnect(t, *replicationConnConfig)
	defer closeReplicationConn(t, replicationConn)

	err = replicationConn.CreateReplicationSlot("pgx_test", "test_decoding")
	if err != nil {
		t.Logf("replication slot create failed: %v", err)
	}

	// Do a simple change so we can get some wal data
	_, err = conn.Exec("create table if not exists replication_test (a integer)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = replicationConn.StartReplication("pgx_test", 0, -1)
	if err != nil {
		t.Fatalf("Failed to start replication: %v", err)
	}

	var i int32
	var insertedTimes []int64
	for i < 5 {
		var ct pgx.CommandTag
		currentTime := time.Now().Unix()
		insertedTimes = append(insertedTimes, currentTime)
		ct, err = conn.Exec("insert into replication_test(a) values($1)", currentTime)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		t.Logf("Inserted %d rows", ct.RowsAffected())
		i++
	}

	i = 0
	var foundTimes []int64
	var foundCount int
	var maxWal uint64
	for {
		var message *pgx.ReplicationMessage

		message, err = replicationConn.WaitForReplicationMessage(time.Duration(1 * time.Second))
		if err != nil {
			if err != pgx.ErrNotificationTimeout {
				t.Fatalf("Replication failed: %v %s", err, reflect.TypeOf(err))
			}
		}
		if message != nil {
			if message.WalMessage != nil {
				// The waldata payload with the test_decoding plugin looks like:
				// public.replication_test: INSERT: a[integer]:2
				// What we wanna do here is check that once we find one of our inserted times,
				// that they occur in the wal stream in the order we executed them.
				walString := string(message.WalMessage.WalData)
				if strings.Contains(walString, "public.replication_test: INSERT") {
					stringParts := strings.Split(walString, ":")
					offset, err := strconv.ParseInt(stringParts[len(stringParts)-1], 10, 64)
					if err != nil {
						t.Fatalf("Failed to parse walString %s", walString)
					}
					if foundCount > 0 || offset == insertedTimes[0] {
						foundTimes = append(foundTimes, offset)
						foundCount++
					}
				}
				if message.WalMessage.WalStart > maxWal {
					maxWal = message.WalMessage.WalStart
				}

			}
			if message.ServerHeartbeat != nil {
				t.Logf("Got heartbeat: %s", message.ServerHeartbeat)
			}
		} else {
			t.Log("Timed out waiting for wal message")
			i++
		}
		if i > 3 {
			t.Log("Actual timeout")
			break
		}
	}

	if foundCount != len(insertedTimes) {
		t.Fatalf("Failed to find all inserted time values in WAL stream (found %d expected %d)", foundCount, len(insertedTimes))
	}

	for i := range insertedTimes {
		if foundTimes[i] != insertedTimes[i] {
			t.Fatalf("Found %d expected %d", foundTimes[i], insertedTimes[i])
		}
	}

	t.Logf("Found %d times, as expected", len(foundTimes))

	// Before closing our connection, let's send a standby status to update our wal
	// position, which should then be reflected if we fetch out our current wal position
	// for the slot
	status, err := pgx.NewStandbyStatus(maxWal)
	if err != nil {
		t.Errorf("Failed to create standby status %v", err)
	}
	replicationConn.SendStandbyStatus(status)
	replicationConn.StopReplication()

	if replicationConn.IsAlive() == false {
		t.Errorf("Connection died: %v", replicationConn.CauseOfDeath())
	}

	err = replicationConn.Close()
	if err != nil {
		t.Fatalf("Replication connection close failed: %v", err)
	}

	// Let's push the boundary conditions of the standby status and ensure it errors correctly
	status, err = pgx.NewStandbyStatus(0, 1, 2, 3, 4)
	if err == nil {
		t.Errorf("Expected error from new standby status, got %v", status)
	}

	// And if you provide 3 args, ensure the right fields are set
	status, err = pgx.NewStandbyStatus(1, 2, 3)
	if err != nil {
		t.Errorf("Failed to create test status: %v", err)
	}
	if status.WalFlushPosition != 1 {
		t.Errorf("Unexpected flush position %d", status.WalFlushPosition)
	}
	if status.WalApplyPosition != 2 {
		t.Errorf("Unexpected apply position %d", status.WalApplyPosition)
	}
	if status.WalWritePosition != 3 {
		t.Errorf("Unexpected write position %d", status.WalWritePosition)
	}

	restartLsn := getConfirmedFlushLsnFor(t, conn, "pgx_test")
	integerRestartLsn, _ := pgx.ParseLSN(restartLsn)
	if integerRestartLsn != maxWal {
		t.Fatalf("Wal offset update failed, expected %s found %s", pgx.FormatLSN(maxWal), restartLsn)
	}

	replicationConn2 := mustReplicationConnect(t, *replicationConnConfig)
	defer closeReplicationConn(t, replicationConn2)

	err = replicationConn2.DropReplicationSlot("pgx_test")
	if err != nil {
		t.Fatalf("Failed to drop replication slot: %v", err)
	}

	droppedLsn := getConfirmedFlushLsnFor(t, conn, "pgx_test")
	if droppedLsn != "" {
		t.Errorf("Got odd flush lsn %s for supposedly dropped slot", droppedLsn)
	}

}
