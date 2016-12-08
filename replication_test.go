package pgx_test

import (
	"github.com/jackc/pgx"
	"strconv"
	"strings"
	"testing"
	"time"
	"reflect"
)

// This function uses a postgresql 9.6 specific column
func getConfirmedFlushLsnFor(t *testing.T, conn *pgx.Conn, slot string) string {
	// Fetch the restart LSN of the slot, to establish a starting point
	rows, err := conn.Query("select confirmed_flush_lsn from pg_replication_slots where slot_name='pgx_test'")
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
	var replicationUserConfig pgx.ConnConfig
	var replicationConnConfig pgx.ConnConfig

	replicationUserConfig = *defaultConnConfig
	replicationUserConfig.User = "pgx_replication"
	conn := mustConnect(t, replicationUserConfig)
	defer closeConn(t, conn)

	replicationConnConfig = *defaultConnConfig
	replicationConnConfig.User = "pgx_replication"
	replicationConnConfig.RuntimeParams = make(map[string]string)
	replicationConnConfig.RuntimeParams["replication"] = "database"

	replicationConn := mustConnect(t, replicationConnConfig)
	defer closeConn(t, replicationConn)

	_, err = replicationConn.Exec("CREATE_REPLICATION_SLOT pgx_test LOGICAL test_decoding")
	if err != nil {
		t.Logf("replication slot create failed: %v", err)
	}

	// Do a simple change so we can get some wal data
	_, err = conn.Exec("create table if not exists replication_test (a integer)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = replicationConn.StartReplication("START_REPLICATION SLOT pgx_test LOGICAL 0/0")
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
	replicationConn.SendStandbyStatus(pgx.NewStandbyStatus(maxWal))
	replicationConn.StopReplication()

	err = replicationConn.Close()
	if err != nil {
		t.Fatalf("Replication connection close failed: %v", err)
	}

	restartLsn := getConfirmedFlushLsnFor(t, conn, "pgx_test")
	integerRestartLsn, _ := pgx.ParseLSN(restartLsn)
	if integerRestartLsn != maxWal {
		t.Fatalf("Wal offset update failed, expected %s found %s", pgx.FormatLSN(maxWal), restartLsn)
	}

	_, err = conn.Exec("select pg_drop_replication_slot($1)", "pgx_test")
	if err != nil {
		t.Fatalf("Failed to drop replication slot: %v", err)
	}

}
