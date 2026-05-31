package pgconn_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeResolver returns a net.Resolver that queries the server specified by the
// PGX_TEST_SRV_DNS_SERVER environment variable (e.g. "ns2.nameself.com" or
// "8.8.8.8"), or the system default resolver if the variable is not set.
func makeResolver() *net.Resolver {
	server := os.Getenv("PGX_TEST_SRV_DNS_SERVER")
	if server == "" {
		return net.DefaultResolver
	}
	// Ensure server has a port.
	if !strings.Contains(server, ":") {
		server = server + ":53"
	}
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{}
			return d.DialContext(ctx, "udp", server)
		},
	}
}

// mockSRV builds a LookupSRVFunc that returns a fixed list of SRV records
// constructed from the provided host:port strings.
func mockSRV(targets ...string) pgconn.LookupSRVFunc {
	var srvs []*net.SRV
	for i, t := range targets {
		host, portStr, err := net.SplitHostPort(t)
		if err != nil {
			panic(fmt.Sprintf("mockSRV: invalid target %q: %v", t, err))
		}
		var port uint64
		fmt.Sscan(portStr, &port)
		srvs = append(srvs, &net.SRV{
			Target:   host,
			Port:     uint16(port),
			Priority: 0,
			Weight:   uint16(len(targets) - i), // first entry has highest weight
		})
	}
	return func(_ context.Context, service, proto, name string) (string, []*net.SRV, error) {
		return name, srvs, nil
	}
}

// TestParseConfigSRVScheme verifies that postgres+srv:// and postgresql+srv://
// URI schemes populate SRVHost and leave Host empty.
func TestParseConfigSRVScheme(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		connString string
		wantSRV    string
	}{
		{
			name:       "postgres+srv scheme",
			connString: "postgres+srv://bob:secret@cluster.example.com/mydb?sslmode=disable",
			wantSRV:    "cluster.example.com",
		},
		{
			name:       "postgresql+srv scheme",
			connString: "postgresql+srv://bob:secret@cluster.example.com/mydb?sslmode=disable",
			wantSRV:    "cluster.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			config, err := pgconn.ParseConfig(tt.connString)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSRV, config.SRVHost)
			assert.NotNil(t, config.LookupSRVFunc)
			assert.Equal(t, "bob", config.User)
			assert.Equal(t, "mydb", config.Database)
		})
	}
}

// TestParseConfigSRVKeyword verifies that the srvhost= keyword sets SRVHost.
func TestParseConfigSRVKeyword(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig("srvhost=cluster.example.com user=bob dbname=mydb sslmode=disable")
	require.NoError(t, err)
	assert.Equal(t, "cluster.example.com", config.SRVHost)
	assert.NotNil(t, config.LookupSRVFunc)
}

// TestParseConfigSRVAndHostMutuallyExclusive verifies that specifying both
// srvhost and host returns an error.
func TestParseConfigSRVAndHostMutuallyExclusive(t *testing.T) {
	t.Parallel()

	_, err := pgconn.ParseConfig("srvhost=cluster.example.com host=pg1.example.com sslmode=disable")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

// TestConnectSRVMocked verifies end-to-end SRV connectivity using a mocked
// LookupSRVFunc.  The mock returns the address of a real running Postgres
// instance (taken from PGX_TEST_TCP_CONN_STRING) so no DNS server is needed.
func TestConnectSRVMocked(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("PGX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping: PGX_TEST_TCP_CONN_STRING not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Parse the real connection string to extract host/port.
	realConfig, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)

	// Build an SRV connection string that names a fake cluster host.
	// The mock will redirect it to the real server.
	srvConnString := fmt.Sprintf(
		"postgres+srv://%s:%s@fake-cluster.test/%s?sslmode=disable",
		realConfig.User, realConfig.Password, realConfig.Database,
	)

	config, err := pgconn.ParseConfig(srvConnString)
	require.NoError(t, err)
	assert.Equal(t, "fake-cluster.test", config.SRVHost)

	realAddr := fmt.Sprintf("%s:%d", realConfig.Host, realConfig.Port)
	lookupCalled := false
	config.LookupSRVFunc = func(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
		lookupCalled = true
		assert.Equal(t, "postgresql", service)
		assert.Equal(t, "tcp", proto)
		assert.Equal(t, "fake-cluster.test", name)
		return name, []*net.SRV{
			{Target: realConfig.Host, Port: realConfig.Port, Priority: 0, Weight: 1},
		}, nil
	}

	conn, err := pgconn.ConnectConfig(ctx, config)
	require.NoError(t, err, "SRV connect to %s should succeed", realAddr)
	require.True(t, lookupCalled, "LookupSRVFunc must have been called")
	defer conn.Close(ctx)

	result := conn.ExecParams(ctx, "SELECT 1", nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)
	assert.Equal(t, "1", string(result.Rows[0][0]))
}

// TestConnectSRVMockedMultipleTargets verifies that when the first SRV target
// is unavailable, pgconn falls through to the next one.
func TestConnectSRVMockedMultipleTargets(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("PGX_TEST_TCP_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping: PGX_TEST_TCP_CONN_STRING not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	realConfig, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)

	srvConnString := fmt.Sprintf(
		"postgres+srv://%s:%s@cluster.test/%s?sslmode=disable",
		realConfig.User, realConfig.Password, realConfig.Database,
	)
	config, err := pgconn.ParseConfig(srvConnString)
	require.NoError(t, err)

	// Return two targets: first is a dead port, second is the real server.
	config.LookupSRVFunc = mockSRV(
		"127.0.0.1:1", // dead
		fmt.Sprintf("%s:%d", realConfig.Host, realConfig.Port), // alive
	)

	conn, err := pgconn.ConnectConfig(ctx, config)
	require.NoError(t, err, "should fall through dead target to the live one")
	defer conn.Close(ctx)
}

// TestConnectSRVAllTargetsDead verifies that when all SRV targets are
// unreachable the error is informative.
func TestConnectSRVAllTargetsDead(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config, err := pgconn.ParseConfig("postgres+srv://bob@cluster.test/mydb?sslmode=disable&connect_timeout=1")
	require.NoError(t, err)

	config.LookupSRVFunc = mockSRV("127.0.0.1:1", "127.0.0.1:2")

	_, err = pgconn.ConnectConfig(ctx, config)
	require.Error(t, err)
}

// TestConnectSRVLookupFailure verifies that a DNS lookup error is wrapped and
// propagated.
func TestConnectSRVLookupFailure(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config, err := pgconn.ParseConfig("postgres+srv://bob@cluster.test/mydb?sslmode=disable")
	require.NoError(t, err)

	config.LookupSRVFunc = func(_ context.Context, _, _, name string) (string, []*net.SRV, error) {
		return "", nil, fmt.Errorf("NXDOMAIN: %s not found", name)
	}

	_, err = pgconn.ConnectConfig(ctx, config)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "SRV lookup"), "error should mention SRV lookup, got: %v", err)
}

// TestResolveSRVLive resolves real public SRV records and verifies pgconn
// builds the correct ordered target list — without opening any TCP connections
// to PostgreSQL.
//
// Set PGX_TEST_SRV_HOST to the cluster hostname whose _postgresql._tcp SRV
// records you want to probe, e.g.:
//
//	PGX_TEST_SRV_HOST=mmatvei.ru go test ./pgconn/... -run TestResolveSRVLive -v
//
// Expected DNS records for mmatvei.ru at time of writing:
//
//	_postgresql._tcp.mmatvei.ru SRV 99  1 5432 pg2.mmatvei.ru.
//	_postgresql._tcp.mmatvei.ru SRV 100 1 5432 pg.mmatvei.ru.
func TestResolveSRVLive(t *testing.T) {
	t.Parallel()

	srvHost := os.Getenv("PGX_TEST_SRV_HOST")
	if srvHost == "" {
		srvHost = "mmatvei.ru" // public test records, no Postgres required
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resolver := makeResolver()
	if server := os.Getenv("PGX_TEST_SRV_DNS_SERVER"); server != "" {
		t.Logf("Using custom DNS server: %s", server)
	}

	// Raw lookup so we can log and assert on raw DNS answers.
	_, srvs, err := resolver.LookupSRV(ctx, "postgresql", "tcp", srvHost)
	if err != nil {
		t.Skipf("SRV lookup failed (records may not be published yet): %v", err)
	}

	t.Logf("_postgresql._tcp.%s resolved to %d record(s):", srvHost, len(srvs))
	for i, s := range srvs {
		t.Logf("  [%d] priority=%d weight=%d %s:%d", i, s.Priority, s.Weight, s.Target, s.Port)
	}

	require.NotEmpty(t, srvs, "expected at least one SRV record")

	// Verify RFC 2782 ordering: records must be sorted by priority ascending.
	for i := 1; i < len(srvs); i++ {
		assert.LessOrEqual(t, srvs[i-1].Priority, srvs[i].Priority,
			"SRV records must be in ascending priority order")
	}

	// Now exercise pgconn's own resolution path and verify it produces the
	// same ordering — still without touching any Postgres port.
	config, err := pgconn.ParseConfig(
		fmt.Sprintf("postgres+srv://testuser@%s/testdb?sslmode=disable", srvHost),
	)
	require.NoError(t, err)
	require.Equal(t, srvHost, config.SRVHost)

	// Use the same resolver (possibly pointing at a specific nameserver) so
	// the pgconn resolution path sees the same records as the raw lookup above.
	config.LookupSRVFunc = resolver.LookupSRV

	targets, err := pgconn.BuildConnectOneConfigsFromSRV(ctx, config)
	require.NoError(t, err)
	require.NotEmpty(t, targets)

	t.Logf("pgconn resolved %d connect target(s):", len(targets))
	for i, tgt := range targets {
		t.Logf("  [%d] %s -> %s (hostname: %s)", i, tgt.Network, tgt.Address, tgt.OriginalHostname)
	}

	// The first target must correspond to the lowest-priority SRV record.
	// (sslmode=disable produces one connectOneConfig per SRV entry; prefer
	// would produce two — TLS then plain — for each.)
	lowestPriority := srvs[0].Priority
	firstLowPrioritySRV := srvs[0]
	expectedFirstAddr := fmt.Sprintf("%s:%d",
		strings.TrimSuffix(firstLowPrioritySRV.Target, "."),
		firstLowPrioritySRV.Port,
	)
	assert.Equal(t, expectedFirstAddr, targets[0].Address,
		"first connect target must match the highest-priority (lowest number) SRV record")
	assert.Equal(t, uint16(lowestPriority), firstLowPrioritySRV.Priority)
}

// TestConnectSRVLive runs against a real public DNS SRV record.
// Set PGX_TEST_SRV_CONN_STRING to a postgres+srv:// connection string that
// uses a real SRV record you control, e.g.:
//
//	PGX_TEST_SRV_CONN_STRING="postgres+srv://user:pass@cluster.yourdomain.com/dbname?sslmode=disable"
//
// The test verifies that the SRV lookup resolves and the connection succeeds.
func TestConnectSRVLive(t *testing.T) {
	t.Parallel()

	connString := os.Getenv("PGX_TEST_SRV_CONN_STRING")
	if connString == "" {
		t.Skipf("Skipping: PGX_TEST_SRV_CONN_STRING not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	config, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)
	require.NotEmpty(t, config.SRVHost, "PGX_TEST_SRV_CONN_STRING must use postgres+srv:// scheme")

	t.Logf("Looking up SRV records for _postgresql._tcp.%s", config.SRVHost)

	// Wrap LookupSRVFunc to log what was resolved.
	origLookup := config.LookupSRVFunc
	config.LookupSRVFunc = func(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
		cname, srvs, err := origLookup(ctx, service, proto, name)
		if err == nil {
			for _, s := range srvs {
				t.Logf("  SRV: priority=%d weight=%d %s:%d", s.Priority, s.Weight, s.Target, s.Port)
			}
		}
		return cname, srvs, err
	}

	conn, err := pgconn.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer conn.Close(ctx)

	result := conn.ExecParams(ctx, "SELECT version()", nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)
	t.Logf("Connected to: %s", result.Rows[0][0])
}
