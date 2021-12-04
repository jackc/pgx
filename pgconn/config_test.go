package pgconn_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	t.Parallel()

	var osUserName string
	osUser, err := user.Current()
	if err == nil {
		// Windows gives us the username here as `DOMAIN\user` or `LOCALPCNAME\user`,
		// but the libpq default is just the `user` portion, so we strip off the first part.
		if runtime.GOOS == "windows" && strings.Contains(osUser.Username, "\\") {
			osUserName = osUser.Username[strings.LastIndex(osUser.Username, "\\")+1:]
		} else {
			osUserName = osUser.Username
		}
	}

	config, err := pgconn.ParseConfig("")
	require.NoError(t, err)
	defaultHost := config.Host

	tests := []struct {
		name       string
		connString string
		config     *pgconn.Config
	}{
		// Test all sslmodes
		{
			name:       "sslmode not set (prefer)",
			connString: "postgres://jack:secret@localhost:5432/mydb",
			config: &pgconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
				Fallbacks: []*pgconn.FallbackConfig{
					&pgconn.FallbackConfig{
						Host:      "localhost",
						Port:      5432,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "sslmode disable",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "sslmode allow",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=allow",
			config: &pgconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
				Fallbacks: []*pgconn.FallbackConfig{
					&pgconn.FallbackConfig{
						Host: "localhost",
						Port: 5432,
						TLSConfig: &tls.Config{
							InsecureSkipVerify: true,
						},
					},
				},
			},
		},
		{
			name:       "sslmode prefer",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=prefer",
			config: &pgconn.Config{

				User:     "jack",
				Password: "secret",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
				Fallbacks: []*pgconn.FallbackConfig{
					&pgconn.FallbackConfig{
						Host:      "localhost",
						Port:      5432,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "sslmode require",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=require",
			config: &pgconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "sslmode verify-ca",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=verify-ca",
			config: &pgconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "localhost",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "sslmode verify-full",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=verify-full",
			config: &pgconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     &tls.Config{ServerName: "localhost"},
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url everything",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=disable&application_name=pgxtest&search_path=myschema&connect_timeout=5",
			config: &pgconn.Config{
				User:           "jack",
				Password:       "secret",
				Host:           "localhost",
				Port:           5432,
				Database:       "mydb",
				TLSConfig:      nil,
				ConnectTimeout: 5 * time.Second,
				RuntimeParams: map[string]string{
					"application_name": "pgxtest",
					"search_path":      "myschema",
				},
			},
		},
		{
			name:       "database url missing password",
			connString: "postgres://jack@localhost:5432/mydb?sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url missing user and password",
			connString: "postgres://localhost:5432/mydb?sslmode=disable",
			config: &pgconn.Config{
				User:          osUserName,
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url missing port",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url unix domain socket host",
			connString: "postgres:///foo?host=/tmp",
			config: &pgconn.Config{
				User:          osUserName,
				Host:          "/tmp",
				Port:          5432,
				Database:      "foo",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url dbname",
			connString: "postgres://localhost/?dbname=foo&sslmode=disable",
			config: &pgconn.Config{
				User:          osUserName,
				Host:          "localhost",
				Port:          5432,
				Database:      "foo",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url postgresql protocol",
			connString: "postgresql://jack@localhost:5432/mydb?sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url IPv4 with port",
			connString: "postgresql://jack@127.0.0.1:5433/mydb?sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Host:          "127.0.0.1",
				Port:          5433,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url IPv6 with port",
			connString: "postgresql://jack@[2001:db8::1]:5433/mydb?sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Host:          "2001:db8::1",
				Port:          5433,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "database url IPv6 no port",
			connString: "postgresql://jack@[2001:db8::1]/mydb?sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Host:          "2001:db8::1",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "DSN everything",
			connString: "user=jack password=secret host=localhost port=5432 dbname=mydb sslmode=disable application_name=pgxtest search_path=myschema connect_timeout=5",
			config: &pgconn.Config{
				User:           "jack",
				Password:       "secret",
				Host:           "localhost",
				Port:           5432,
				Database:       "mydb",
				TLSConfig:      nil,
				ConnectTimeout: 5 * time.Second,
				RuntimeParams: map[string]string{
					"application_name": "pgxtest",
					"search_path":      "myschema",
				},
			},
		},
		{
			name:       "DSN with escaped single quote",
			connString: "user=jack\\'s password=secret host=localhost port=5432 dbname=mydb sslmode=disable",
			config: &pgconn.Config{
				User:          "jack's",
				Password:      "secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "DSN with escaped backslash",
			connString: "user=jack password=sooper\\\\secret host=localhost port=5432 dbname=mydb sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Password:      "sooper\\secret",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "DSN with single quoted values",
			connString: "user='jack' host='localhost' dbname='mydb' sslmode='disable'",
			config: &pgconn.Config{
				User:          "jack",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "DSN with single quoted value with escaped single quote",
			connString: "user='jack\\'s' host='localhost' dbname='mydb' sslmode='disable'",
			config: &pgconn.Config{
				User:          "jack's",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "DSN with empty single quoted value",
			connString: "user='jack' password='' host='localhost' dbname='mydb' sslmode='disable'",
			config: &pgconn.Config{
				User:          "jack",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "DSN with space between key and value",
			connString: "user = 'jack' password = '' host = 'localhost' dbname = 'mydb' sslmode='disable'",
			config: &pgconn.Config{
				User:          "jack",
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "URL multiple hosts",
			connString: "postgres://jack:secret@foo,bar,baz/mydb?sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "foo",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
				Fallbacks: []*pgconn.FallbackConfig{
					&pgconn.FallbackConfig{
						Host:      "bar",
						Port:      5432,
						TLSConfig: nil,
					},
					&pgconn.FallbackConfig{
						Host:      "baz",
						Port:      5432,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "URL multiple hosts and ports",
			connString: "postgres://jack:secret@foo:1,bar:2,baz:3/mydb?sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "foo",
				Port:          1,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
				Fallbacks: []*pgconn.FallbackConfig{
					&pgconn.FallbackConfig{
						Host:      "bar",
						Port:      2,
						TLSConfig: nil,
					},
					&pgconn.FallbackConfig{
						Host:      "baz",
						Port:      3,
						TLSConfig: nil,
					},
				},
			},
		},
		// https://github.com/jackc/pgconn/issues/72
		{
			name:       "URL without host but with port still uses default host",
			connString: "postgres://jack:secret@:1/mydb?sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          defaultHost,
				Port:          1,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
		{
			name:       "DSN multiple hosts one port",
			connString: "user=jack password=secret host=foo,bar,baz port=5432 dbname=mydb sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "foo",
				Port:          5432,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
				Fallbacks: []*pgconn.FallbackConfig{
					&pgconn.FallbackConfig{
						Host:      "bar",
						Port:      5432,
						TLSConfig: nil,
					},
					&pgconn.FallbackConfig{
						Host:      "baz",
						Port:      5432,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "DSN multiple hosts multiple ports",
			connString: "user=jack password=secret host=foo,bar,baz port=1,2,3 dbname=mydb sslmode=disable",
			config: &pgconn.Config{
				User:          "jack",
				Password:      "secret",
				Host:          "foo",
				Port:          1,
				Database:      "mydb",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
				Fallbacks: []*pgconn.FallbackConfig{
					&pgconn.FallbackConfig{
						Host:      "bar",
						Port:      2,
						TLSConfig: nil,
					},
					&pgconn.FallbackConfig{
						Host:      "baz",
						Port:      3,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "multiple hosts and fallback tsl",
			connString: "user=jack password=secret host=foo,bar,baz dbname=mydb sslmode=prefer",
			config: &pgconn.Config{
				User:     "jack",
				Password: "secret",
				Host:     "foo",
				Port:     5432,
				Database: "mydb",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
				Fallbacks: []*pgconn.FallbackConfig{
					&pgconn.FallbackConfig{
						Host:      "foo",
						Port:      5432,
						TLSConfig: nil,
					},
					&pgconn.FallbackConfig{
						Host: "bar",
						Port: 5432,
						TLSConfig: &tls.Config{
							InsecureSkipVerify: true,
						}},
					&pgconn.FallbackConfig{
						Host:      "bar",
						Port:      5432,
						TLSConfig: nil,
					},
					&pgconn.FallbackConfig{
						Host: "baz",
						Port: 5432,
						TLSConfig: &tls.Config{
							InsecureSkipVerify: true,
						}},
					&pgconn.FallbackConfig{
						Host:      "baz",
						Port:      5432,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "target_session_attrs",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=disable&target_session_attrs=read-write",
			config: &pgconn.Config{
				User:            "jack",
				Password:        "secret",
				Host:            "localhost",
				Port:            5432,
				Database:        "mydb",
				TLSConfig:       nil,
				RuntimeParams:   map[string]string{},
				ValidateConnect: pgconn.ValidateConnectTargetSessionAttrsReadWrite,
			},
		},
	}

	for i, tt := range tests {
		config, err := pgconn.ParseConfig(tt.connString)
		if !assert.Nilf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}

		assertConfigsEqual(t, tt.config, config, fmt.Sprintf("Test %d (%s)", i, tt.name))
	}
}

// https://github.com/jackc/pgconn/issues/47
func TestParseConfigDSNWithTrailingEmptyEqualDoesNotPanic(t *testing.T) {
	_, err := pgconn.ParseConfig("host= user= password= port= database=")
	require.NoError(t, err)
}

func TestParseConfigDSNLeadingEqual(t *testing.T) {
	_, err := pgconn.ParseConfig("= user=jack")
	require.Error(t, err)
}

// https://github.com/jackc/pgconn/issues/49
func TestParseConfigDSNTrailingBackslash(t *testing.T) {
	_, err := pgconn.ParseConfig(`x=x\`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid backslash")
}

func TestConfigCopyReturnsEqualConfig(t *testing.T) {
	connString := "postgres://jack:secret@localhost:5432/mydb?application_name=pgxtest&search_path=myschema&connect_timeout=5"
	original, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()
	assertConfigsEqual(t, original, copied, "Test Config.Copy() returns equal config")
}

func TestConfigCopyOriginalConfigDidNotChange(t *testing.T) {
	connString := "postgres://jack:secret@localhost:5432/mydb?application_name=pgxtest&search_path=myschema&connect_timeout=5&sslmode=prefer"
	original, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()
	assertConfigsEqual(t, original, copied, "Test Config.Copy() returns equal config")

	copied.Port = uint16(5433)
	copied.RuntimeParams["foo"] = "bar"
	copied.Fallbacks[0].Port = uint16(5433)

	assert.Equal(t, uint16(5432), original.Port)
	assert.Equal(t, "", original.RuntimeParams["foo"])
	assert.Equal(t, uint16(5432), original.Fallbacks[0].Port)
}

func TestConfigCopyCanBeUsedToConnect(t *testing.T) {
	connString := os.Getenv("PGX_TEST_CONN_STRING")
	original, err := pgconn.ParseConfig(connString)
	require.NoError(t, err)

	copied := original.Copy()
	assert.NotPanics(t, func() {
		_, err = pgconn.ConnectConfig(context.Background(), copied)
	})
	assert.NoError(t, err)
}

func assertConfigsEqual(t *testing.T, expected, actual *pgconn.Config, testName string) {
	if !assert.NotNil(t, expected) {
		return
	}
	if !assert.NotNil(t, actual) {
		return
	}

	assert.Equalf(t, expected.Host, actual.Host, "%s - Host", testName)
	assert.Equalf(t, expected.Database, actual.Database, "%s - Database", testName)
	assert.Equalf(t, expected.Port, actual.Port, "%s - Port", testName)
	assert.Equalf(t, expected.User, actual.User, "%s - User", testName)
	assert.Equalf(t, expected.Password, actual.Password, "%s - Password", testName)
	assert.Equalf(t, expected.ConnectTimeout, actual.ConnectTimeout, "%s - ConnectTimeout", testName)
	assert.Equalf(t, expected.RuntimeParams, actual.RuntimeParams, "%s - RuntimeParams", testName)

	// Can't test function equality, so just test that they are set or not.
	assert.Equalf(t, expected.ValidateConnect == nil, actual.ValidateConnect == nil, "%s - ValidateConnect", testName)
	assert.Equalf(t, expected.AfterConnect == nil, actual.AfterConnect == nil, "%s - AfterConnect", testName)

	if assert.Equalf(t, expected.TLSConfig == nil, actual.TLSConfig == nil, "%s - TLSConfig", testName) {
		if expected.TLSConfig != nil {
			assert.Equalf(t, expected.TLSConfig.InsecureSkipVerify, actual.TLSConfig.InsecureSkipVerify, "%s - TLSConfig InsecureSkipVerify", testName)
			assert.Equalf(t, expected.TLSConfig.ServerName, actual.TLSConfig.ServerName, "%s - TLSConfig ServerName", testName)
		}
	}

	if assert.Equalf(t, len(expected.Fallbacks), len(actual.Fallbacks), "%s - Fallbacks", testName) {
		for i := range expected.Fallbacks {
			assert.Equalf(t, expected.Fallbacks[i].Host, actual.Fallbacks[i].Host, "%s - Fallback %d - Host", testName, i)
			assert.Equalf(t, expected.Fallbacks[i].Port, actual.Fallbacks[i].Port, "%s - Fallback %d - Port", testName, i)

			if assert.Equalf(t, expected.Fallbacks[i].TLSConfig == nil, actual.Fallbacks[i].TLSConfig == nil, "%s - Fallback %d - TLSConfig", testName, i) {
				if expected.Fallbacks[i].TLSConfig != nil {
					assert.Equalf(t, expected.Fallbacks[i].TLSConfig.InsecureSkipVerify, actual.Fallbacks[i].TLSConfig.InsecureSkipVerify, "%s - Fallback %d - TLSConfig InsecureSkipVerify", testName)
					assert.Equalf(t, expected.Fallbacks[i].TLSConfig.ServerName, actual.Fallbacks[i].TLSConfig.ServerName, "%s - Fallback %d - TLSConfig ServerName", testName)
				}
			}
		}
	}
}

func TestParseConfigEnvLibpq(t *testing.T) {
	var osUserName string
	osUser, err := user.Current()
	if err == nil {
		// Windows gives us the username here as `DOMAIN\user` or `LOCALPCNAME\user`,
		// but the libpq default is just the `user` portion, so we strip off the first part.
		if runtime.GOOS == "windows" && strings.Contains(osUser.Username, "\\") {
			osUserName = osUser.Username[strings.LastIndex(osUser.Username, "\\")+1:]
		} else {
			osUserName = osUser.Username
		}
	}

	pgEnvvars := []string{"PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD", "PGAPPNAME", "PGSSLMODE", "PGCONNECT_TIMEOUT"}

	savedEnv := make(map[string]string)
	for _, n := range pgEnvvars {
		savedEnv[n] = os.Getenv(n)
	}
	defer func() {
		for k, v := range savedEnv {
			err := os.Setenv(k, v)
			if err != nil {
				t.Fatalf("Unable to restore environment: %v", err)
			}
		}
	}()

	tests := []struct {
		name    string
		envvars map[string]string
		config  *pgconn.Config
	}{
		{
			// not testing no environment at all as that would use default host and that can vary.
			name:    "PGHOST only",
			envvars: map[string]string{"PGHOST": "123.123.123.123"},
			config: &pgconn.Config{
				User: osUserName,
				Host: "123.123.123.123",
				Port: 5432,
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
				Fallbacks: []*pgconn.FallbackConfig{
					&pgconn.FallbackConfig{
						Host:      "123.123.123.123",
						Port:      5432,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name: "All non-TLS environment",
			envvars: map[string]string{
				"PGHOST":            "123.123.123.123",
				"PGPORT":            "7777",
				"PGDATABASE":        "foo",
				"PGUSER":            "bar",
				"PGPASSWORD":        "baz",
				"PGCONNECT_TIMEOUT": "10",
				"PGSSLMODE":         "disable",
				"PGAPPNAME":         "pgxtest",
			},
			config: &pgconn.Config{
				Host:           "123.123.123.123",
				Port:           7777,
				Database:       "foo",
				User:           "bar",
				Password:       "baz",
				ConnectTimeout: 10 * time.Second,
				TLSConfig:      nil,
				RuntimeParams:  map[string]string{"application_name": "pgxtest"},
			},
		},
	}

	for i, tt := range tests {
		for _, n := range pgEnvvars {
			err := os.Unsetenv(n)
			require.NoError(t, err)
		}

		for k, v := range tt.envvars {
			err := os.Setenv(k, v)
			require.NoError(t, err)
		}

		config, err := pgconn.ParseConfig("")
		if !assert.Nilf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}

		assertConfigsEqual(t, tt.config, config, fmt.Sprintf("Test %d (%s)", i, tt.name))
	}
}

func TestParseConfigReadsPgPassfile(t *testing.T) {
	t.Parallel()

	tf, err := ioutil.TempFile("", "")
	require.NoError(t, err)

	defer tf.Close()
	defer os.Remove(tf.Name())

	_, err = tf.Write([]byte("test1:5432:curlydb:curly:nyuknyuknyuk"))
	require.NoError(t, err)

	connString := fmt.Sprintf("postgres://curly@test1:5432/curlydb?sslmode=disable&passfile=%s", tf.Name())
	expected := &pgconn.Config{
		User:          "curly",
		Password:      "nyuknyuknyuk",
		Host:          "test1",
		Port:          5432,
		Database:      "curlydb",
		TLSConfig:     nil,
		RuntimeParams: map[string]string{},
	}

	actual, err := pgconn.ParseConfig(connString)
	assert.NoError(t, err)

	assertConfigsEqual(t, expected, actual, "passfile")
}

func TestParseConfigReadsPgServiceFile(t *testing.T) {
	t.Parallel()

	tf, err := ioutil.TempFile("", "")
	require.NoError(t, err)

	defer tf.Close()
	defer os.Remove(tf.Name())

	_, err = tf.Write([]byte(`
[abc]
host=abc.example.com
port=9999
dbname=abcdb
user=abcuser

[def]
host = def.example.com
dbname = defdb
user = defuser
application_name = spaced string
`))
	require.NoError(t, err)

	tests := []struct {
		name       string
		connString string
		config     *pgconn.Config
	}{
		{
			name:       "abc",
			connString: fmt.Sprintf("postgres:///?servicefile=%s&service=%s", tf.Name(), "abc"),
			config: &pgconn.Config{
				Host:     "abc.example.com",
				Database: "abcdb",
				User:     "abcuser",
				Port:     9999,
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{},
				Fallbacks: []*pgconn.FallbackConfig{
					&pgconn.FallbackConfig{
						Host:      "abc.example.com",
						Port:      9999,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "def",
			connString: fmt.Sprintf("postgres:///?servicefile=%s&service=%s", tf.Name(), "def"),
			config: &pgconn.Config{
				Host:     "def.example.com",
				Port:     5432,
				Database: "defdb",
				User:     "defuser",
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
				RuntimeParams: map[string]string{"application_name": "spaced string"},
				Fallbacks: []*pgconn.FallbackConfig{
					&pgconn.FallbackConfig{
						Host:      "def.example.com",
						Port:      5432,
						TLSConfig: nil,
					},
				},
			},
		},
		{
			name:       "conn string has precedence",
			connString: fmt.Sprintf("postgres://other.example.com:7777/?servicefile=%s&service=%s&sslmode=disable", tf.Name(), "abc"),
			config: &pgconn.Config{
				Host:          "other.example.com",
				Database:      "abcdb",
				User:          "abcuser",
				Port:          7777,
				TLSConfig:     nil,
				RuntimeParams: map[string]string{},
			},
		},
	}

	for i, tt := range tests {
		config, err := pgconn.ParseConfig(tt.connString)
		if !assert.NoErrorf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}

		assertConfigsEqual(t, tt.config, config, fmt.Sprintf("Test %d (%s)", i, tt.name))
	}
}

func TestParseConfigExtractsMinReadBufferSize(t *testing.T) {
	t.Parallel()

	config, err := pgconn.ParseConfig("min_read_buffer_size=0")
	require.NoError(t, err)
	_, present := config.RuntimeParams["min_read_buffer_size"]
	require.False(t, present)

	// The buffer size is internal so there isn't much that can be done to test it other than see that the runtime param
	// was removed.
}
