package pgconn_test

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"testing"

	"github.com/jackc/pgx/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	t.Parallel()

	var osUserName string
	osUser, err := user.Current()
	if err == nil {
		osUserName = osUser.Username
	}

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
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=disable&application_name=pgxtest&search_path=myschema",
			config: &pgconn.Config{
				User:      "jack",
				Password:  "secret",
				Host:      "localhost",
				Port:      5432,
				Database:  "mydb",
				TLSConfig: nil,
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
			name:       "DSN everything",
			connString: "user=jack password=secret host=localhost port=5432 database=mydb sslmode=disable application_name=pgxtest search_path=myschema",
			config: &pgconn.Config{
				User:      "jack",
				Password:  "secret",
				Host:      "localhost",
				Port:      5432,
				Database:  "mydb",
				TLSConfig: nil,
				RuntimeParams: map[string]string{
					"application_name": "pgxtest",
					"search_path":      "myschema",
				},
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
		{
			name:       "DSN multiple hosts one port",
			connString: "user=jack password=secret host=foo,bar,baz port=5432 database=mydb sslmode=disable",
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
			connString: "user=jack password=secret host=foo,bar,baz port=1,2,3 database=mydb sslmode=disable",
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
			connString: "user=jack password=secret host=foo,bar,baz database=mydb sslmode=prefer",
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
	}

	for i, tt := range tests {
		config, err := pgconn.ParseConfig(tt.connString)
		if !assert.Nilf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}

		assertConfigsEqual(t, tt.config, config, fmt.Sprintf("Test %d (%s)", i, tt.name))
	}
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
	assert.Equalf(t, expected.RuntimeParams, actual.RuntimeParams, "%s - RuntimeParams", testName)

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
		osUserName = osUser.Username
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
				Host:          "123.123.123.123",
				Port:          7777,
				Database:      "foo",
				User:          "bar",
				Password:      "baz",
				TLSConfig:     nil,
				RuntimeParams: map[string]string{"application_name": "pgxtest"},
			},
		},
	}

	for i, tt := range tests {
		for _, n := range pgEnvvars {
			err := os.Unsetenv(n)
			require.Nil(t, err)
		}

		for k, v := range tt.envvars {
			err := os.Setenv(k, v)
			require.Nil(t, err)
		}

		config, err := pgconn.ParseConfig("")
		if !assert.Nilf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}

		assertConfigsEqual(t, tt.config, config, fmt.Sprintf("Test %d (%s)", i, tt.name))
	}
}

func TestParseConfigReadsPgPassfile(t *testing.T) {
	tf, err := ioutil.TempFile("", "")
	require.Nil(t, err)

	defer tf.Close()
	defer os.Remove(tf.Name())

	_, err = tf.Write([]byte("test1:5432:curlydb:curly:nyuknyuknyuk"))
	require.Nil(t, err)

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
	assert.Nil(t, err)

	assertConfigsEqual(t, expected, actual, "passfile")
}
