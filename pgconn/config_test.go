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
		configs    []*pgconn.Config
	}{
		// Test all sslmodes
		{
			name:       "sslmode not set (prefer)",
			connString: "postgres://jack:secret@localhost:5432/mydb",
			configs: []*pgconn.Config{
				&pgconn.Config{
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
				&pgconn.Config{
					User:          "jack",
					Password:      "secret",
					Host:          "localhost",
					Port:          5432,
					Database:      "mydb",
					TLSConfig:     nil,
					RuntimeParams: map[string]string{},
				},
			},
		},
		{
			name:       "sslmode disable",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=disable",
			configs: []*pgconn.Config{
				&pgconn.Config{
					User:          "jack",
					Password:      "secret",
					Host:          "localhost",
					Port:          5432,
					Database:      "mydb",
					TLSConfig:     nil,
					RuntimeParams: map[string]string{},
				},
			},
		},
		{
			name:       "sslmode allow",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=allow",
			configs: []*pgconn.Config{
				&pgconn.Config{
					User:          "jack",
					Password:      "secret",
					Host:          "localhost",
					Port:          5432,
					Database:      "mydb",
					TLSConfig:     nil,
					RuntimeParams: map[string]string{},
				},
				&pgconn.Config{
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
		},
		{
			name:       "sslmode prefer",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=prefer",
			configs: []*pgconn.Config{
				&pgconn.Config{
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
				&pgconn.Config{
					User:          "jack",
					Password:      "secret",
					Host:          "localhost",
					Port:          5432,
					Database:      "mydb",
					TLSConfig:     nil,
					RuntimeParams: map[string]string{},
				},
			},
		},
		{
			name:       "sslmode require",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=require",
			configs: []*pgconn.Config{
				&pgconn.Config{
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
		},
		{
			name:       "sslmode verify-ca",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=verify-ca",
			configs: []*pgconn.Config{
				&pgconn.Config{
					User:          "jack",
					Password:      "secret",
					Host:          "localhost",
					Port:          5432,
					Database:      "mydb",
					TLSConfig:     &tls.Config{ServerName: "localhost"},
					RuntimeParams: map[string]string{},
				},
			},
		},
		{
			name:       "sslmode verify-full",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=verify-full",
			configs: []*pgconn.Config{
				&pgconn.Config{
					User:          "jack",
					Password:      "secret",
					Host:          "localhost",
					Port:          5432,
					Database:      "mydb",
					TLSConfig:     &tls.Config{ServerName: "localhost"},
					RuntimeParams: map[string]string{},
				},
			},
		},
		{
			name:       "database url everything",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=disable&application_name=pgxtest&search_path=myschema",
			configs: []*pgconn.Config{
				&pgconn.Config{
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
		},
		{
			name:       "database url missing password",
			connString: "postgres://jack@localhost:5432/mydb?sslmode=disable",
			configs: []*pgconn.Config{
				&pgconn.Config{
					User:          "jack",
					Host:          "localhost",
					Port:          5432,
					Database:      "mydb",
					TLSConfig:     nil,
					RuntimeParams: map[string]string{},
				},
			},
		},
		{
			name:       "database url missing user and password",
			connString: "postgres://localhost:5432/mydb?sslmode=disable",
			configs: []*pgconn.Config{
				&pgconn.Config{
					User:          osUserName,
					Host:          "localhost",
					Port:          5432,
					Database:      "mydb",
					TLSConfig:     nil,
					RuntimeParams: map[string]string{},
				},
			},
		},
		{
			name:       "database url missing port",
			connString: "postgres://jack:secret@localhost:5432/mydb?sslmode=disable",
			configs: []*pgconn.Config{
				&pgconn.Config{
					User:          "jack",
					Password:      "secret",
					Host:          "localhost",
					Port:          5432,
					Database:      "mydb",
					TLSConfig:     nil,
					RuntimeParams: map[string]string{},
				},
			},
		},
		{
			name:       "database url unix domain socket host",
			connString: "postgres:///foo?host=/tmp",
			configs: []*pgconn.Config{
				&pgconn.Config{
					User:          osUserName,
					Host:          "/tmp",
					Port:          5432,
					Database:      "foo",
					TLSConfig:     nil,
					RuntimeParams: map[string]string{},
				},
			},
		},
		{
			name:       "DSN everything",
			connString: "user=jack password=secret host=localhost port=5432 database=mydb sslmode=disable application_name=pgxtest search_path=myschema",
			configs: []*pgconn.Config{
				&pgconn.Config{
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
		},
	}

	for i, tt := range tests {
		configs, err := pgconn.ParseConfig(tt.connString)
		if !assert.Nilf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}

		assertConfigsEqual(t, tt.configs, configs, fmt.Sprintf("Test %d (%s)", i, tt.name))
	}
}

func assertConfigsEqual(t *testing.T, expected, actual []*pgconn.Config, testName string) {
	if assert.Equalf(t, len(expected), len(actual), "%s", testName) {
		for i := range actual {
			assert.Equalf(t, expected[i].Host, actual[i].Host, "%s - Config %v - Host", testName, i)
			assert.Equalf(t, expected[i].Database, actual[i].Database, "%s - Config %v - Database", testName, i)
			assert.Equalf(t, expected[i].Port, actual[i].Port, "%s - Config %v - Port", testName, i)
			assert.Equalf(t, expected[i].User, actual[i].User, "%s - Config %v - User", testName, i)
			assert.Equalf(t, expected[i].Password, actual[i].Password, "%s - Config %v - Password", testName, i)
			assert.Equalf(t, expected[i].RuntimeParams, actual[i].RuntimeParams, "%s - Config %v - RuntimeParams", testName, i)

			if assert.Equalf(t, expected[i].TLSConfig == nil, actual[i].TLSConfig == nil, "%s - Config %v", testName, i) {
				if expected[i].TLSConfig != nil {
					assert.Equalf(t, expected[i].TLSConfig.InsecureSkipVerify, actual[i].TLSConfig.InsecureSkipVerify, "%s - Config %v", testName, i)
					assert.Equalf(t, expected[i].TLSConfig.ServerName, actual[i].TLSConfig.ServerName, "%s - Config %v", testName, i)
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
		configs []*pgconn.Config
	}{
		{
			name:    "No environment",
			envvars: map[string]string{},
			configs: []*pgconn.Config{
				&pgconn.Config{
					User: osUserName,
					Port: 5432,
					TLSConfig: &tls.Config{
						InsecureSkipVerify: true,
					},
					RuntimeParams: map[string]string{},
				},
				&pgconn.Config{
					User:          osUserName,
					Port:          5432,
					TLSConfig:     nil,
					RuntimeParams: map[string]string{},
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
			configs: []*pgconn.Config{
				&pgconn.Config{
					Host:          "123.123.123.123",
					Port:          7777,
					Database:      "foo",
					User:          "bar",
					Password:      "baz",
					TLSConfig:     nil,
					RuntimeParams: map[string]string{"application_name": "pgxtest"},
				},
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

		configs, err := pgconn.ParseConfig("")
		if !assert.Nilf(t, err, "Test %d (%s)", i, tt.name) {
			continue
		}

		assertConfigsEqual(t, tt.configs, configs, fmt.Sprintf("Test %d (%s)", i, tt.name))
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
	expected := []*pgconn.Config{
		&pgconn.Config{
			User:          "curly",
			Password:      "nyuknyuknyuk",
			Host:          "test1",
			Port:          5432,
			Database:      "curlydb",
			TLSConfig:     nil,
			RuntimeParams: map[string]string{},
		},
	}

	actual, err := pgconn.ParseConfig(connString)
	assert.Nil(t, err)

	assertConfigsEqual(t, expected, actual, "passfile")
}
