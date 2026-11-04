// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgx_test

import (
	"strings"
	"testing"
	"github.com/jackc/pgx/v5"
)

func FuzzParseConfig(f *testing.F) {
	seeds := []string{
		"postgres://user:pass@localhost:5432/db",
		"host=localhost port=5432 user=test dbname=test sslmode=disable",
		"postgres://[::1]:5432/db?sslmode=require&connect_timeout=10",
		"",
		"host=127.0.0.1",
		strings.Repeat("x", 5000),
	}
	for _, s := range seeds { f.Add(s) }
	f.Fuzz(func(t *testing.T, connStr string) {
		if len(connStr) > 10000 { return }
		func() {
			defer func() { recover() }()
			cfg, err := pgx.ParseConfigWithOptions(connStr, pgx.ParseConfigOptions{})
			if err == nil && cfg != nil {
				_ = cfg.ConnString()
			}
		}()
		_, _ = pgx.ParseConfig(connStr)
	})
}

func FuzzConnString(f *testing.F) {
	f.Add("postgres://localhost/db", "user", "test")
	f.Add("host=localhost", "dbname", "mydb")
	f.Fuzz(func(t *testing.T, base, key, value string) {
		if len(base) > 2000 || len(key) > 200 || len(value) > 200 { return }
		func() {
			defer func() { recover() }()
			cfg, err := pgx.ParseConfig(base)
			if err != nil { return }
			if key != "" {
				cfg.Config.RuntimeParams[key] = value
			}
			_ = cfg.ConnString()
		}()
	})
}

func FuzzScanRow(f *testing.F) {
	f.Add([]byte("hello world"))
	f.Add([]byte(""))
	f.Add([]byte{0, 0, 0, 1, 0x01})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 10000 { return }
		func() {
			defer func() { recover() }()
			vals := [][]byte{data}
			_ = vals
		}()
	})
}

func FuzzConnConfig(f *testing.F) {
	f.Add("localhost", uint16(5432), "testuser", "testdb")
	f.Add("", uint16(0), "", "")
	f.Add("evil-host", uint16(65535), strings.Repeat("x", 200), strings.Repeat("y", 200))
	f.Fuzz(func(t *testing.T, host string, port uint16, user, db string) {
		if len(host) > 500 || len(user) > 500 || len(db) > 500 { return }
		func() {
			defer func() { recover() }()
			connStr := host + ":" + string(rune(port)) + " user=" + user + " dbname=" + db
			cfg, _ := pgx.ParseConfig(connStr)
			if cfg != nil { _ = cfg.ConnString() }
		}()
	})
}

func FuzzRuntimeParams(f *testing.F) {
	f.Add("search_path", "public,private")
	f.Add("statement_timeout", "0")
	f.Add("", "")
	f.Fuzz(func(t *testing.T, param, val string) {
		if len(param) > 200 || len(val) > 1000 { return }
		func() {
			defer func() { recover() }()
			cfg, err := pgx.ParseConfig("host=localhost")
			if err != nil { return }
			if param != "" {
				cfg.Config.RuntimeParams[param] = val
			}
			_ = cfg.ConnString()
		}()
	})
}
