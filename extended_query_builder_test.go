package pgx_test

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

// type SomeObject struct {
// 	Item *Item
// }

type Item struct {
	Name  string
	Value string
}

func TestExtendedQueryBuilder(t *testing.T) {
	t.Parallel()

	var conn *pgx.Conn
	t.Run("connect to database", func(t *testing.T) {
		connString := os.Getenv("PGX_TEST_DATABASE")
		config := mustParseConfig(t, connString)
		config.DefaultQueryExecMode = pgx.QueryExecModeExec
		conn = mustConnect(t, config)
		conn.TypeMap().RegisterDefaultPgType(&Item{}, "jsonb")
	})

	t.Run("create table", func(t *testing.T) {
		sql := `
			CREATE TABLE IF NOT EXISTS some_objects (
				item jsonb
			)
		`
		mustExec(t, conn, sql)
	})

	t.Run("insert data", func(t *testing.T) {
		item := &Item{
			Name:  "test",
			Value: "value",
		}
		sql := `INSERT INTO some_objects (item) VALUES ($1)`
		mustExec(t, conn, sql, item)
	})
}
