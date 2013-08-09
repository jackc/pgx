package migrate

import (
	"fmt"
	"github.com/JackC/pgx"
)

type BadVersionError string

func (e BadVersionError) Error() string {
	return string(e)
}

type Migration struct {
	Sequence int32
	Name     string
	SQL      string
}

type Migrator struct {
	conn         *pgx.Connection
	versionTable string
	Migrations   []*Migration
	OnStart      func(*Migration) `called when Migrate starts a migration`
}

func NewMigrator(conn *pgx.Connection, versionTable string) (m *Migrator, err error) {
	m = &Migrator{conn: conn, versionTable: versionTable}
	err = m.ensureSchemaVersionTableExists()
	m.Migrations = make([]*Migration, 0)
	return
}

func (m *Migrator) AppendMigration(name, sql string) {
	m.Migrations = append(m.Migrations, &Migration{Sequence: int32(len(m.Migrations)), Name: name, SQL: sql})
	return
}

// Migrate runs pending migrations
// It calls m.OnStart when it begins a migration
func (m *Migrator) Migrate() error {
	var done bool

	for !done {
		var innerErr error

		var txErr error
		_, txErr = m.conn.Transaction(func() bool {
			// Lock version table for duration of transaction to ensure multiple migrations cannot occur simultaneously
			if _, innerErr = m.conn.Execute("lock table " + m.versionTable); innerErr != nil {
				return false
			}

			// Get pending migrations
			var pending []*Migration
			if pending, innerErr = m.PendingMigrations(); innerErr != nil {
				return false
			}

			// If no migrations are pending set the done flag and return
			if len(pending) == 0 {
				done = true
				return true
			}

			// Fire on start callback
			if m.OnStart != nil {
				m.OnStart(pending[0])
			}

			// Execute the first pending migration
			if _, innerErr = m.conn.Execute(pending[0].SQL); innerErr != nil {
				return false
			}

			// Add one to the version
			if _, innerErr = m.conn.Execute("update " + m.versionTable + " set version=version+1"); innerErr != nil {
				return false
			}

			// A migration was completed successfully, return true to commit the transaction
			return true
		})

		if txErr != nil {
			return txErr
		}
		if innerErr != nil {
			return innerErr
		}
	}

	return nil
}

func (m *Migrator) PendingMigrations() ([]*Migration, error) {
	if len(m.Migrations) == 0 {
		return m.Migrations, nil
	}

	if current, err := m.GetCurrentVersion(); err == nil {
		current := int(current)
		if current < 0 || len(m.Migrations) < current {
			errMsg := fmt.Sprintf("%s version %d is outside the known migrations of 0 to %d", m.versionTable, current, len(m.Migrations))
			return nil, BadVersionError(errMsg)
		}
		return m.Migrations[current:len(m.Migrations)], nil
	} else {
		return nil, err
	}
}

func (m *Migrator) GetCurrentVersion() (int32, error) {
	if v, err := m.conn.SelectValue("select version from " + m.versionTable); err == nil {
		return v.(int32), nil
	} else {
		return 0, err
	}
}

func (m *Migrator) ensureSchemaVersionTableExists() (err error) {
	_, err = m.conn.Execute(`
    create table if not exists schema_version(version int4 not null);

    insert into schema_version(version)
    select 0
    where 0=(select count(*) from schema_version);
  `)
	return
}
