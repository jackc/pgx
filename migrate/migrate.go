package migrate

import (
	"fmt"
	"github.com/JackC/pgx"
	"io/ioutil"
	"path/filepath"
	"strings"
)

type BadVersionError string

func (e BadVersionError) Error() string {
	return string(e)
}

type IrreversibleMigrationError struct {
	m *Migration
}

func (e IrreversibleMigrationError) Error() string {
	return fmt.Sprintf("Irreversible migration: %d - %s", e.m.Sequence, e.m.Name)
}

type NoMigrationsFoundError struct {
	Path string
}

func (e NoMigrationsFoundError) Error() string {
	return fmt.Sprintf("No migrations found at %s", e.Path)
}

type Migration struct {
	Sequence int32
	Name     string
	UpSQL    string
	DownSQL  string
}

type Migrator struct {
	conn         *pgx.Connection
	versionTable string
	Migrations   []*Migration
	OnStart      func(*Migration, string) // OnStart is called when a migration is run with the migration and direction
}

func NewMigrator(conn *pgx.Connection, versionTable string) (m *Migrator, err error) {
	m = &Migrator{conn: conn, versionTable: versionTable}
	err = m.ensureSchemaVersionTableExists()
	m.Migrations = make([]*Migration, 0)
	return
}

func (m *Migrator) LoadMigrations(path string) error {
	paths, err := filepath.Glob(filepath.Join(path, "*.sql"))
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return NoMigrationsFoundError{Path: path}
	}

	for _, p := range paths {
		body, err := ioutil.ReadFile(p)
		if err != nil {
			return err
		}

		pieces := strings.SplitN(string(body), "---- create above / drop below ----", 2)
		var upSQL, downSQL string
		upSQL = strings.TrimSpace(pieces[0])
		if len(pieces) == 2 {
			downSQL = strings.TrimSpace(pieces[1])
		}
		m.AppendMigration(filepath.Base(p), upSQL, downSQL)
	}

	return nil
}

func (m *Migrator) AppendMigration(name, upSQL, downSQL string) {
	m.Migrations = append(m.Migrations, &Migration{Sequence: int32(len(m.Migrations)) + 1, Name: name, UpSQL: upSQL, DownSQL: downSQL})
	return
}

// Migrate runs pending migrations
// It calls m.OnStart when it begins a migration
func (m *Migrator) Migrate() error {
	return m.MigrateTo(int32(len(m.Migrations)))
}

// MigrateTo migrates to targetVersion
func (m *Migrator) MigrateTo(targetVersion int32) (err error) {
	// Lock to ensure multiple migrations cannot occur simultaneously
	lockNum := int64(9628173550095224) // arbitrary random number
	if _, lockErr := m.conn.Execute("select pg_advisory_lock($1)", lockNum); lockErr != nil {
		return lockErr
	}
	defer func() {
		_, unlockErr := m.conn.Execute("select pg_advisory_unlock($1)", lockNum)
		if err == nil && unlockErr != nil {
			err = unlockErr
		}
	}()

	currentVersion, err := m.GetCurrentVersion()
	if err != nil {
		return err
	}

	if targetVersion < 0 || int32(len(m.Migrations)) < targetVersion {
		errMsg := fmt.Sprintf("%s version %d is outside the valid versions of 0 to %d", m.versionTable, targetVersion, len(m.Migrations))
		return BadVersionError(errMsg)
	}

	var direction int32
	if currentVersion < targetVersion {
		direction = 1
	} else {
		direction = -1
	}

	for currentVersion != targetVersion {
		var current *Migration
		var sql, directionName string
		var sequence int32
		if direction == 1 {
			current = m.Migrations[currentVersion]
			sequence = current.Sequence
			sql = current.UpSQL
			directionName = "up"
		} else {
			current = m.Migrations[currentVersion-1]
			sequence = current.Sequence - 1
			sql = current.DownSQL
			directionName = "down"
			if current.DownSQL == "" {
				return IrreversibleMigrationError{m: current}
			}
		}

		var innerErr error
		_, txErr := m.conn.Transaction(func() bool {

			// Fire on start callback
			if m.OnStart != nil {
				m.OnStart(current, directionName)
			}

			// Execute the migration
			if _, innerErr = m.conn.Execute(sql); innerErr != nil {
				return false
			}

			// Add one to the version
			if _, innerErr = m.conn.Execute("update "+m.versionTable+" set version=$1", sequence); innerErr != nil {
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

		currentVersion = currentVersion + direction
	}

	return nil
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
