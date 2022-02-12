package pgconn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommandTag(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		commandTag   CommandTag
		rowsAffected int64
		isInsert     bool
		isUpdate     bool
		isDelete     bool
		isSelect     bool
	}{
		{commandTag: CommandTag{buf: []byte("INSERT 0 5")}, rowsAffected: 5, isInsert: true},
		{commandTag: CommandTag{buf: []byte("UPDATE 0")}, rowsAffected: 0, isUpdate: true},
		{commandTag: CommandTag{buf: []byte("UPDATE 1")}, rowsAffected: 1, isUpdate: true},
		{commandTag: CommandTag{buf: []byte("DELETE 0")}, rowsAffected: 0, isDelete: true},
		{commandTag: CommandTag{buf: []byte("DELETE 1")}, rowsAffected: 1, isDelete: true},
		{commandTag: CommandTag{buf: []byte("DELETE 1234567890")}, rowsAffected: 1234567890, isDelete: true},
		{commandTag: CommandTag{buf: []byte("SELECT 1")}, rowsAffected: 1, isSelect: true},
		{commandTag: CommandTag{buf: []byte("SELECT 99999999999")}, rowsAffected: 99999999999, isSelect: true},
		{commandTag: CommandTag{buf: []byte("CREATE TABLE")}, rowsAffected: 0},
		{commandTag: CommandTag{buf: []byte("ALTER TABLE")}, rowsAffected: 0},
		{commandTag: CommandTag{buf: []byte("DROP TABLE")}, rowsAffected: 0},
	}

	for i, tt := range tests {
		ct := tt.commandTag
		assert.Equalf(t, tt.rowsAffected, ct.RowsAffected(), "%d. %v", i, tt.commandTag)
		assert.Equalf(t, tt.isInsert, ct.Insert(), "%d. %v", i, tt.commandTag)
		assert.Equalf(t, tt.isUpdate, ct.Update(), "%d. %v", i, tt.commandTag)
		assert.Equalf(t, tt.isDelete, ct.Delete(), "%d. %v", i, tt.commandTag)
		assert.Equalf(t, tt.isSelect, ct.Select(), "%d. %v", i, tt.commandTag)
	}
}
