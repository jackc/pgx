package pgservicefile_test

import (
	"bytes"
	"testing"

	"github.com/jackc/pgx/v5/pgservicefile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseServicefile(t *testing.T) {
	buf := bytes.NewBufferString(`# A comment
[abc]
host=abc.example.com
port=9999
dbname=abcdb
user=abcuser
# Another comment

[def]
host = def.example.com
dbname = defdb
user = defuser
application_name = has space
`)

	servicefile, err := pgservicefile.ParseServicefile(buf)
	require.NoError(t, err)
	require.NotNil(t, servicefile)

	assert.Len(t, servicefile.Services, 2)
	assert.Equal(t, "abc", servicefile.Services[0].Name)
	assert.Equal(t, "def", servicefile.Services[1].Name)

	abc, err := servicefile.GetService("abc")
	require.NoError(t, err)
	assert.Equal(t, servicefile.Services[0], abc)
	assert.Len(t, abc.Settings, 4)
	assert.Equal(t, "abc.example.com", abc.Settings["host"])
	assert.Equal(t, "9999", abc.Settings["port"])
	assert.Equal(t, "abcdb", abc.Settings["dbname"])
	assert.Equal(t, "abcuser", abc.Settings["user"])

	def, err := servicefile.GetService("def")
	require.NoError(t, err)
	assert.Equal(t, servicefile.Services[1], def)
	assert.Len(t, def.Settings, 4)
	assert.Equal(t, "def.example.com", def.Settings["host"])
	assert.Equal(t, "defdb", def.Settings["dbname"])
	assert.Equal(t, "defuser", def.Settings["user"])
	assert.Equal(t, "has space", def.Settings["application_name"])
}

func TestParseServicefileWithInvalidFile(t *testing.T) {
	buf := bytes.NewBufferString("Invalid syntax\n")

	servicefile, err := pgservicefile.ParseServicefile(buf)
	assert.Error(t, err)
	assert.Nil(t, servicefile)
}

// https://github.com/jackc/pgservicefile/issues/5
func TestParseServicefileWithMissedServiceSection(t *testing.T) {
	buf := bytes.NewBufferString("a = b\n")

	servicefile, err := pgservicefile.ParseServicefile(buf)
	assert.Error(t, err)
	assert.Nil(t, servicefile)
}
