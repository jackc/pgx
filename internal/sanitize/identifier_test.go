package sanitize_test

import (
	"testing"

	"github.com/jackc/pgx/v5/internal/sanitize"
	"github.com/stretchr/testify/require"
)

func TestSanitizeUnquotedDollarIdentifier(t *testing.T) {
	for _, name := range []string{"value$1", "_value$2", "café$1", "value$tag$"} {
		t.Run(name, func(t *testing.T) {
			query, err := sanitize.NewQuery("select " + name + " from data where id=$1")
			require.NoError(t, err)
			sql, err := query.Sanitize(int64(7))
			require.NoError(t, err)
			require.Equal(t, "select "+name+" from data where id= 7 ", sql)
		})
	}
}
