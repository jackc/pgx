package tracelog

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogQueryArgsTruncation(t *testing.T) {
	t.Parallel()

	t.Run("short string not truncated", func(t *testing.T) {
		args := logQueryArgs([]any{"hello"})
		assert.Equal(t, "hello", args[0])
	})

	t.Run("long string truncated with ellipsis", func(t *testing.T) {
		s := strings.Repeat("a", 65)
		args := logQueryArgs([]any{s})
		result := args[0].(string)
		// maxCut = len(v)-4 = 61, so output = 61 a's + … (3 bytes) = 64 bytes
		assert.Equal(t, strings.Repeat("a", 61)+"…", result)
		// Result must be shorter than original
		assert.Less(t, len(result), len(s))
	})

	t.Run("short bytes not truncated", func(t *testing.T) {
		b := []byte{0x01, 0x02, 0x03}
		args := logQueryArgs([]any{b})
		assert.Equal(t, "010203", args[0])
	})

	t.Run("long bytes truncated with ellipsis", func(t *testing.T) {
		b := make([]byte, 65)
		for i := range b {
			b[i] = byte(i)
		}
		args := logQueryArgs([]any{b})
		result := args[0].(string)
		// Should be 120 hex chars (60 bytes) + "…", always shorter than full hex
		assert.True(t, strings.HasSuffix(result, "…"), "truncated bytes should end with …")
		assert.Less(t, len(result), len(hex.EncodeToString(b)), "truncated result should be shorter than full hex")
	})

	t.Run("UTF-8 string truncated at rune boundary", func(t *testing.T) {
		// 63 ASCII chars + 1 multi-byte rune (4 bytes) = 67 bytes, should not truncate
		s := strings.Repeat("x", 63) + "😊"
		args := logQueryArgs([]any{s})
		assert.Equal(t, s, args[0])

		// 67 bytes + more = should truncate at rune boundary, result shorter than original
		s2 := s + "aaa" // 70 bytes
		args2 := logQueryArgs([]any{s2})
		result := args2[0].(string)
		assert.True(t, strings.HasSuffix(result, "…"), "truncated string should end with …")
		assert.Less(t, len(result), len(s2), "truncated result should be shorter than original")
	})
}