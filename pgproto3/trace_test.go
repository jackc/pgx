package pgproto3_test

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTrace(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGX_TEST_DATABASE"))
	require.NoError(t, err)
	defer conn.Close(ctx)

	if conn.ParameterStatus("crdb_version") != "" {
		t.Skip("Skipping message trace on CockroachDB as it varies slightly from PostgreSQL")
	}

	traceOutput := &bytes.Buffer{}
	conn.Frontend().Trace(traceOutput, pgproto3.TracerOptions{
		SuppressTimestamps: true,
		RegressMode:        true,
	})

	result := conn.ExecParams(ctx, "select n from generate_series(1,5) n", nil, nil, nil, nil).Read()
	require.NoError(t, result.Err)

	expected := `F	Parse	45	 "" "select n from generate_series(1,5) n" 0
F	Bind	13	 "" "" 0 0 0
F	Describe	7	 P ""
F	Execute	10	 "" 0
F	Sync	5
B	ParseComplete	5
B	BindComplete	5
B	RowDescription	27	 1 "n" 0 0 23 4 -1 0
B	DataRow	12	 1 1 '1'
B	DataRow	12	 1 1 '2'
B	DataRow	12	 1 1 '3'
B	DataRow	12	 1 1 '4'
B	DataRow	12	 1 1 '5'
B	CommandComplete	14	 "SELECT 5"
B	ReadyForQuery	6	 I
`

	require.Equal(t, expected, traceOutput.String())
}

func TestTracerOptionsParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		line               string
		suppressTimestamps bool
		wantTimestamp      time.Time
		wantActor          byte
		wantMsgType        string
		wantSize           int32
		wantArgs           string
		wantErr            bool
	}{
		{
			name:               "with timestamp and args",
			line:               "2024-01-15 10:30:45.123456\tB\tParameterStatus\t25\t\"server_version\" \"15.4\"",
			suppressTimestamps: false,
			wantTimestamp:      time.Date(2024, 1, 15, 10, 30, 45, 123456000, time.UTC),
			wantActor:          'B',
			wantMsgType:        "ParameterStatus",
			wantSize:           25,
			wantArgs:           "\"server_version\" \"15.4\"",
		},
		{
			name:               "with timestamp no args",
			line:               "2024-01-15 10:30:45.123456\tF\tSync\t5",
			suppressTimestamps: false,
			wantTimestamp:      time.Date(2024, 1, 15, 10, 30, 45, 123456000, time.UTC),
			wantActor:          'F',
			wantMsgType:        "Sync",
			wantSize:           5,
			wantArgs:           "",
		},
		{
			name:               "suppress timestamps with args",
			line:               "B\tDataRow\t12\t 1 1 '1'",
			suppressTimestamps: true,
			wantActor:          'B',
			wantMsgType:        "DataRow",
			wantSize:           12,
			wantArgs:           " 1 1 '1'",
		},
		{
			name:               "suppress timestamps no args",
			line:               "F\tSync\t5",
			suppressTimestamps: true,
			wantActor:          'F',
			wantMsgType:        "Sync",
			wantSize:           5,
			wantArgs:           "",
		},
		{
			name:               "invalid actor",
			line:               "X\tSync\t5",
			suppressTimestamps: true,
			wantErr:            true,
		},
		{
			name:               "invalid actor multi-char",
			line:               "FB\tSync\t5",
			suppressTimestamps: true,
			wantErr:            true,
		},
		{
			name:               "invalid timestamp",
			line:               "not-a-timestamp\tB\tSync\t5",
			suppressTimestamps: false,
			wantErr:            true,
		},
		{
			name:               "not enough fields",
			line:               "B\tSync",
			suppressTimestamps: true,
			wantErr:            true,
		},
		{
			name:               "invalid size",
			line:               "B\tSync\tnotanumber",
			suppressTimestamps: true,
			wantErr:            true,
		},
		{
			name:               "args with tabs",
			line:               "B\tTest\t10\tfirst\tsecond",
			suppressTimestamps: true,
			wantActor:          'B',
			wantMsgType:        "Test",
			wantSize:           10,
			wantArgs:           "first\tsecond",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := pgproto3.TracerOptions{SuppressTimestamps: tt.suppressTimestamps}
			timestamp, actor, msgType, size, args, err := opts.Parse([]byte(tt.line))

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantTimestamp, timestamp)
			assert.Equal(t, tt.wantActor, actor)
			assert.Equal(t, tt.wantMsgType, msgType)
			assert.Equal(t, tt.wantSize, size)
			assert.Equal(t, tt.wantArgs, string(args))
		})
	}
}

func TestTracerOptionsParseArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     string
		want     []string
		wantErrs []bool // true if that iteration should yield an error
	}{
		{
			name: "double quoted strings",
			args: `"hello" "world"`,
			want: []string{"hello", "world"},
		},
		{
			name: "single quoted strings",
			args: `'hello' 'world'`,
			want: []string{"hello", "world"},
		},
		{
			name: "unquoted values",
			args: `123 456 abc`,
			want: []string{"123", "456", "abc"},
		},
		{
			name: "mixed types",
			args: `"name" 42 'value'`,
			want: []string{"name", "42", "value"},
		},
		{
			name: "single quoted with hex escape",
			args: `'hello\x0aworld'`,
			want: []string{"hello\nworld"},
		},
		{
			name: "single quoted with multiple escapes",
			args: `'\x00\x01\x02'`,
			want: []string{"\x00\x01\x02"},
		},
		{
			name: "single quoted with tab escape",
			args: `'col1\x09col2'`,
			want: []string{"col1\tcol2"},
		},
		{
			name: "leading space in args",
			args: ` "first" "second"`,
			want: []string{"first", "second"},
		},
		{
			name: "multiple spaces between args",
			args: `"one"   "two"    "three"`,
			want: []string{"one", "two", "three"},
		},
		{
			name: "empty args",
			args: ``,
			want: nil,
		},
		{
			name: "only spaces",
			args: `   `,
			want: nil,
		},
		{
			name: "empty double quoted",
			args: `""`,
			want: []string{""},
		},
		{
			name: "empty single quoted",
			args: `''`,
			want: []string{""},
		},
		{
			name:     "unclosed double quote",
			args:     `"hello`,
			wantErrs: []bool{true},
		},
		{
			name:     "unclosed single quote",
			args:     `'hello`,
			wantErrs: []bool{true},
		},
		{
			name:     "invalid hex escape",
			args:     `'hello\xgg'`,
			wantErrs: []bool{true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := pgproto3.TracerOptions{}
			var got []string
			var gotErrs []bool

			for val, err := range opts.ParseArgs([]byte(tt.args)) {
				if err != nil {
					gotErrs = append(gotErrs, true)
				} else {
					got = append(got, string(val))
					gotErrs = append(gotErrs, false)
				}
			}

			if tt.wantErrs != nil {
				assert.Equal(t, tt.wantErrs, gotErrs)
			} else {
				assert.Equal(t, tt.want, got)
				for _, gotErr := range gotErrs {
					assert.False(t, gotErr)
				}
			}
		})
	}
}

func TestParseRoundTrip(t *testing.T) {
	t.Parallel()

	// Test that we can parse the output from the tracer
	traceOutput := `F	Parse	45	 "" "select n from generate_series(1,5) n" 0
F	Bind	13	 "" "" 0 0 0
F	Describe	7	 P ""
F	Execute	10	 "" 0
F	Sync	5
B	ParseComplete	5
B	BindComplete	5
B	RowDescription	27	 1 "n" 0 0 23 4 -1 0
B	DataRow	12	 1 1 '1'
B	DataRow	12	 1 1 '2'
B	CommandComplete	14	 "SELECT 5"
B	ReadyForQuery	6	 I
`

	opts := pgproto3.TracerOptions{SuppressTimestamps: true}
	lines := bytes.Split([]byte(traceOutput), []byte{'\n'})

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		_, actor, msgType, size, _, err := opts.Parse(line)
		require.NoError(t, err)
		assert.True(t, actor == 'F' || actor == 'B')
		assert.NotEmpty(t, msgType)
		assert.Greater(t, size, int32(0))
	}
}

func BenchmarkParse(b *testing.B) {
	benchmarks := []struct {
		name               string
		line               []byte
		suppressTimestamps bool
	}{
		{
			name:               "with timestamp and args",
			line:               []byte("2024-01-15 10:30:45.123456\tB\tParameterStatus\t25\t\"server_version\" \"15.4\""),
			suppressTimestamps: false,
		},
		{
			name:               "with timestamp no args",
			line:               []byte("2024-01-15 10:30:45.123456\tF\tSync\t5"),
			suppressTimestamps: false,
		},
		{
			name:               "suppress timestamps with args",
			line:               []byte("B\tDataRow\t12\t 1 1 '1'"),
			suppressTimestamps: true,
		},
		{
			name:               "suppress timestamps no args",
			line:               []byte("F\tSync\t5"),
			suppressTimestamps: true,
		},
		{
			name:               "row description",
			line:               []byte("B\tRowDescription\t27\t 1 \"n\" 0 0 23 4 -1 0"),
			suppressTimestamps: true,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			opts := pgproto3.TracerOptions{SuppressTimestamps: bm.suppressTimestamps}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _, _, _, _ = opts.Parse(bm.line)
			}
		})
	}
}

func BenchmarkParseArgs(b *testing.B) {
	benchmarks := []struct {
		name string
		args []byte
	}{
		{
			name: "double quoted",
			args: []byte(`"server_version" "15.4"`),
		},
		{
			name: "single quoted",
			args: []byte(`'hello' 'world'`),
		},
		{
			name: "single quoted with escapes",
			args: []byte(`'hello\x0aworld\x09tab'`),
		},
		{
			name: "mixed",
			args: []byte(`"name" 42 'value'`),
		},
		{
			name: "data row",
			args: []byte(` 1 1 '1'`),
		},
		{
			name: "row description",
			args: []byte(` 1 "n" 0 0 23 4 -1 0`),
		},
		{
			name: "parse message",
			args: []byte(` "" "select n from generate_series(1,5) n" 0`),
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			opts := pgproto3.TracerOptions{}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, err := range opts.ParseArgs(bm.args) {
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
