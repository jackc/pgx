package pgproto3

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFunctionCall_EncodeDecode(t *testing.T) {
	type fields struct {
		Function         uint32
		ArgFormatCodes   []uint16
		Arguments        [][]byte
		ResultFormatCode uint16
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{"valid", fields{uint32(123), []uint16{0, 1, 0, 1}, [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")}, uint16(1)}, false},
		{"invalid format code", fields{uint32(123), []uint16{2, 1, 0, 1}, [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")}, uint16(0)}, true},
		{"invalid result format code", fields{uint32(123), []uint16{1, 1, 0, 1}, [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")}, uint16(2)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &FunctionCall{
				Function:         tt.fields.Function,
				ArgFormatCodes:   tt.fields.ArgFormatCodes,
				Arguments:        tt.fields.Arguments,
				ResultFormatCode: tt.fields.ResultFormatCode,
			}
			encoded, err := src.Encode([]byte{})
			require.NoError(t, err)
			dst := &FunctionCall{}
			// Check the header
			msgTypeCode := encoded[0]
			if msgTypeCode != 'F' {
				t.Errorf("msgTypeCode %v should be 'F'", msgTypeCode)
				return
			}
			// Check length, does not include type code character
			l := binary.BigEndian.Uint32(encoded[1:5])
			if int(l) != (len(encoded) - 1) {
				t.Errorf("Incorrect message length, got = %v, wanted = %v", l, len(encoded))
			}
			// Check decoding works as expected
			err = dst.Decode(encoded[5:])
			if err != nil {
				if !tt.wantErr {
					t.Errorf("FunctionCall.Decode() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			if !reflect.DeepEqual(src, dst) {
				t.Error("difference after encode / decode cycle")
				t.Errorf("src = %v", src)
				t.Errorf("dst = %v", dst)
			}
		})
	}
}
