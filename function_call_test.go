package pgproto3

import (
	"github.com/go-test/deep"
	"testing"
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
		{"foo", fields{uint32(123), []uint16{0, 1, 0, 1}, [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")}, uint16(0)}, false},
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
			encoded := src.Encode([]byte{})
			decoded := &FunctionCall{}
			err := decoded.Decode(encoded[5:])
			if (err != nil) != tt.wantErr {
                t.Errorf("FunctionCall.Decode() error = %v, wantErr %v", err, tt.wantErr)
                return
            }
			if diff := deep.Equal(src, decoded); diff != nil {
				t.Error(diff)
			}
		})
	}
}