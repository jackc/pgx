package pgproto3

import (
	"encoding/json"
)

type ReadyForQuery struct {
	TxStatus byte
}

func (*ReadyForQuery) Backend() {}

func (dst *ReadyForQuery) UnmarshalBinary(src []byte) error {
	if len(src) != 1 {
		return &invalidMessageLenErr{messageType: "ReadyForQuery", expectedLen: 1, actualLen: len(src)}
	}

	dst.TxStatus = src[0]

	return nil
}

func (src *ReadyForQuery) MarshalBinary() ([]byte, error) {
	return []byte{'Z', 0, 0, 0, 5, src.TxStatus}, nil
}

func (src *ReadyForQuery) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type     string
		TxStatus string
	}{
		Type:     "ReadyForQuery",
		TxStatus: string(src.TxStatus),
	})
}
