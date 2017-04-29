package pgproto3

import (
	"encoding/json"
)

type NoData struct{}

func (*NoData) Backend() {}

func (dst *NoData) Decode(src []byte) error {
	if len(src) != 0 {
		return &invalidMessageLenErr{messageType: "NoData", expectedLen: 0, actualLen: len(src)}
	}

	return nil
}

func (src *NoData) MarshalBinary() ([]byte, error) {
	return []byte{'n', 0, 0, 0, 4}, nil
}

func (src *NoData) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
	}{
		Type: "NoData",
	})
}
