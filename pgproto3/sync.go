package pgproto3

import (
	"encoding/json"
)

type Sync struct{}

func (*Sync) Frontend() {}

func (dst *Sync) Decode(src []byte) error {
	if len(src) != 0 {
		return &invalidMessageLenErr{messageType: "Sync", expectedLen: 0, actualLen: len(src)}
	}

	return nil
}

func (src *Sync) MarshalBinary() ([]byte, error) {
	return []byte{'S', 0, 0, 0, 4}, nil
}

func (src *Sync) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
	}{
		Type: "Sync",
	})
}
