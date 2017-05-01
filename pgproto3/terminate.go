package pgproto3

import (
	"encoding/json"
)

type Terminate struct{}

func (*Terminate) Frontend() {}

func (dst *Terminate) Decode(src []byte) error {
	if len(src) != 0 {
		return &invalidMessageLenErr{messageType: "Terminate", expectedLen: 0, actualLen: len(src)}
	}

	return nil
}

func (src *Terminate) MarshalBinary() ([]byte, error) {
	return []byte{'X', 0, 0, 0, 4}, nil
}

func (src *Terminate) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
	}{
		Type: "Terminate",
	})
}
