package pgproto3

import (
	"encoding/json"
)

type BindComplete struct{}

func (*BindComplete) Backend() {}

func (dst *BindComplete) UnmarshalBinary(src []byte) error {
	if len(src) != 0 {
		return &invalidMessageLenErr{messageType: "BindComplete", expectedLen: 0, actualLen: len(src)}
	}

	return nil
}

func (src *BindComplete) MarshalBinary() ([]byte, error) {
	return []byte{'2', 0, 0, 0, 4}, nil
}

func (src *BindComplete) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string
	}{
		Type: "BindComplete",
	})
}
