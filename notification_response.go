package pgproto3

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

type NotificationResponse struct {
	PID     uint32
	Channel string
	Payload string
}

func (*NotificationResponse) Backend() {}

func (dst *NotificationResponse) UnmarshalBinary(src []byte) error {
	buf := bytes.NewBuffer(src)

	pid := binary.BigEndian.Uint32(buf.Next(4))

	b, err := buf.ReadBytes(0)
	if err != nil {
		return err
	}
	channel := string(b[:len(b)-1])

	b, err = buf.ReadBytes(0)
	if err != nil {
		return err
	}
	payload := string(b[:len(b)-1])

	*dst = NotificationResponse{PID: pid, Channel: channel, Payload: payload}
	return nil
}

func (src *NotificationResponse) MarshalBinary() ([]byte, error) {
	var bigEndian BigEndianBuf
	buf := &bytes.Buffer{}

	buf.WriteByte('A')
	buf.Write(bigEndian.Uint32(uint32(4 + 4 + len(src.Channel) + len(src.Payload))))

	buf.WriteString(src.Channel)
	buf.WriteByte(0)
	buf.WriteString(src.Payload)
	buf.WriteByte(0)

	return buf.Bytes(), nil
}

func (src *NotificationResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string
		PID     uint32
		Channel string
		Payload string
	}{
		Type:    "NotificationResponse",
		PID:     src.PID,
		Channel: src.Channel,
		Payload: src.Payload,
	})
}
