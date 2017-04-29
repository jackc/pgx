package pgproto3

type NoticeResponse ErrorResponse

func (*NoticeResponse) Backend() {}

func (dst *NoticeResponse) UnmarshalBinary(src []byte) error {
	return (*ErrorResponse)(dst).UnmarshalBinary(src)
}

func (src *NoticeResponse) MarshalBinary() ([]byte, error) {
	return (*ErrorResponse)(src).marshalBinary('N')
}
