package pgtype

type Cidr Inet

func (dst *Cidr) Set(src interface{}) error {
	return (*Inet)(dst).Set(src)
}

func (dst *Cidr) Get() interface{} {
	return (*Inet)(dst).Get()
}

func (src *Cidr) AssignTo(dst interface{}) error {
	return (*Inet)(src).AssignTo(dst)
}

func (dst *Cidr) DecodeText(ci *ConnInfo, src []byte) error {
	return (*Inet)(dst).DecodeText(ci, src)
}

func (dst *Cidr) DecodeBinary(ci *ConnInfo, src []byte) error {
	return (*Inet)(dst).DecodeBinary(ci, src)
}

func (src *Cidr) EncodeText(ci *ConnInfo, buf []byte) ([]byte, error) {
	return (*Inet)(src).EncodeText(ci, buf)
}

func (src *Cidr) EncodeBinary(ci *ConnInfo, buf []byte) ([]byte, error) {
	return (*Inet)(src).EncodeBinary(ci, buf)
}
