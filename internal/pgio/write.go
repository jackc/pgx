package pgio

func AppendUint16(buf []byte, n uint16) []byte {
	return append(buf, byte(n>>8), byte(n))
}

func AppendUint32(buf []byte, n uint32) []byte {
	return append(buf, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
}

func AppendUint64(buf []byte, n uint64) []byte {
	return append(buf,
		byte(n>>56), byte(n>>48), byte(n>>40), byte(n>>32),
		byte(n>>24), byte(n>>16), byte(n>>8), byte(n),
	)
}

func AppendInt16(buf []byte, n int16) []byte {
	return AppendUint16(buf, uint16(n))
}

func AppendInt32(buf []byte, n int32) []byte {
	return AppendUint32(buf, uint32(n))
}

func AppendInt64(buf []byte, n int64) []byte {
	return AppendUint64(buf, uint64(n))
}

func SetInt32(buf []byte, n int32) {
	*(*[4]byte)(buf) = [4]byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
}
