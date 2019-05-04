package pgx

const int16SlicePoolSizeStep = 16

type int16SlicePool struct {
	buf        []int16
	pos        int
	allocCount int
	resetCount int
}

func (p *int16SlicePool) get(n int) []int16 {
	if n > len(p.buf[p.pos:]) {
		growthStep := ((n / int16SlicePoolSizeStep) + 1) * int16SlicePoolSizeStep
		p.buf = make([]int16, len(p.buf)+growthStep)
		p.pos = 0
	}

	result := p.buf[p.pos : p.pos+n]
	p.pos += n

	p.allocCount += n

	return result
}

func (p *int16SlicePool) reset() {
	p.pos = 0
	p.resetCount += 1

	if p.resetCount == 128 {
		allocsPerReset := p.allocCount / p.resetCount
		maxSize := allocsPerReset + (int16SlicePoolSizeStep * 4)

		if len(p.buf) > maxSize {
			p.buf = make([]int16, maxSize)
		}

		p.allocCount = 0
		p.resetCount = 0
	}
}
