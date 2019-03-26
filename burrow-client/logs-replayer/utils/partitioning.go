package utils

type HashPartitioning struct {
	nPartitions  int64
	partitionMap map[int64]int64
}

func NewHashPartitioning(nPartitions int64) *HashPartitioning {
	return &HashPartitioning{
		nPartitions:  nPartitions,
		partitionMap: make(map[int64]int64),
	}
}

func (p *HashPartitioning) Add(k int64) {
	p.partitionMap[k] = k % p.nPartitions
}

func (p *HashPartitioning) Get(k int64) int64 {
	return p.partitionMap[k]
}

func (p *HashPartitioning) IsSame(keys ...int64) bool {
	part := p.Get(keys[0])
	for _, k := range keys[1:] {
		if p.Get(k) != part {
			return false
		}
	}
	return true
}

func (p *HashPartitioning) Move(k, m int64) {
	p.partitionMap[k] = m
}
