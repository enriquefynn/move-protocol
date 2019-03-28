package partitioning

import "github.com/enriquefynn/sharding-runner/burrow-client/config"

// Partitions start at 1 to nPartitions inclusive
type Partitioning interface {
	Add(k int64) int64 // Return partition added
	Get(k int64) int64
	IsSame(keys ...int64) bool
	Move(k, m int64)
	WhereToMove(keys ...int64) int64
}

type HashPartitioning struct {
	nPartitions             int64
	elementsInEachPartition map[int64]int64
	partitionMap            map[int64]int64
}

func NewHashPartitioning(nPartitions int64) *HashPartitioning {
	return &HashPartitioning{
		nPartitions:             nPartitions,
		partitionMap:            make(map[int64]int64),
		elementsInEachPartition: make(map[int64]int64),
	}
}

func (hp *HashPartitioning) Add(k int64) int64 {
	// Partitions start at 1
	partition := k%hp.nPartitions + 1
	hp.partitionMap[k] = partition
	hp.elementsInEachPartition[partition]++
	return partition
}

func (hp *HashPartitioning) Get(k int64) int64 {
	return hp.partitionMap[k]
}

func (hp *HashPartitioning) IsSame(keys ...int64) bool {
	part := hp.Get(keys[0])
	for _, k := range keys[1:] {
		if hp.Get(k) != part {
			return false
		}
	}
	return true
}

func (hp *HashPartitioning) Move(k, m int64) {
	hp.partitionMap[k] = m
}

func (hp *HashPartitioning) WhereToMove(keys ...int64) int64 {
	moveToPartition := hp.Get(keys[0])
	if moveToPartition == 0 {
		moveToPartition = hp.Add(keys[0])
	}
	for _, k := range keys[1:] {

		partitionK := hp.Get(k)
		if partitionK == 0 {
			partitionK = hp.Add(k)
		}
		if hp.elementsInEachPartition[moveToPartition] > hp.elementsInEachPartition[partitionK] {
			moveToPartition = partitionK
		}
	}
	return moveToPartition
}

func GetPartitioning(config *config.Config) Partitioning {
	var partitioning Partitioning
	if config.Partitioning.Type == "hash" {
		partitioning = NewHashPartitioning(config.Partitioning.NumberPartitions)
	}
	return partitioning
}
