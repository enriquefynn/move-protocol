package main

import (
	"encoding/binary"
	"sync"

	"github.com/hyperledger/burrow/crypto"
)

type HashPartitioning struct {
	crossShardPercentage     float32
	createContractPercentage float32
	maximumAccounts          int

	nPartitions             int64
	elementsInEachPartition map[int64]int64
	partitionMap            map[crypto.Address]int64
	// partitionObjs           map[int64][]crypto.Address

	partitionObjMap map[int64]map[crypto.Address]bool

	numberContracts      int
	crossShardRandChoice [][]int64
	sync.RWMutex
}

func NewHashPartitioning(nPartitions int64, crossShardPercentage, createContractPercentage float32, maximumAccounts int) *HashPartitioning {
	hp := &HashPartitioning{
		crossShardPercentage:     crossShardPercentage,
		createContractPercentage: createContractPercentage,
		maximumAccounts:          maximumAccounts,

		nPartitions:             nPartitions,
		partitionMap:            make(map[crypto.Address]int64),
		elementsInEachPartition: make(map[int64]int64),
		// partitionObjs:           make(map[int64][]crypto.Address),
		partitionObjMap: make(map[int64]map[crypto.Address]bool),
	}

	for i := int64(0); i < nPartitions; i++ {
		var randChoice []int64
		for j := int64(0); j < nPartitions; j++ {
			if j == i {
				continue
			}
			randChoice = append(randChoice, j)
		}
		hp.crossShardRandChoice = append(hp.crossShardRandChoice, randChoice)
		hp.partitionObjMap[i+1] = make(map[crypto.Address]bool)
	}
	return hp
}

func (hp *HashPartitioning) GetHash(k crypto.Address) int64 {
	return int64(binary.BigEndian.Uint64(k[12:])%uint64(hp.nPartitions)) + 1
}

func (hp *HashPartitioning) Add(k crypto.Address) int64 {
	hp.Lock()
	hp.numberContracts++
	defer hp.Unlock()
	// Partitions start at 1

	partition := hp.GetHash(k)
	hp.partitionMap[k] = partition
	hp.elementsInEachPartition[partition]++

	hp.partitionObjMap[partition][k] = true
	return partition
}

func (hp *HashPartitioning) Get(k crypto.Address) (int64, bool) {
	v, ok := hp.partitionMap[k]
	return v, ok
}

func (hp *HashPartitioning) IsSame(keys ...crypto.Address) bool {
	part, exists := hp.Get(keys[0])
	if !exists {
		panic("Got key that doesn't exist")
	}
	for _, k := range keys[1:] {
		p, exists := hp.Get(k)
		if !exists {
			panic("Got key that doesn't exist")
		}
		if p != part {
			return false
		}
	}
	return true
}

func (hp *HashPartitioning) Move(k crypto.Address, m int64) {
	hp.Lock()
	defer hp.Unlock()
	originalPartition, exists := hp.Get(k)
	if !exists {
		hp.elementsInEachPartition[m]++
	} else {
		hp.elementsInEachPartition[originalPartition]--
		hp.elementsInEachPartition[m]++
	}
	hp.partitionMap[k] = m

	if exists {
		debug("Moving %v to %v", k, m)
		// Delete from objMap
		delete(hp.partitionObjMap[originalPartition], k)
		hp.partitionObjMap[m][k] = true
	}
}

func (hp *HashPartitioning) WhereToMove(keys ...crypto.Address) int64 {
	moveToPartition, exists := hp.Get(keys[0])
	if !exists {
		moveToPartition = hp.Add(keys[0])
		panic("Should exist")
	}
	for _, k := range keys[1:] {
		partitionK, exists := hp.Get(k)
		if !exists {
			partitionK = hp.Add(k)
			panic("Should exist!")
		}
		if hp.elementsInEachPartition[moveToPartition] > hp.elementsInEachPartition[partitionK] {
			moveToPartition = partitionK
		}
	}
	return moveToPartition
}
