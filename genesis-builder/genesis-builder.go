package main

import (
	"encoding/json"
	"io/ioutil"
	"math/big"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

var (
	key, _  = crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000001")
	key2, _ = crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000002")
	key3, _ = crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000003")

// key2, _ = crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000002")
)

func main() {
	initialBalance := common.Big1
	initialBalance = initialBalance.Lsh(initialBalance, 200)
	var initialAllocationKeys int64 = 10

	genesisFileLocation := os.Args[1]
	shardID, _ := strconv.Atoi(os.Args[2])

	os.MkdirAll(filepath.Dir(genesisFileLocation), 0755)

	initialAllocation := make(core.GenesisAlloc)
	var i int64
	for i = 1; i <= initialAllocationKeys; i++ {
		privateKey, _ := crypto.HexToECDSA(common.BigToHash(big.NewInt(i)).String()[2:])
		publicKey := crypto.PubkeyToAddress(privateKey.PublicKey)
		initialAllocation[publicKey] = core.GenesisAccount{Balance: initialBalance}
	}
	config := params.AllCliqueProtocolChanges
	shardLeaderAddress := crypto.PubkeyToAddress(key.PublicKey)
	shardLeaderAddress2 := crypto.PubkeyToAddress(key2.PublicKey)
	// shardLeaderAddress3 := crypto.PubkeyToAddress(key3.PublicKey)

	shardLeaders := shardLeaderAddress[:]
	shardLeaders = append(shardLeaders, shardLeaderAddress2[:]...)
	// shardLeaders = append(shardLeaders, shardLeaderAddress3[:]...)

	config.ShardID = shardID - 1
	config.Clique.Period = 5
	config.Clique.Epoch = 1

	genesis := core.Genesis{
		Config:     config,
		ExtraData:  append(append(make([]byte, 32), shardLeaders...), make([]byte, 65)...),
		GasLimit:   8000000,
		Difficulty: big.NewInt(1),
		Alloc:      initialAllocation,
	}
	out, _ := json.MarshalIndent(genesis, "", "  ")
	ioutil.WriteFile(genesisFileLocation, out, 0644)
}
