package main

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/big"
	"math/rand"
	"os"
	"time"

	"github.com/enriquefynn/sharding-runner/burrow-client/config"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/utils"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/binary"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/deploy/def"
	"github.com/hyperledger/burrow/txs"
	"github.com/hyperledger/burrow/txs/payload"
	log "github.com/sirupsen/logrus"
)

type ScalableCoin struct {
	abi          abi.ABI
	accountABI   abi.ABI
	contractAddr crypto.Address
	logs         *utils.Log

	partitioning *HashPartitioning
}

func NewScalableCoinAPI(config *config.Config, logs *utils.Log) *ScalableCoin {
	contractABIJson, err := os.Open(config.Contracts.CKABI)
	fatalError(err)
	contractABI, err := abi.JSON(contractABIJson)
	fatalError(err)

	accountABIJson, err := os.Open(config.Contracts.KittyABI)
	fatalError(err)
	accountABI, err := abi.JSON(accountABIJson)
	fatalError(err)

	crossShardPercentage := config.Benchmark.CrossShardPercentage
	createContractPercentage := config.Benchmark.CreateContractPercentage
	maximumAccounts := config.Benchmark.MaximumAccounts

	partitioning := NewHashPartitioning(config.Partitioning.NumberPartitions, crossShardPercentage, createContractPercentage, maximumAccounts)

	return &ScalableCoin{
		abi:          contractABI,
		accountABI:   accountABI,
		partitioning: partitioning,
		logs:         logs,
	}
}

func (sc *ScalableCoin) CreateContract(client *def.Client, chainID, codePath string, account acm.AddressableSigner, args ...interface{}) error {
	var byteArgs []byte
	var err error
	if len(args) != 0 {
		byteArgs, err = sc.abi.Constructor.Inputs.Pack(args...)
		if err != nil {
			return err
		}
	}

	contractContents, err := ioutil.ReadFile(codePath)
	if err != nil {
		return err
	}

	contractHex, err := hex.DecodeString(string(contractContents))
	if err != nil {
		return err
	}

	tx := payload.Payload(&payload.CallTx{
		Input: &payload.TxInput{
			Address:  account.GetAddress(),
			Amount:   1,
			Sequence: 1,
		},
		Data:     append(contractHex, byteArgs...),
		Address:  nil,
		Fee:      1,
		GasLimit: 4100000000,
	})
	env := txs.Enclose(chainID, tx)
	err = env.Sign(account)

	receipt, err := client.BroadcastEnvelope(env)
	if err != nil {
		return err
	}
	contract := receipt.Receipt.ContractAddress
	contractAccount, err := client.GetAccount(contract)
	if len(contractAccount.Code) == 0 {
		return fmt.Errorf("Contract creation failed : %v", account)
	}
	log.Infof("Created contract: %v", contract)
	sc.contractAddr = contract
	return nil
}

type Operation struct {
	Name            string
	Tx              *payload.CallTx
	moveToPartition int64 // To partition
}

func (sc *ScalableCoin) createNewAccount() *payload.CallTx {
	tx := payload.CallTx{
		Input: &payload.TxInput{
			Amount: 1e5,
		},
		Address:  &sc.contractAddr,
		Fee:      1,
		GasLimit: 4100000000,
	}

	txInput, err := sc.abi.Methods["newAccount"].Inputs.Pack()
	fatalError(err)
	tx.Data = append(sc.abi.Methods["newAccount"].Id(), txInput...)
	return &tx
}

func (sc *ScalableCoin) createTransfer(from, to crypto.Address) *payload.CallTx {
	tx := payload.CallTx{
		Input: &payload.TxInput{
			Amount: 1,
		},
		Address:  &from,
		Fee:      1,
		GasLimit: 4100000000,
	}

	defaultTokenAmount := big.NewInt(1)
	txInput, err := sc.accountABI.Methods["transfer"].Inputs.Pack(to, defaultTokenAmount)
	fatalError(err)
	tx.Data = append(sc.accountABI.Methods["transfer"].Id(), txInput...)
	return &tx
}

func (sc *ScalableCoin) createMoveTo(token crypto.Address, from int) *payload.CallTx {
	moveToTx := &payload.CallTx{
		Input: &payload.TxInput{
			Amount: 1,
		},
		Address:  &token,
		Fee:      1,
		GasLimit: 4100000000,
	}

	txInput, err := sc.accountABI.Methods["moveTo"].Inputs.Pack(big.NewInt(int64(from)))
	fatalError(err)
	moveToTx.Data = append(sc.accountABI.Methods["moveTo"].Id(), txInput...)
	return moveToTx
}

func (sc *ScalableCoin) NewAccount(address crypto.Address) Operation {
	tx := sc.createNewAccount()
	return Operation{
		Name: "newAccount",
		Tx:   tx,
	}
}

func fatalError(err error) {
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func (sc *ScalableCoin) extractContractAddress(event binary.HexBytes) *crypto.Address {
	// log.Infof("CREATE: %v", event)
	addr := crypto.MustAddressFromBytes(event[12:32])
	return &addr
}

func (sc *ScalableCoin) GetOp(address, token crypto.Address) *Operation {
	createToss := rand.Float32()
	sc.partitioning.RLock()
	defer sc.partitioning.RUnlock()
	op := Operation{}

	if sc.partitioning.numberContracts >= sc.partitioning.maximumAccounts {
		log.Infof("Reached maximum contracts: %v %v", sc.partitioning.numberContracts, time.Now().UnixNano())
		sc.logs.Log("reached-maximum-contracts", "%d %d\n", sc.partitioning.numberContracts, time.Now().UnixNano())
		// Ensure doesn't come here again
	}

	if sc.partitioning.numberContracts < sc.partitioning.maximumAccounts && createToss < sc.partitioning.createContractPercentage {
		op.Name = "newAccount"
		op.Tx = sc.createNewAccount()
	} else {
		op.Name = "transfer"
		crossShardToss := rand.Float32()
		partition := sc.partitioning.partitionMap[token] - 1
		var randPartition int64
		if sc.partitioning.nPartitions == 1 {
			randPartition = 1
		} else {
			randPartition = sc.partitioning.crossShardRandChoice[partition][rand.Intn(int(sc.partitioning.nPartitions-1))] + 1
		}

		if crossShardToss < sc.partitioning.crossShardPercentage && len(sc.partitioning.partitionObjMap[randPartition]) != 0 {
			for toToken := range sc.partitioning.partitionObjMap[randPartition] {
				op.Tx = sc.createTransfer(token, toToken)
				op.moveToPartition = randPartition
				debug("Creating cross-shard transfer from partition %v to partition: %v, from token %v, to token: %v", partition+1, randPartition, token, toToken)
				break
			}
		} else {
			partition := sc.partitioning.partitionMap[token]
			for toToken := range sc.partitioning.partitionObjMap[partition] {
				op.Tx = sc.createTransfer(token, toToken)
				debug("Creating same-shard transfer on partition %v from: %v, to: %v", partition, token, toToken)
				break
			}
		}
	}
	return &op
}
