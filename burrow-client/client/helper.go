package main

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/big"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/enriquefynn/sharding-runner/burrow-client/config"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/utils"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/binary"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/deploy/def"
	"github.com/hyperledger/burrow/logging"
	"github.com/hyperledger/burrow/txs"
	"github.com/hyperledger/burrow/txs/payload"
	log "github.com/sirupsen/logrus"
)

type ScalableCoin struct {
	abi          abi.ABI
	accountABI   abi.ABI
	contractAddr crypto.Address
	logs         *utils.Log
	logger       *logging.Logger

	partitioning        *HashPartitioning
	reachedMaxContracts bool

	balancePrediction []int64
	sync.RWMutex
	nextPartition int64

	// contractsInShard map[int64]map[crypto.Address]bool
	// allowedCrossShard map[int64]map[crypto.Address]bool

	crossShardCount map[crypto.Address]int

	// shouldCrossShard int

	staticToken map[int64][]crypto.Address
}

func (sc *ScalableCoin) GetCrossShardRandom(fromToken crypto.Address, fromPartition, toPartition int64) (crypto.Address, error) {
	// if !sc.allowedCrossShard[fromPartition][fromToken] {
	// 	return crypto.ZeroAddress, fmt.Errorf("Cannot do cross-shard")
	// 	log.Fatalf("Contract %v not allowed to cross shard", fromToken)
	// }

	// delete(sc.contractsInShard[fromPartition], fromToken)

	// for token := range sc.contractsInShard[toPartition] {
	// 	sc.crossShardCount[token]++
	// 	delete(sc.allowedCrossShard[toPartition], token)
	// 	return token, nil
	// }
	// log.Fatalf("Should not happen")
	// return crypto.ZeroAddress, nil

	randToken := rand.Intn(len(sc.staticToken[toPartition]))
	return sc.staticToken[toPartition][randToken], nil
}

func (sc *ScalableCoin) GetSameShardRandom(partition int64) crypto.Address {
	// sc.contractsInShardMutex.Lock()
	// defer sc.contractsInShardMutex.Unlock()
	// sc.Lock()
	// defer sc.Unlock()

	for _, token := range sc.staticToken[partition] {
		// delete(sc.allowedCrossShard[partition], token)
		return token
	}
	log.Fatalf("Should not happen")
	return crypto.ZeroAddress
}

func (sc *ScalableCoin) FinishCrossShard(fromToken, toToken crypto.Address, fromPartition, toPartition int64) {
	// sc.contractsInShardMutex.Lock()
	// sc.allowedCrossShardMutex.Lock()
	// defer sc.contractsInShardMutex.Unlock()
	// defer sc.allowedCrossShardMutex.Unlock()
	// sc.Lock()
	// defer sc.Unlock()

	// sc.contractsInShard[fromPartition][fromToken] = true
	// sc.crossShardCount[toToken]--
	// if sc.crossShardCount[toToken] == 0 {
	// 	sc.allowedCrossShard[toPartition][toToken] = true
	// }
}

func (sc *ScalableCoin) FinishSameShard(toToken crypto.Address, toPartition int64) {
	// sc.contractsInShardMutex.Lock()
	// sc.allowedCrossShardMutex.Lock()
	// defer sc.contractsInShardMutex.Unlock()
	// defer sc.allowedCrossShardMutex.Unlock()
	sc.Lock()
	defer sc.Unlock()

	sc.crossShardCount[toToken]--
	// if sc.crossShardCount[toToken] == 0 {
	// 	sc.allowedCrossShard[toPartition][toToken] = true
	// }
}

func (sc *ScalableCoin) AddToken(token crypto.Address, partition int64) {
	// sc.contractsInShardMutex.Lock()
	// sc.allowedCrossShardMutex.Lock()
	// defer sc.contractsInShardMutex.Unlock()
	// defer sc.allowedCrossShardMutex.Unlock()
	// sc.Lock()
	// defer sc.Unlock()

	// sc.contractsInShard[partition][token] = true
	// sc.allowedCrossShard[partition][token] = true
}

func (sc *ScalableCoin) AddStaticToken(token crypto.Address, partition int64) {
	// sc.contractsInShardMutex.Lock()
	// sc.allowedCrossShardMutex.Lock()
	// defer sc.contractsInShardMutex.Unlock()
	// defer sc.allowedCrossShardMutex.Unlock()
	sc.Lock()
	defer sc.Unlock()

	sc.staticToken[partition] = append(sc.staticToken[partition], token)

	// sc.contractsInShard[partition][token] = true
	// sc.allowedCrossShard[partition][token] = true
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
	var balancePrediction []int64
	for i := int64(0); i < config.Partitioning.NumberPartitions; i++ {
		balancePrediction = append(balancePrediction, 0)
	}
	sc := &ScalableCoin{
		abi:               contractABI,
		accountABI:        accountABI,
		partitioning:      partitioning,
		logs:              logs,
		logger:            logging.NewNoopLogger(),
		balancePrediction: balancePrediction,
		// contractsInShard:  make(map[int64]map[crypto.Address]bool),
		// allowedCrossShard: make(map[int64]map[crypto.Address]bool),
		crossShardCount: make(map[crypto.Address]int),

		staticToken: make(map[int64][]crypto.Address),
	}
	// for i := int64(1); i <= config.Partitioning.NumberPartitions; i++ {
	// sc.contractsInShard[i] = make(map[crypto.Address]bool)
	// sc.allowedCrossShard[i] = make(map[crypto.Address]bool)
	// }

	go func() {
		for {
			logs.Log("balance", "%d ", time.Now().UnixNano())
			sc.Lock()
			for i := int64(1); i <= config.Partitioning.NumberPartitions; i++ {
				sc.balancePrediction[i-1] = partitioning.elementsInEachPartition[i]
				logs.Log("balance", "%d ", partitioning.elementsInEachPartition[i])
			}
			log.Infof("Balance: %v", partitioning.elementsInEachPartition)
			sc.Unlock()
			logs.Log("balance", "\n")
			time.Sleep(time.Minute)
		}
	}()

	return sc
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

	receipt, err := client.BroadcastEnvelope(env, sc.logger)
	if err != nil {
		return err
	}
	contract := receipt.Receipt.ContractAddress
	contractAccount, err := client.GetAccount(contract)
	if len(contractAccount.Code) == 0 {
		return fmt.Errorf("Contract creation failed : %v", account)
	}
	sc.contractAddr = contract
	return nil
}

type Operation struct {
	Name            string
	Tx              *payload.CallTx
	moveToPartition int64 // To partition
	toToken         crypto.Address
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

func (sc *ScalableCoin) GetNext() int64 {
	sc.nextPartition = (sc.nextPartition + 1) % sc.partitioning.nPartitions
	return sc.nextPartition + 1
}

func (sc *ScalableCoin) GetOp(token crypto.Address) *Operation {
	// createToss := rand.Float32()
	sc.partitioning.RLock()
	sc.Lock()
	// sc.allowedCrossShardMutex.RLock()
	defer sc.partitioning.RUnlock()
	defer sc.Unlock()
	// defer sc.allowedCrossShardMutex.RUnlock()

	op := Operation{}

	op.Name = "transfer"
	fromPartition := sc.partitioning.partitionMap[token]
	var randPartition int64
	var toCrossShardToken crypto.Address
	var crossShardToss float32
	// log.Infof("shouldCrossShard: %v", sc.shouldCrossShard)
	// if sc.shouldCrossShard > 0 {
	// 	crossShardToss = 0.0
	// } else {
	crossShardToss = rand.Float32()
	// }
	if crossShardToss < sc.partitioning.crossShardPercentage {
		// Not allowed to make crossshard
		// if !sc.allowedCrossShard[fromPartition][token] {
		// 	log.Warnf("Contract not allowed to make cross-shard")
		// 	randPartition = fromPartition
		// 	sc.shouldCrossShard++
		// 	goto decision
		// } else {
		// randPartition = sc.partitioning.crossShardRandChoice[fromPartition-1][rand.Intn(int(sc.partitioning.nPartitions-1))] + 1
		mostUnbalancedPartition := 0
		secondMostUnbalanced := 1

		for idx, bal := range sc.balancePrediction {
			if bal < sc.balancePrediction[mostUnbalancedPartition] {
				secondMostUnbalanced = mostUnbalancedPartition
				mostUnbalancedPartition = idx
			} else if bal < sc.balancePrediction[secondMostUnbalanced] && idx != mostUnbalancedPartition {
				secondMostUnbalanced = idx
			}
		}
		if mostUnbalancedPartition == secondMostUnbalanced {
			log.Fatalf("Should not happen %v %v", mostUnbalancedPartition, secondMostUnbalanced)
		}
		// randPartition = sc.partitioning.GetMostUnbalanced()
		randPartition = int64(mostUnbalancedPartition + 1)
		if fromPartition == randPartition {
			randPartition = int64(secondMostUnbalanced + 1)
		}
		sc.balancePrediction[fromPartition-1]--
		sc.balancePrediction[randPartition-1]++
		// }

		// randPartition = sc.partitioning.crossShardRandChoice[fromPartition-1][rand.Intn(int(sc.partitioning.nPartitions-1))] + 1

		if len(sc.partitioning.partitionObjMap[randPartition]) == 0 {
			randPartition = fromPartition
			log.Warnf("No objects in partition %v for cross-shard", randPartition)
		}
		// var err error
		toCrossShardToken, _ = sc.GetCrossShardRandom(token, fromPartition, randPartition)
		// if err != nil {
		// 	randPartition = fromPartition
		// 	sc.shouldCrossShard++
		// } else if sc.shouldCrossShard > 0 {
		// 	sc.shouldCrossShard--
		// }
	} else {
		randPartition = fromPartition
	}
	// decision:
	if randPartition == fromPartition {
		// Sameshard
		toToken := sc.GetSameShardRandom(fromPartition)
		op.toToken = toToken
		op.Tx = sc.createTransfer(token, toToken)
		debug("Creating same-shard transfer on partition %v from: %v, to: %v", fromPartition, token, op.toToken)
		// log.Infof("Creating same-shard transfer on partition %v from: %v, to: %v", fromPartition, token, op.toToken)
	} else {
		// toToken := sc.GetCrossShardRandom(token, fromPartition, randPartition)
		op.toToken = toCrossShardToken
		op.Tx = sc.createTransfer(token, toCrossShardToken)
		op.moveToPartition = randPartition
		debug("Creating cross-shard transfer from partition %v to partition: %v, from token %v, to token: %v", fromPartition, randPartition, token, op.toToken)
		// log.Infof("Creating cross-shard transfer from partition %v to partition: %v, from token %v, to token: %v", fromPartition, randPartition, token, op.toToken)
	}
	return &op
}

func (sc *ScalableCoin) GetRetryOp(token crypto.Address, op *Operation) {
	sc.partitioning.RLock()
	defer sc.partitioning.RUnlock()
	partition := sc.partitioning.partitionMap[token]
	toTokenPartition := sc.partitioning.partitionMap[op.toToken]
	if partition == toTokenPartition {
		op.moveToPartition = 0
	} else {
		log.Infof("Retry got, send to partition %v", toTokenPartition)
		op.moveToPartition = toTokenPartition
	}
	debug("Creating retry transfer from partition %v to partition: %v, from token %v, to token: %v", partition, toTokenPartition, token, op.toToken)
}
