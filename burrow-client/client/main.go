package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/rpc/rpcquery"
	"github.com/hyperledger/burrow/txs"
	"github.com/hyperledger/burrow/txs/payload"

	"github.com/hyperledger/burrow/acm"
	yaml "gopkg.in/yaml.v2"

	"github.com/hyperledger/burrow/deploy/def"

	"github.com/enriquefynn/sharding-runner/burrow-client/config"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/utils"
	log "github.com/sirupsen/logrus"
)

func checkFatalError(err error) {
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func debug(format string, args ...interface{}) {
	// log.Infof(format, args...)
}

type Client struct {
	id           int
	scalableCoin *ScalableCoin

	chainID string

	clients          map[string][]*def.Client
	acc              acm.AddressableSigner
	myTokens         []*crypto.Address
	tokenToPartition map[crypto.Address]int64

	myAddress            crypto.Address
	sequencePerPartition map[string]uint64

	signedHeaderCh chan MoveResponse
	logs           *utils.Log
}

func NewClient(accountID int, clients map[string][]*def.Client, scalableCoin *ScalableCoin, acc acm.AddressableSigner, logs *utils.Log, signedHeaderCh chan MoveResponse) *Client {
	return &Client{
		id:           accountID,
		scalableCoin: scalableCoin,

		clients:          clients,
		acc:              acc,
		tokenToPartition: make(map[crypto.Address]int64),

		myAddress:            acc.GetAddress(),
		sequencePerPartition: make(map[string]uint64),

		signedHeaderCh: signedHeaderCh,
		logs:           logs,
	}
}

func (c *Client) getRandomClient(partition string) *def.Client {
	return c.clients[partition][rand.Intn(len(c.clients[partition]))]
}

func (c *Client) createContract(tx *payload.CallTx) error {
	isMoving := false
	startTime := time.Now()
	defer func() {
		c.logs.Log("latencies-cli"+strconv.Itoa(c.id), "newAccount %d %d %v\n", startTime.UnixNano(), time.Since(startTime).Nanoseconds(), isMoving)
	}()

	c.sequencePerPartition["1"]++
	tx.Input.Sequence = c.sequencePerPartition["1"]
	tx.Input.Address = c.myAddress

	txPayload := payload.Payload(tx)
	env := txs.Enclose("1", txPayload)
	err := env.Sign(c.acc)
	if err != nil {
		return err
	}
	ex, err := c.getRandomClient("1").BroadcastEnvelope(env)
	if err != nil {
		return err
	}
	if ex.Exception != nil {
		return fmt.Errorf("Exception: %v", ex.Exception.Exception)
	}
	var addr *crypto.Address
	var partition int64
	for _, ev := range ex.Events {
		if ev.Log != nil {
			addr = c.scalableCoin.extractContractAddress(ev.Log.Data)
			partition = c.scalableCoin.partitioning.GetHash(*addr)
			// Should move
			if partition != 1 {
				debug("Moving %v to partition %v", tx.Address, partition)
				isMoving = true
				moveTo := c.scalableCoin.createMoveTo(*addr, int(partition))
				err := c.broadcastMove(moveTo, "1", strconv.Itoa(int(partition)))
				if err != nil {
					log.Warnf("ERROR doing move while making contract")
					return err
				}
			}
		}
	}
	c.myTokens = append(c.myTokens, addr)
	c.tokenToPartition[*addr] = partition
	c.scalableCoin.partitioning.Add(*addr)
	return nil
}

func (c *Client) transfer(tx *payload.CallTx, moveToPartition int64) error {
	isMoving := false
	startTime := time.Now()
	failed := false
	defer func() {
		c.logs.Log("latencies-cli"+strconv.Itoa(c.id), "transfer %d %d %v %v\n", startTime.UnixNano(), time.Since(startTime).Nanoseconds(), isMoving, failed)
	}()

	fromPartition := c.tokenToPartition[*tx.Address]
	fromPartitionStr := strconv.Itoa(int(fromPartition))

	toPartitionStr := "10"
	// Should move
	if moveToPartition != 0 {
		isMoving = true
		toPartitionStr := strconv.Itoa(int(moveToPartition))
		if fromPartition == 0 {
			return fmt.Errorf("Not owned token, should not happen")
		}
		moveTo := c.scalableCoin.createMoveTo(*tx.Address, int(moveToPartition))
		err := c.broadcastMove(moveTo, fromPartitionStr, toPartitionStr)
		if err != nil {
			log.Warnf("ERROR DOING MOVE while transfering")
			return err
		}
	} else {
		toPartitionStr = strconv.Itoa(int(c.tokenToPartition[*tx.Address]))
		if c.tokenToPartition[*tx.Address] == 0 {
			return fmt.Errorf("c.tokenToPartition[tx.Address] == 0")
		}
	}

	if toPartitionStr == "10" {
		if moveToPartition != 0 {
			toPartitionStr = strconv.Itoa(int(moveToPartition))
		} else {
			toPartitionStr = strconv.Itoa(int(c.tokenToPartition[*tx.Address]))
		}
	}

	c.sequencePerPartition[toPartitionStr]++

	tx.Input.Sequence = c.sequencePerPartition[toPartitionStr]
	tx.Input.Address = c.myAddress

	txPayload := payload.Payload(tx)
	env := txs.Enclose(toPartitionStr, txPayload)
	err := env.Sign(c.acc)
	if err != nil {
		return err
	}

	ex, err := c.getRandomClient(toPartitionStr).BroadcastEnvelope(env)
	if err != nil {
		return err
	}
	if ex.Exception != nil {
		failed = true
		return fmt.Errorf("Exception executing transfer %v", ex.Exception.Exception)
	}

	return nil
}

type MoveResponse struct {
	height       int64
	chainID      string
	responseChan chan *payload.CallTx
}

func (c *Client) broadcastMove(tx *payload.CallTx, from string, to string) error {
	c.sequencePerPartition[from]++
	tx.Input.Address = c.myAddress
	tx.Input.Sequence = c.sequencePerPartition[from]

	txPayload := payload.Payload(tx)
	env := txs.Enclose(from, txPayload)
	err := env.Sign(c.acc)
	if err != nil {
		return err
	}
	cli := c.getRandomClient(from)
	ex, err := cli.BroadcastEnvelope(env)
	debug("Executed moveTo %v from %v to %v", tx.Address, from, to)
	if err != nil {
		log.Warnf("moveTo error")
		return err
	}
	if ex.Exception != nil {
		return fmt.Errorf("Exception: %v", ex.Exception.Exception)
	}
	proofs, err := cli.GetAccountProof(*tx.Address)
	debug("Got proof: wait for block %v", proofs.AccountProof.Version)
	if err != nil {
		return err
	}

	waitSignedHeader := make(chan *payload.CallTx)
	moveResponse := MoveResponse{
		height:       proofs.StorageProof.Version,
		chainID:      from,
		responseChan: waitSignedHeader,
	}
	c.signedHeaderCh <- moveResponse
	move2Tx := <-waitSignedHeader
	// if !ok {
	// 	return nil
	// }
	debug("Got signed header, sending to %v", to)

	c.sequencePerPartition[to]++
	move2Tx.StorageProof = &proofs.StorageProof
	move2Tx.AccountProof = &proofs.AccountProof
	move2Tx.Input = &payload.TxInput{
		Amount:   1,
		Address:  c.myAddress,
		Sequence: c.sequencePerPartition[to],
	}

	move2Tx.Address = tx.Address
	move2Tx.Fee = 1
	move2Tx.GasLimit = 4100000000

	txPayload = payload.Payload(move2Tx)
	env = txs.Enclose(to, txPayload)
	err = env.Sign(c.acc)
	if err != nil {
		return err
	}
	cli = c.getRandomClient(to)
	ex, err = cli.BroadcastEnvelope(env)

	toInt, err := strconv.Atoi(to)
	if err != nil {
		panic("ERROR converting int")
	}
	c.scalableCoin.partitioning.Move(*tx.Address, int64(toInt))
	c.tokenToPartition[*tx.Address] = int64(toInt)

	if err != nil {
		debug("Error sending move2 to %v", to)
		return err
	}
	if ex == nil {
		debug("Exception sending move2")
		return fmt.Errorf("Exception: Nil exception")
	} else if ex.Exception != nil {
		debug("Exception sending move2")
		return fmt.Errorf("Exception: %v", ex.Exception.Exception)
	}

	return nil
}

func signedHeaderGetter(blockChans []chan *rpcquery.SignedHeadersResult, clients map[string][]*def.Client, getHeader chan MoveResponse) {
	cases := make([]reflect.SelectCase, len(blockChans))
	mapMutex := sync.RWMutex{}
	blockGetHeaderMap := make(map[string]map[int64][]chan *payload.CallTx)
	running := true

	go func() {
		for {
			select {
			case get := <-getHeader:
				mapMutex.Lock()
				blockGetHeaderMap[get.chainID][get.height] = append(blockGetHeaderMap[get.chainID][get.height], get.responseChan)
				mapMutex.Unlock()
			}
		}
	}()

	for i, ch := range blockChans {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
		chainID := strconv.Itoa(i + 1)
		blockGetHeaderMap[chainID] = make(map[int64][]chan *payload.CallTx)
	}
	for running {
		_, selectValue, _ := reflect.Select(cases)
		signedBlock := selectValue.Interface().(*rpcquery.SignedHeadersResult)
		debug("Got block from partition: %v %v", signedBlock.SignedHeader.ChainID, signedBlock.SignedHeader.Height)
		chainID := signedBlock.SignedHeader.ChainID
		mapMutex.RLock()
		found := false
		for _, resp := range blockGetHeaderMap[chainID][signedBlock.SignedHeader.Height] {
			found = true
			tx := &payload.CallTx{
				SignedHeader: signedBlock.SignedHeader,
			}
			resp <- tx
		}
		mapMutex.RUnlock()
		if found {
			mapMutex.Lock()
			delete(blockGetHeaderMap[chainID], signedBlock.SignedHeader.Height)
			mapMutex.Unlock()
		}
	}
}

func (c *Client) spawnClient(ctx context.Context) {
	running := true
	go func() {
		<-ctx.Done()
		running = false
	}()

	tx := c.scalableCoin.createNewAccount()
	err := c.createContract(tx)
	for err != nil {
		log.Warnf("[Client %v] ERROR creating initial contract: %v", c.id, err)
		tx = c.scalableCoin.createNewAccount()
		err = c.createContract(tx)
	}

	for running {
		randomToken := *c.myTokens[rand.Intn(len(c.myTokens))]
		op := c.scalableCoin.GetOp(c.myAddress, randomToken)
		if op.Name == "newAccount" {
			err = c.createContract(op.Tx)
			if err != nil {
				log.Warnf("[Client %v] Error creating contract %v", c.id, err)
				break
			}
		} else {
			err = c.transfer(op.Tx, op.moveToPartition)
			if err != nil {
				log.Warnf("[Client %v] Error transfering %v", c.id, err)
				// break
			}
		}
	}
}

func generateClients(wg *sync.WaitGroup, ctx context.Context, clients map[string][]*def.Client, accountID int,
	scalableCoin *ScalableCoin, logs *utils.Log, signedHeaderCh chan MoveResponse) {
	defer wg.Done()

	tmpAccounts := make([]*acm.PrivateAccount, 1)
	tmpAccounts[0] = acm.GeneratePrivateAccountFromSecret(strconv.Itoa(accountID + 1))
	acc := acm.SigningAccounts(tmpAccounts)[0]

	c := NewClient(accountID, clients, scalableCoin, acc, logs, signedHeaderCh)
	c.spawnClient(ctx)
	log.Warnf("Stopping client %v", c.id)
}

func main() {
	config := config.Config{}
	configFile, err := ioutil.ReadFile(os.Args[1])
	checkFatalError(err)
	err = yaml.Unmarshal(configFile, &config)
	checkFatalError(err)

	var blockChans []chan *rpcquery.SignedHeadersResult
	logs, err := utils.NewLog(config.Logs.Dir)
	defer logs.Flush()

	defaultAccount := acm.SigningAccounts([]*acm.PrivateAccount{acm.GeneratePrivateAccountFromSecret("0")})
	clients := make(map[string][]*def.Client)
	// var blockChans []chan *rpcquery.SignedHeadersResult
	// Mapping for partition to created contracts address
	for _, c := range config.Servers {
		for _, shardClient := range c.Addresses {
			clients[c.ChainID] = append(clients[c.ChainID], def.NewClientWithLocalSigning(shardClient, time.Duration(config.Benchmark.Timeout)*time.Second, defaultAccount))
		}
	}

	scalableCoin := NewScalableCoinAPI(&config, logs)
	err = scalableCoin.CreateContract(clients["1"][0], "1", config.Contracts.Path, defaultAccount[0])
	checkFatalError(err)

	signedHeaderCh := make(chan MoveResponse)

	c := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(context.Background())
	signal.Notify(c, os.Interrupt)

	go func() {
		for range c {
			cancel()
		}
	}()
	timer := time.NewTimer(time.Second * config.Benchmark.ExperimentTime)
	go func() {
		<-timer.C
		log.Infof("Finishing experiment")
		cancel()
	}()

	checkFatalError(err)

	for partition := 0; partition < int(config.Partitioning.NumberPartitions); partition++ {
		chainID := strconv.Itoa(partition + 1)

		blockChans = append(blockChans, make(chan *rpcquery.SignedHeadersResult, 50))
		go utils.ListenBlockHeaders2(chainID, clients[chainID][0], logs, blockChans[partition])
	}

	var wg sync.WaitGroup
	for cli := 0; cli < config.Benchmark.Clients; cli++ {
		wg.Add(1)
		go generateClients(&wg, ctx, clients, cli, scalableCoin, logs, signedHeaderCh)
	}

	go signedHeaderGetter(blockChans, clients, signedHeaderCh)

	wg.Wait()
}
