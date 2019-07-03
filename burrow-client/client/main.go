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
	"github.com/hyperledger/burrow/rpc/rpcevents"
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

const expectedBlockTime = time.Duration(6 * time.Second)

type Client struct {
	id           int
	scalableCoin *ScalableCoin
	clientConn   map[string]*def.Client

	chainID string

	clients          map[string][]*def.Client
	acc              acm.AddressableSigner
	myTokens         []*crypto.Address
	tokenToPartition map[crypto.Address]int64

	myAddress            crypto.Address
	sequencePerPartition map[string]uint64

	signedHeaderCh     chan MoveResponse
	logs               *utils.Log
	contractsPerClient int
}

func NewClient(accountID int, clients map[string][]*def.Client, scalableCoin *ScalableCoin,
	acc acm.AddressableSigner, logs *utils.Log, contractsPerClient int,
	signedHeaderCh chan MoveResponse) *Client {
	clientConns := make(map[string]*def.Client)

	for p := range clients {
		randClientN := accountID % len(clients[p])
		clientConns[p] = clients[p][randClientN]
	}
	return &Client{
		id:           accountID,
		scalableCoin: scalableCoin,
		clientConn:   clientConns,

		clients:          clients,
		acc:              acc,
		tokenToPartition: make(map[crypto.Address]int64),

		myAddress:            acc.GetAddress(),
		sequencePerPartition: make(map[string]uint64),

		signedHeaderCh:     signedHeaderCh,
		logs:               logs,
		contractsPerClient: contractsPerClient,
	}
}

func (c *Client) createContract(tx *payload.CallTx, staticContract bool) error {
	isMoving := false
	startTime := time.Now()
	defer func() {
		c.logs.Log("latencies", "%v newAccount %d %d %v\n", c.id, startTime.UnixNano(), time.Since(startTime).Nanoseconds(), isMoving)
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
	ex, err := c.clientConn["1"].BroadcastEnvelope(env, c.scalableCoin.logger)
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
				err := c.broadcastMove(moveTo, "1", strconv.Itoa(int(partition)), staticContract)
				if err != nil {
					log.Warnf("ERROR doing move while making contract %v", err)
					return err
				}
			}
		}
	}
	if staticContract {
		c.scalableCoin.AddStaticToken(*addr, partition)
	} else {
		c.myTokens = append(c.myTokens, addr)
		c.tokenToPartition[*addr] = partition
		c.scalableCoin.partitioning.Move(*addr, partition)
		c.scalableCoin.AddToken(*addr, partition)
	}
	return nil
}

func (c *Client) transfer(tx *payload.CallTx, moveToPartition int64) error {
	isMoving := false
	startTime := time.Now()
	failed := false
	defer func() {
		c.logs.Log("latencies", "%v transfer %d %d %v %v\n", c.id, startTime.UnixNano(), time.Since(startTime).Nanoseconds(), isMoving, failed)
	}()

	fromPartition := c.tokenToPartition[*tx.Address]
	fromPartitionStr := strconv.Itoa(int(fromPartition))

	toPartitionStr := ""
	// Should move
	if moveToPartition != 0 {
		isMoving = true
		toPartitionStr = strconv.Itoa(int(moveToPartition))
		if fromPartition == 0 {
			return fmt.Errorf("Not owned token, should not happen")
		}
		moveTo := c.scalableCoin.createMoveTo(*tx.Address, int(moveToPartition))
		err := c.broadcastMove(moveTo, fromPartitionStr, toPartitionStr, false)
		if err != nil {
			log.Warnf("ERROR DOING MOVE while transfering: %v %v", err, c.id)
			return err
		}
	} else {
		toPartitionStr = strconv.Itoa(int(c.tokenToPartition[*tx.Address]))
		if c.tokenToPartition[*tx.Address] == 0 {
			return fmt.Errorf("c.tokenToPartition[tx.Address] == 0")
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

	ex, err := c.clientConn[toPartitionStr].BroadcastEnvelope(env, c.scalableCoin.logger)
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

func (c *Client) broadcastMove(tx *payload.CallTx, from string, to string, static bool) error {
	if from == to {
		log.Fatalf("Cannot move to itself: %v, from: %v to: %v", tx.Address, from, to)
	}
	if len(c.myTokens) == 0 {
		log.Warnf("Client %v has no tokens", c.id)
	} else {
		// if !bytes.Equal(tx.Address.Bytes(), c.myTokens[0].Bytes()) {
		// 	log.Fatalf("Moving not owned contract")
		// }
		if from != strconv.Itoa(int(c.tokenToPartition[*c.myTokens[0]])) {
			log.Warnf("FROM DIVERGE IN CLIENT: %v %v!", from, c.tokenToPartition[*c.myTokens[0]])
		}
	}
	c.sequencePerPartition[from]++
	tx.Input.Address = c.myAddress
	tx.Input.Sequence = c.sequencePerPartition[from]

	txPayload := payload.Payload(tx)
	env := txs.Enclose(from, txPayload)
	err := env.Sign(c.acc)
	if err != nil {
		return err
	}
	cli := c.clientConn[from]
	ex, err := cli.BroadcastEnvelope(env, c.scalableCoin.logger)
	debug("Executed moveTo %v from %v to %v", tx.Address, from, to)
	c.logs.Log("latencies", "%v moveTo %v %v %v\n", c.id, from, ex.Height, err == nil)
	if err != nil {
		log.Warnf("moveTo error client %v, contract %v from %v to %v", c.id, tx.Address, from, to)
		acc, err := cli.GetAccount(c.myAddress)
		if err != nil {
			log.Warnf("Error getting contract")
		}
		// Probably moveTo was already done
		if strconv.Itoa(int(acc.ShardID)) != from {
			return err
		}
		// return err
	}
	// if ex.Exception != nil {
	// 	return fmt.Errorf("Exception: %v", ex.Exception.Exception)
	// }
move2:
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
	select {
	case <-time.After(3 * time.Minute):
		log.Infof("Timeout while getting signed header")
		goto move2
		// return fmt.Errorf("Error: signed header timeout %v", c.id)

	case move2Tx := <-waitSignedHeader:
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
		cli = c.clientConn[to]
		ex, err = cli.BroadcastEnvelope(env, c.scalableCoin.logger)

		if err != nil {
			debug("Error sending move2 to %v", to)
			return err
		}
		for ex == nil {
			ex, err = cli.BroadcastEnvelope(env, c.scalableCoin.logger)
			log.Warnf("Error sending move2, retrying")
		}
		if ex.Exception != nil {
			debug("Exception sending move2")
			return fmt.Errorf("Exception: %v", ex.Exception.Exception)
		}
		c.logs.Log("latencies", "%v move2 %v %v\n", c.id, to, ex.Height)

		toInt, err := strconv.Atoi(to)
		if err != nil {
			panic("ERROR converting int")
		}

		// log.Infof("MOVED %v from %v to %v", tx.Address, from, to)
		if !static {
			c.scalableCoin.partitioning.Move(*tx.Address, int64(toInt))
			c.tokenToPartition[*tx.Address] = int64(toInt)
		}
		return nil
	}
}

func signedHeaderGetter(blockChans []chan *rpcevents.SignedHeadersResult, clients map[string][]*def.Client, getHeader chan MoveResponse) {
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
		signedBlock := selectValue.Interface().(*rpcevents.SignedHeadersResult)
		chainID := signedBlock.SignedHeader.ChainID
		debug("Got block from partition: %v %v, len: %v", signedBlock.SignedHeader.ChainID, signedBlock.SignedHeader.Height, len(blockGetHeaderMap[chainID]))
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

func (c *Client) spawnClient(ctx context.Context, experimentCtr chan chan bool) {
	running := true
	go func() {
		<-ctx.Done()
		running = false
	}()

	beginExperiment := make(chan bool)

	for contractsPerClient := 0; contractsPerClient < c.contractsPerClient; contractsPerClient++ {
		tx := c.scalableCoin.createNewAccount()
		err := c.createContract(tx, false)
		for err != nil {
			log.Warnf("[Client %v] ERROR creating initial contract: %v", c.id, err)
			tx = c.scalableCoin.createNewAccount()
			err = c.createContract(tx, false)
		}

		tx = c.scalableCoin.createNewAccount()
		err = c.createContract(tx, true)
		for err != nil {
			log.Warnf("[Client %v] ERROR creating initial static contract: %v", c.id, err)
			tx = c.scalableCoin.createNewAccount()
			err = c.createContract(tx, true)
		}
	}
	experimentCtr <- beginExperiment
	<-beginExperiment
	log.Infof("[Client %v] Begin transfering", c.id)

	for running {
		randomToken := *c.myTokens[rand.Intn(len(c.myTokens))]
		op := c.scalableCoin.GetOp(randomToken)
		if op.Name == "newAccount" {
			err := c.createContract(op.Tx, false)
			if err != nil {
				log.Warnf("[Client %v] Error creating contract %v", c.id, err)
				break
			}
		} else {
			err := c.transfer(op.Tx, op.moveToPartition)
			retry := 1
			for err != nil {
				awaitTime := time.Duration(rand.Intn(10)) * expectedBlockTime
				log.Fatalf("[Client %v] Error transfering %v, retrying in %v s", c.id, err, awaitTime)
				time.Sleep(awaitTime)
				c.scalableCoin.GetRetryOp(randomToken, op)
				err = c.transfer(op.Tx, op.moveToPartition)
				if retry > 10 {
					log.Infof("[Client %v] Gave up", c.id)
					c.logs.Log("latencies", "%v gaveUp %v\n", c.id, time.Now().UnixNano())
					break
				}
				retry++
			}
			if op.moveToPartition != 0 {
				if c.tokenToPartition[randomToken] != op.moveToPartition {
					log.Fatalf("c.tokenToPartition[randomToken] !=op.moveToPartition -> %v != %v", c.tokenToPartition[randomToken], op.moveToPartition)
				}
				c.scalableCoin.FinishCrossShard(randomToken, op.toToken, c.tokenToPartition[randomToken], op.moveToPartition)
			} else {
				c.scalableCoin.FinishSameShard(op.toToken, c.tokenToPartition[randomToken])
			}
		}
	}
}

func generateClient(wg *sync.WaitGroup, ctx context.Context, clients map[string][]*def.Client, accountID int,
	scalableCoin *ScalableCoin, logs *utils.Log, contractsPerClient int, signedHeaderCh chan MoveResponse, experimentCtr chan chan bool) {
	defer wg.Done()

	tmpAccounts := make([]*acm.PrivateAccount, 1)
	tmpAccounts[0] = acm.GeneratePrivateAccountFromSecret(strconv.Itoa(accountID + 1))
	acc := acm.SigningAccounts(tmpAccounts)[0]

	c := NewClient(accountID, clients, scalableCoin, acc, logs, contractsPerClient, signedHeaderCh)
	waitFor := (time.Duration(accountID) * time.Second) / 50
	log.Infof("Client %v waiting for %v", accountID, waitFor)
	time.Sleep(waitFor)
	c.spawnClient(ctx, experimentCtr)
	log.Warnf("Stopping client %v", c.id)
}

func main() {
	config := config.Config{}
	configFile, err := ioutil.ReadFile(os.Args[1])
	checkFatalError(err)
	err = yaml.Unmarshal(configFile, &config)
	checkFatalError(err)

	var blockChans []chan *rpcevents.SignedHeadersResult
	logs, err := utils.NewLog(config.Logs.Dir)
	defer logs.Flush()

	defaultAccount := acm.SigningAccounts([]*acm.PrivateAccount{acm.GeneratePrivateAccountFromSecret("0")})
	clients := make(map[string][]*def.Client)
	// var blockChans []chan *rpcquery.SignedHeadersResult
	// Mapping for partition to created contracts address
	for _, c := range config.Servers {
		for _, shardClient := range c.Addresses {
			clients[c.ChainID] = append(clients[c.ChainID], def.NewClient(shardClient, "", false, time.Duration(config.Benchmark.Timeout)*time.Second))
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
			log.Infof("Canceling experiment...")
			cancel()
		}
	}()

	for partition := 0; partition < int(config.Partitioning.NumberPartitions); partition++ {
		chainID := strconv.Itoa(partition + 1)

		blockChans = append(blockChans, make(chan *rpcevents.SignedHeadersResult, 50))
		go utils.ListenBlockHeaders2(chainID, clients[chainID][0], logs, blockChans[partition])
	}

	experimentCtr := make(chan chan bool)

	var wg sync.WaitGroup
	for cli := 0; cli < config.Benchmark.Clients; cli++ {
		wg.Add(1)
		go generateClient(&wg, ctx, clients, cli, scalableCoin, logs, config.Benchmark.MaximumAccounts, signedHeaderCh, experimentCtr)
	}

	go signedHeaderGetter(blockChans, clients, signedHeaderCh)

	go func() {
		var beginExperimentCh []chan bool
		for cli := 0; cli < config.Benchmark.Clients; cli++ {
			clientCh := <-experimentCtr
			beginExperimentCh = append(beginExperimentCh, clientCh)
		}

		// Send begin experiment
		for cli := 0; cli < config.Benchmark.Clients; cli++ {
			beginExperimentCh[cli] <- true
		}

		logs.Log("begin-experiment", "%d\n", time.Now().UnixNano())
		log.Infof("Beggining countdown at %v", time.Now().UnixNano())

		timer := time.NewTimer(time.Second * config.Benchmark.ExperimentTime)
		<-timer.C
		log.Infof("Finishing experiment")
		cancel()
	}()

	wg.Wait()
}
