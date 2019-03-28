package main

import (
	"io/ioutil"
	"os"
	"reflect"
	"time"

	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/execution/exec"
	"github.com/hyperledger/burrow/rpc/rpcquery"
	yaml "gopkg.in/yaml.v2"

	"github.com/hyperledger/burrow/crypto"

	"github.com/hyperledger/burrow/deploy/def"

	"github.com/enriquefynn/sharding-runner/burrow-client/config"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/logsreader"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/partitioning"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/utils"
	"github.com/sirupsen/logrus"
)

func checkFatalError(err error) {
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}
}

func clientEmitter(config *config.Config, contractsMap []*crypto.Address, clients []*def.Client,
	logsReader *logsreader.LogsReader, blockChans []chan *rpcquery.SignedHeadersResult) {
	logrus.Infof("BEGIN")

	// Partitioning
	var partitioning = partitioning.GetPartitioning(config)
	// Txs streamer
	txsChan := logsReader.LogsLoader()
	// Mapping for kittens ids to address
	idMap := make(map[int64]*crypto.Address)

	// Number of simultaneous txs allowed
	outstandingTxs := config.Benchmark.OutstandingTxs

	// SentTx that are not received
	var sentTxs []map[string]*logsreader.TxResponse
	// for range blockChan {
	// 	sentTxs = append(sentTxs, make(map[string]*logsreader.TxResponse))
	// }
	sentTxs = append(sentTxs, make(map[string]*logsreader.TxResponse))
	totalOngoingTxs := 0

	// Dependency graph
	dependencyGraph := utils.NewDependencies()

	// Aux functions
	sendTx := func(tx *logsreader.TxResponse) {
		logsReader.ChangeIDsMultiShard(tx, idMap, contractsMap)
		signedTx := tx.Sign()
		_, err := clients[tx.PartitionIndex].BroadcastEnvelopeAsync(signedTx)
		checkFatalError(err)
		sentTxs[tx.PartitionIndex][string(signedTx.Tx.Hash())] = tx
	}

	addToDependenciesAndSend := func(tx *logsreader.TxResponse) bool {
		totalOngoingTxs++
		moveTxPairs := logsReader.CreateMoveDecidePartitioning(tx, partitioning, idMap)
		for _, moveTx := range moveTxPairs {
			dependencyGraph.AddDependency(moveTx)
		}
		shouldWait := dependencyGraph.AddDependency(tx)
		if !shouldWait {
			sendTx(tx)
		}
		return shouldWait
	}

	// First txs
	for i := 0; i < outstandingTxs; i++ {
		txResponse, chOpen := <-txsChan
		if !chOpen {
			break
		}
		// logrus.Infof("SENDING: %v", txResponse.MethodName)
		addToDependenciesAndSend(txResponse)
	}

	cases := make([]reflect.SelectCase, len(blockChans))
	for i, ch := range blockChans {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}

	for {
		partitionID, selectValue, _ := reflect.Select(cases)
		signedBlock := selectValue.Interface().(*rpcquery.SignedHeadersResult)

		// signedBlock := <-blockChans[partitionID]
		logrus.Infof("RECEIVED BLOCK %v from partition %v", signedBlock.SignedHeader.Height, partitionID)
		logrus.Infof("Dependencies: %v", dependencyGraph.Length)
		// dependencyGraph.bfs()
		for _, tx := range signedBlock.TxExecutions {
			txHash := string(tx.TxHash)
			// Found tx
			if sentTx, ok := sentTxs[partitionID][txHash]; ok {
				// logrus.Infof("Executing: %v %v", sentTx.MethodName, sentTx.OriginalIds)
				if tx.Exception != nil {
					logrus.Fatalf("Exception happened %v executing %v %v", tx.Exception, sentTx.MethodName, sentTx.OriginalIds)
				}

				freedTxs := dependencyGraph.RemoveDependency(sentTx.OriginalIds)

				if sentTx.MethodName == "createPromoKitty" || sentTx.MethodName == "giveBirth" {
					var event *exec.Event

					// Get the Birth event (1st not null event)
					for _, ev := range tx.Events {
						if ev.Log != nil {
							event = ev
							break
						}
					}
					idMap[int64(sentTx.OriginalBirthID)] = logsReader.ExtractNewContractAddress(event)
				}

				totalOngoingTxs--
				delete(sentTxs[partitionID], txHash)
				if len(freedTxs) != 0 {
					for freedTx := range freedTxs {
						// logrus.Infof("Sending blocked tx: %v (%v)", freedTx.methodName, freedTx.originalIds)
						sendTx(freedTx)
					}
				}
			}
		}
		added := 0
		numTxsSent := 0
		for pID := range blockChans {
			numTxsSent += len(sentTxs[pID])
		}
		for numTxsSent < outstandingTxs {
			numTxsSent++
			txResponse, chOpen := <-txsChan
			if !chOpen {
				logrus.Warnf("No more txs in channel")
				break
			}
			// Should we move this thing?
			addToDependenciesAndSend(txResponse)
		}
		logrus.Infof("Last sentTxs %v added: %v", len(sentTxs[partitionID]), added)
		// logrus.Infof("Added: %v SentTxs: %v", added, len(sentTxs))
		// logrus.Infof("Sent this round: %v", sentTxsThisRound)
	}
}

func main() {
	config := config.Config{}
	configFile, err := ioutil.ReadFile(os.Args[1])
	checkFatalError(err)
	err = yaml.Unmarshal(configFile, &config)
	checkFatalError(err)

	logs, err := utils.NewLog(config.Logs.Dir)
	checkFatalError(err)

	logsReader := logsreader.CreateLogsReader(config.Contracts.ReplayTransactionsPath, config.Contracts.CKABI, config.Contracts.KittyABI)
	defaultAccount := acm.SigningAccounts([]*acm.PrivateAccount{acm.GeneratePrivateAccountFromSecret("0")})

	// numberOfPartitions := config.Partitioning.NumberPartitions
	// Clients in partitions
	var clients []*def.Client
	var blockChans []chan *rpcquery.SignedHeadersResult
	// Mapping for partition to created contracts address
	var contractsMap []*crypto.Address

	for part, c := range config.Servers {
		clients = append(clients, def.NewClientWithLocalSigning(c.Address, time.Duration(config.Benchmark.Timeout)*time.Second, defaultAccount))

		// Deploy Genes contract
		address, err := utils.CreateContract(c.ChainID, &config, logsReader, clients[part], config.Contracts.GenePath)
		checkFatalError(err)
		logrus.Infof("Deployed GeneScience at: %v", address)
		address, err = utils.CreateContract(c.ChainID, &config, logsReader, clients[part], config.Contracts.Path, address)
		logrus.Infof("Deployed CK in partition %v at: %v", c.ChainID, address)
		// Set CK address to contractsMap[partition]
		contractsMap = append(contractsMap, address)
		logsReader.Advance(2)
		checkFatalError(err)

		// Deploy CK contract

		blockChans = append(blockChans, make(chan *rpcquery.SignedHeadersResult))
	}

	// Deploy CK contract
	go utils.ListenBlockHeaders(clients[0], logs, blockChans[0])
	clientEmitter(&config, contractsMap, clients, logsReader, blockChans)

	// config := config.Config{}
	// configFile, err := ioutil.ReadFile(os.Args[1])
	// checkFatalError(err)
	// err = yaml.Unmarshal(configFile, &config)
	// checkFatalError(err)

	// logs, err := utils.NewLog(config.Logs.Dir)
	// checkFatalError(err)

	// // Chain id: 1
	// logsReader := logsreader.CreateLogsReader(config.Contracts.ReplayTransactionsPath, config.Contracts.CKABI, config.Contracts.KittyABI)

	// defaultAccount := acm.SigningAccounts([]*acm.PrivateAccount{acm.GeneratePrivateAccountFromSecret("0")})
	// client := def.NewClientWithLocalSigning(config.Servers[0].Address, time.Duration(config.Benchmark.Timeout)*time.Second, defaultAccount)

	// // Deploy Genes contract
	// address, err := utils.CreateContract("1", &config, logsReader, client, config.Contracts.GenePath)
	// checkFatalError(err)
	// logrus.Infof("Deployed GeneScience at: %v", address)

	// // Deploy CK contract
	// address, err = utils.CreateContract("1", &config, logsReader, client, config.Contracts.Path, address)
	// logsReader.Advance(2)
	// checkFatalError(err)
	// logrus.Infof("Deployed CK at: %v", address)
	// logsReader.SetContractAddr(address)

	// blockChan := make(chan *rpcquery.SignedHeadersResult)

	// go utils.ListenBlockHeaders(client, logs, blockChan)
	// contractsMap := []*crypto.Address{address}
	// clientEmitter(&config, contractsMap, client, logsReader, blockChan)
}
