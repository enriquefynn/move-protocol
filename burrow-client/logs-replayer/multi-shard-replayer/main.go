package main

import (
	"io/ioutil"
	"os"
	"os/signal"
	"reflect"
	"time"

	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/dependencies"
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

func clientEmitter(config *config.Config, logs *utils.Log, contractsMap []*crypto.Address, clients []*def.Client,
	logsReader *logsreader.LogsReader, blockChans []chan *rpcquery.SignedHeadersResult) {

	txStreamOpen := true
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			logrus.Warn("Stopping reading transactions now")
			txStreamOpen = false
		}
	}()

	// Partitioning
	var partitioning = partitioning.GetPartitioning(config)
	// Txs streamer
	txsChan := logsReader.LogsLoader()
	// Mapping for kittens ids to address
	idMap := make(map[int64]*crypto.Address)

	// Number of simultaneous txs allowed
	outstandingTxs := config.Benchmark.OutstandingTxs

	// Signed Headers (Have to wait N + 2) to send the move2
	// [partitionID][BlockNumber][]ids to append header to
	var shouldGetSignedHeader []map[int64][]*dependencies.TxResponse
	// SentTx that are not received
	var sentTxs []map[string]*dependencies.TxResponse
	// Store freed txs to send later
	var freedTxsMaps []map[*dependencies.TxResponse]bool
	for range blockChans {
		sentTxs = append(sentTxs, make(map[string]*dependencies.TxResponse))
		shouldGetSignedHeader = append(shouldGetSignedHeader, make(map[int64][]*dependencies.TxResponse))
		freedTxsMaps = append(freedTxsMaps, make(map[*dependencies.TxResponse]bool))
	}

	// Dependency graph
	dependencyGraph := dependencies.NewDependencies()

	// Aux functions
	changeIdsSignAndsendTx := func(tx *dependencies.TxResponse) {
		logsReader.ChangeIDsMultiShard(tx, idMap, contractsMap)
		signedTx := tx.Sign()
		// logrus.Infof("SENDING %v %v to %v", tx.MethodName, tx.OriginalIds, tx.ChainID)
		_, err := clients[tx.PartitionIndex].BroadcastEnvelopeAsync(signedTx)
		checkFatalError(err)
		sentTxs[tx.PartitionIndex][string(signedTx.Tx.Hash())] = tx
	}

	addToDependenciesAndSend := func(tx *dependencies.TxResponse) int {
		txsToSend := dependencyGraph.AddDependencyWithMoves(tx, partitioning)
		for _, tx := range txsToSend {
			changeIdsSignAndsendTx(tx)
		}
		return len(txsToSend)
	}

	// First txs
	logrus.Infof("Sending first txs")
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

	movedAccounts := 0
	running := true

	for running {
		// Select a block from channels, get channel id and block
		logrus.Infof("Awaiting for block")
		partitionID, selectValue, _ := reflect.Select(cases)
		signedBlock := selectValue.Interface().(*rpcquery.SignedHeadersResult)

		// If we are waiting for a header
		if txsToMod, ok := shouldGetSignedHeader[partitionID][signedBlock.SignedHeader.Height]; ok {
			for _, txToMod := range txsToMod {
				txToMod.Tx.SignedHeader = signedBlock.SignedHeader
				// Send Tx
				changeIdsSignAndsendTx(txToMod)
				// logrus.Infof("Sending move2 to partition %v", txToMod.PartitionIndex+1)
			}
			delete(shouldGetSignedHeader[partitionID], signedBlock.SignedHeader.Height)
		}
		// Go trough received transactions
		for _, tx := range signedBlock.TxExecutions {
			txHash := string(tx.TxHash)
			// Found tx
			if sentTx, ok := sentTxs[partitionID][txHash]; ok {
				// logrus.Infof("Executing: %v %v tokenID: %v at partition %v, block height: %v", sentTx.MethodName, sentTx.OriginalIds, idMap[sentTx.OriginalIds[0]],
				// sentTx.PartitionIndex+1, signedBlock.SignedHeader.Height)
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
					kittyID := logsReader.ExtractKittyID(event)
					idMap[sentTx.OriginalBirthID] = logsReader.ExtractNewContractAddress(event)
					// logrus.Infof("KITTY ID: %v", kittyId)
					if kittyID != sentTx.OriginalBirthID {
						// logrus.Warnf("IDs differ %v != %v", kittyID, sentTx.OriginalBirthID)
					}
				} else if sentTx.MethodName == "moveTo" {
					// Ids are changed from here on
					// Get proofs to partition issuing move
					toPartition := sentTx.PartitionIndex
					// logrus.Infof("Partition %v ADDR %v getting proof", toPartition, sentTx.Tx.Address)
					// TODO: How to parallelise this
					proofs, err := clients[toPartition].GetAccountProof(*sentTx.Tx.Address)
					checkFatalError(err)
					// Save that I need signed header
					dependencyGraph.AddFieldsToMove2(sentTx.OriginalIds[0], shouldGetSignedHeader, partitionID, signedBlock.SignedHeader.Height, proofs)
				} else if sentTx.MethodName == "move2" {
					movedAccounts++
				}

				delete(sentTxs[partitionID], txHash)
				if len(freedTxs) != 0 {
					for freedTx := range freedTxs {
						// logrus.Infof("Sending blocked tx: %v (%v)", freedTx.MethodName, freedTx.OriginalIds)
						// Have to wait to get the right signed header
						if freedTx.MethodName != "move2" {
							// changeIdsSignAndsendTx(freedTx)
							freedTxsMaps[freedTx.PartitionIndex][freedTx] = true
						}
					}
				}
			}
		}

		if len(signedBlock.TxExecutions) < outstandingTxs {
			outstandingTxs--
		} else {
			outstandingTxs++
		}

		dependenciesSent := 0
		streamSent := 0
		numTxsSent := len(sentTxs[partitionID])
		if numTxsSent == 0 && dependencyGraph.Length == 0 && txStreamOpen == false {
			logrus.Warnf("Shutting down")
			running = false
		}
		// TODO: Executed txs

		for tx := range freedTxsMaps[partitionID] {
			if dependenciesSent >= outstandingTxs {
				break
			}
			dependenciesSent++
			changeIdsSignAndsendTx(tx)
			delete(freedTxsMaps[partitionID], tx)
		}

		for dependenciesSent+streamSent < outstandingTxs && txStreamOpen {
			txResponse, chOpen := <-txsChan
			if !chOpen {
				logrus.Warnf("No more txs in channel")
				txStreamOpen = false
				break
			}
			sentTxs := addToDependenciesAndSend(txResponse)
			streamSent += sentTxs
		}
		logrus.Infof("[PARTITION %v] Sending: %v: Last sentTxs: dependency: %v, stream: %v dependency graph: %v, txs executed: %v",
			partitionID, outstandingTxs, dependenciesSent, streamSent, dependencyGraph.Length, len(signedBlock.TxExecutions))
		// logrus.Infof("Added: %v SentTxs: %v", added, len(sentTxs))
		// logrus.Infof("Sent this round: %v", sentTxsThisRound)
		logs.Log("moved-accounts-partition-"+signedBlock.SignedHeader.ChainID, "%d %d\n", movedAccounts, signedBlock.SignedHeader.Time.UnixNano())
		go logs.Flush()
		movedAccounts = 0
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
		geneScienceAddress, err := utils.CreateContract(c.ChainID, &config, logsReader, clients[part], config.Contracts.GenePath)
		checkFatalError(err)
		logrus.Infof("Deployed GeneScience at: %v", geneScienceAddress)
		// Deploy CK contract
		ckAddress, err := utils.CreateContract(c.ChainID, &config, logsReader, clients[part], config.Contracts.Path, geneScienceAddress)
		checkFatalError(err)
		logrus.Infof("Deployed CK in partition %v at: %v", c.ChainID, ckAddress)
		// Set CK address to contractsMap[partition]
		contractsMap = append(contractsMap, ckAddress)

		blockChans = append(blockChans, make(chan *rpcquery.SignedHeadersResult))
		go utils.ListenBlockHeaders(c.ChainID, clients[part], logs, blockChans[part])
	}
	logsReader.Advance(2)

	clientEmitter(&config, logs, contractsMap, clients, logsReader, blockChans)
}
