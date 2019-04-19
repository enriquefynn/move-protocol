package main

import (
	"io/ioutil"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"time"

	"gopkg.in/cheggaaa/pb.v1"

	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/dependencies"
	"github.com/hyperledger/burrow/rpc/rpcquery"
	"github.com/hyperledger/burrow/txs"
	yaml "gopkg.in/yaml.v2"

	"github.com/hyperledger/burrow/crypto"

	"github.com/hyperledger/burrow/deploy/def"

	"github.com/enriquefynn/sharding-runner/burrow-client/config"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/logsreader"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/partitioning"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/utils"
	log "github.com/sirupsen/logrus"
)

func checkFatalError(err error) {
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func sendTxBatch(wg *sync.WaitGroup, client *def.Client, signedTxs []txs.Envelope) {
	defer wg.Done()
	_, err := client.BroadcastEnvelopeBatchAsync(signedTxs)
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
}

func clientEmitter(config *config.Config, logs *utils.Log, contractsMap []*crypto.Address, clients []*def.Client,
	logsReader *logsreader.LogsReader, blockChans []chan *rpcquery.SignedHeadersResult, readyToSentTxs []*dependencies.TxResponse, dependencyGraph *dependencies.Dependencies) {

	txStreamOpen := true
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Warn("Stopping reading transactions now")
			txStreamOpen = false
		}
	}()

	// Partitioning
	// var partitioning = partitioning.GetPartitioning(config)
	// Txs streamer
	// txsChan := logsReader.LogsLoader()
	// Mapping for kittens ids to address
	idMap := make(map[int64]*crypto.Address)

	// Number of simultaneous txs allowed
	var outstandingTxs []int
	// Signed Headers (Have to wait N + 2) to send the move2
	// [partitionID][BlockNumber][]ids to append header to
	var shouldGetSignedHeader []map[int64][]*dependencies.TxResponse
	// SentTx that are not received
	var sentTxs []map[string]*dependencies.TxResponse
	// Store freed txs to send later
	var freedTxsMaps []map[*dependencies.TxResponse]bool
	// txs to send per client

	for i := int64(0); i < config.Partitioning.NumberPartitions; i++ {
		sentTxs = append(sentTxs, make(map[string]*dependencies.TxResponse))
		shouldGetSignedHeader = append(shouldGetSignedHeader, make(map[int64][]*dependencies.TxResponse))
		freedTxsMaps = append(freedTxsMaps, make(map[*dependencies.TxResponse]bool))
		outstandingTxs = append(outstandingTxs, config.Benchmark.OutstandingTxs)
	}

	// Dependency graph
	// dependencyGraph := dependencies.NewDependencies()

	// Aux functions
	changeIdsSignAndsendTx := func(tx *dependencies.TxResponse) float64 {
		logsReader.ChangeIDsMultiShard(tx, idMap, contractsMap)
		signedTx := tx.Sign()
		// log.Infof("SENDING %v %v to %v", tx.MethodName, tx.OriginalIds, tx.ChainID)
		start := time.Now()
		_, err := clients[tx.PartitionIndex].BroadcastEnvelopeAsync(signedTx)
		// sendTx(clients[tx.PartitionIndex], signedTx)
		elapsed := time.Since(start).Seconds()
		checkFatalError(err)
		sentTxs[tx.PartitionIndex][string(signedTx.Tx.Hash())] = tx
		return elapsed
	}

	changeIdsSignAndSendTxBatch := func(txRes []*dependencies.TxResponse) float64 {
		if len(txRes) == 0 {
			return 0
		}
		start := time.Now()

		var txsEnvelopesPerClient [][]txs.Envelope
		for i := int64(0); i < config.Partitioning.NumberPartitions; i++ {
			txsEnvelopesPerClient = append(txsEnvelopesPerClient, []txs.Envelope{})
		}

		for _, tx := range txRes {
			logsReader.ChangeIDsMultiShard(tx, idMap, contractsMap)
			signedTx := tx.Sign()
			txsEnvelopesPerClient[tx.PartitionIndex] = append(txsEnvelopesPerClient[tx.PartitionIndex], *signedTx)
			sentTxs[tx.PartitionIndex][string(signedTx.Tx.Hash())] = tx
			// txsEnvelopes = append(txsEnvelopes, *signedTx)
		}
		var wg sync.WaitGroup
		// log.Infof("WAIT FOR %v", int(config.Partitioning.NumberPartitions))
		wg.Add(int(config.Partitioning.NumberPartitions))
		for i := int64(0); i < config.Partitioning.NumberPartitions; i++ {
			sendTxBatch(&wg, clients[i], txsEnvelopesPerClient[i])
		}
		wg.Wait()
		// sendTxBatch(clients[0], txsEnvelopes)
		elapsed := time.Since(start).Seconds()
		return elapsed
	}

	// First txs
	log.Infof("Sending first txs")
	sendNTxs := int(config.Partitioning.NumberPartitions) * config.Benchmark.OutstandingTxs
	if sendNTxs > len(readyToSentTxs) {
		sendNTxs = len(readyToSentTxs)
	}
	sendTxs := readyToSentTxs[:sendNTxs]
	changeIdsSignAndSendTxBatch(sendTxs)

	readyToSentTxs = readyToSentTxs[sendNTxs:]

	cases := make([]reflect.SelectCase, len(blockChans))
	for i, ch := range blockChans {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}

	movedAccounts := 0
	running := true

	for running {
		sentMoved := 0
		// Select a block from channels, get channel id and block
		partitionID, selectValue, _ := reflect.Select(cases)
		signedBlock := selectValue.Interface().(*rpcquery.SignedHeadersResult)

		// If we are waiting for a header
		if txsToMod, ok := shouldGetSignedHeader[partitionID][signedBlock.SignedHeader.Height]; ok {
			for _, txToMod := range txsToMod {
				txToMod.Tx.SignedHeader = signedBlock.SignedHeader
				// Send Tx
				changeIdsSignAndsendTx(txToMod)
				sentMoved++
				// log.Infof("Sending move2 to partition %v", txToMod.PartitionIndex+1)
			}
			delete(shouldGetSignedHeader[partitionID], signedBlock.SignedHeader.Height)
		}
		// Go trough received transactions
		for _, tx := range signedBlock.TxExecutions {
			txHash := string(tx.TxHash)
			// Found tx
			if sentTx, ok := sentTxs[partitionID][txHash]; ok {
				// log.Infof("Executing: %v %v tokenID: %v at partition %v, block height: %v", sentTx.MethodName, sentTx.OriginalIds, idMap[sentTx.OriginalIds[0]],
				// 	sentTx.PartitionIndex+1, signedBlock.SignedHeader.Height)
				if tx.Exception != nil {
					log.Fatalf("Exception happened %v executing %v %v", tx.Exception, sentTx.MethodName, sentTx.OriginalIds)
				}
				freedTxs := dependencyGraph.RemoveDependency(sentTx.OriginalIds)

				if sentTx.MethodName == "createPromoKitty" || sentTx.MethodName == "giveBirth" {
					// log.Infof("%v", tx.LogData)
					kittyID := logsReader.ExtractKittyID(tx.LogData[0])
					idMap[sentTx.OriginalBirthID] = logsReader.ExtractNewContractAddress(tx.LogData[0])
					// log.Infof("KITTY ID: %v", kittyId)
					if kittyID != sentTx.OriginalBirthID {
						// log.Warnf("IDs differ %v != %v", kittyID, sentTx.OriginalBirthID)
					}
				} else if sentTx.MethodName == "moveTo" {
					// Ids are changed from here on
					// Get proofs to partition issuing move
					toPartition := sentTx.PartitionIndex
					// log.Infof("Partition %v ADDR %v getting proof", toPartition, sentTx.Tx.Address)
					// TODO: How to parallelise this
					proofs, err := clients[toPartition].GetAccountProof(*sentTx.Tx.Address)
					checkFatalError(err)
					// Save that I need signed header
					dependencyGraph.AddFieldsToMove2(sentTx.OriginalIds[0], shouldGetSignedHeader, partitionID, proofs)
				} else if sentTx.MethodName == "move2" {
					movedAccounts++
				}

				delete(sentTxs[partitionID], txHash)
				for freedTx := range freedTxs {
					// log.Infof("Sending blocked tx: %v (%v)", freedTx.MethodName, freedTx.OriginalIds)
					// Have to wait to get the right signed header
					if freedTx.MethodName != "move2" {
						freedTxsMaps[freedTx.PartitionIndex][freedTx] = true
					}
				}
			}
		}

		outstandingTxs[partitionID] = len(signedBlock.TxExecutions)
		log.Infof("Sending %v txs", len(signedBlock.TxExecutions))
		// if len(signedBlock.TxExecutions) < outstandingTxs[partitionID] {
		// 	outstandingTxs[partitionID]--
		// } else {
		// 	outstandingTxs[partitionID]++
		// }

		dependenciesSent := 0
		streamSent := 0
		numTxsSent := len(sentTxs[partitionID])
		if numTxsSent == 0 && dependencyGraph.Length == 0 && txStreamOpen == false {
			log.Warnf("Shutting down")
			running = false
		}
		// TODO: Executed txs

		var dependenciesToSend []*dependencies.TxResponse
		for tx := range freedTxsMaps[partitionID] {
			if dependenciesSent >= outstandingTxs[partitionID] {
				break
			}
			dependenciesSent++
			// timeSpentSending += changeIdsSignAndsendTx(tx)
			dependenciesToSend = append(dependenciesToSend, tx)
			delete(freedTxsMaps[partitionID], tx)
		}
		// log.Infof("SENDING DEPS")
		changeIdsSignAndSendTxBatch(dependenciesToSend)
		var streamToSend []*dependencies.TxResponse
		for dependenciesSent+streamSent+sentMoved < outstandingTxs[partitionID] && txStreamOpen {
			if streamSent == len(readyToSentTxs) {
				txStreamOpen = false
				log.Warnf("Stop sending streamed txs")
				logs.Log("stopped-stream-txs", "%d\n", signedBlock.SignedHeader.Time.UnixNano())
				break
			}
			// txResponse, chOpen := <-txsChan
			// if !chOpen {
			// 	log.Warnf("No more txs in channel")
			// 	txStreamOpen = false
			// 	break
			// }
			// sentTxs := addToDependenciesAndSend(txResponse)
			// streamSent += sentTxs
			// timeSpentSending += changeIdsSignAndsendTx(readyToSentTxs[streamSent])
			streamToSend = append(streamToSend, readyToSentTxs[streamSent])
			streamSent++
		}
		// log.Infof("SENDING STREAM")
		changeIdsSignAndSendTxBatch(streamToSend)
		readyToSentTxs = readyToSentTxs[streamSent:]
		log.Infof("[PARTITION %v] Sending: %v: Last sentTxs: dependency: %v, stream: %v/%v dependency graph: %v, txs executed: %v",
			partitionID, outstandingTxs[partitionID], dependenciesSent, streamSent, len(readyToSentTxs), dependencyGraph.Length, len(signedBlock.TxExecutions))
		// log.Infof("Added: %v SentTxs: %v", added, len(sentTxs))
		// log.Infof("Sent this round: %v", sentTxsThisRound)
		logs.Log("moved-accounts-partition-"+signedBlock.SignedHeader.ChainID, "%d %d\n", movedAccounts, signedBlock.SignedHeader.Time.UnixNano())
		logs.Flush()
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

	log.Infof("Building dependencies in memory")
	txsChan := logsReader.LogsLoader()
	logsReader.Advance(2)
	dependencyGraph := dependencies.NewDependencies()
	var readyToSentTxs []*dependencies.TxResponse
	var partitioning = partitioning.GetPartitioning(&config)
	bar := pb.StartNew(5203957)
	for tx := range txsChan {
		readyTxs := dependencyGraph.AddDependencyWithMoves(tx, partitioning)
		readyToSentTxs = append(readyToSentTxs, readyTxs...)
		bar.Add(1)
	}
	bar.Finish()
	log.Infof("Ready to send %v txs", len(readyToSentTxs))

	for part, c := range config.Servers {
		clients = append(clients, def.NewClientWithLocalSigning(c.Address, time.Duration(config.Benchmark.Timeout)*time.Second, defaultAccount))

		// Deploy Genes contract
		geneScienceAddress, err := utils.CreateContract(c.ChainID, &config, logsReader, clients[part], config.Contracts.GenePath)
		checkFatalError(err)
		log.Infof("Deployed GeneScience at: %v", geneScienceAddress)
		// Deploy CK contract
		ckAddress, err := utils.CreateContract(c.ChainID, &config, logsReader, clients[part], config.Contracts.Path, geneScienceAddress)
		checkFatalError(err)
		log.Infof("Deployed CK in partition %v at: %v", c.ChainID, ckAddress)
		// Set CK address to contractsMap[partition]
		contractsMap = append(contractsMap, ckAddress)

		blockChans = append(blockChans, make(chan *rpcquery.SignedHeadersResult))
		go utils.ListenBlockHeaders(c.ChainID, clients[part], logs, blockChans[part])
	}

	clientEmitter(&config, logs, contractsMap, clients, logsReader, blockChans, readyToSentTxs, dependencyGraph)
}
