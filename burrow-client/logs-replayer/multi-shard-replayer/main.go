package main

import (
	"io/ioutil"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/hyperledger/burrow/dependencies"
	"github.com/hyperledger/burrow/rpc/rpcevents"
	"github.com/hyperledger/burrow/txs"
	"gopkg.in/cheggaaa/pb.v1"
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

func sendTxToClient(wg *sync.WaitGroup, client *def.Client, signedTxs []*txs.Envelope) {
	defer wg.Done()
	client.BroadcastEnvelopeBatchAsync(signedTxs)
}

func sendTxBatch(wg *sync.WaitGroup, clients []*def.Client, signedTxs []*txs.Envelope) {
	defer wg.Done()
	// _, err := client.BroadcastEnvelopeBatchAsync(signedTxs)
	// txsPerClient := make(map[crypto.Address][]*txs.Envelope)
	// for _, tx := range signedTxs {
	// 	txsPerClient[tx.Tx.GetInputs()[0].Address] = append(txsPerClient[tx.Tx.GetInputs()[0].Address], tx)
	// }
	// var wg2 sync.WaitGroup
	// wg2.Add(len(txsPerClient))

	// for cli := range txsPerClient {
	// 	randomClient := rand.Intn(len(clients))
	// 	go sendTxToClient(&wg2, clients[randomClient], txsPerClient[cli])
	// }
	// wg2.Wait()
	clients[0].BroadcastEnvelopeBatchAsync(signedTxs)
}

func clientEmitter(config *config.Config, logs *utils.Log, contractsMap []*crypto.Address,
	clients map[string][]*def.Client, logsReader *logsreader.LogsReader, blockChans []chan *rpcevents.SignedHeadersResult,
	readyToSendTxs []*dependencies.TxResponse, dependencyGraph *dependencies.Dependencies) {
	defer logs.Flush()

	running := true
	txStreamOpen := true
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Warn("Stopping reading transactions now")
			running = false
		}
	}()

	// Mapping for kittens ids to address
	idMap := make(map[int64]*crypto.Address)

	// Number of simultaneous txs allowed
	var outstandingTxs []int
	var surplusTxs []int
	// Signed Headers (Have to wait N + 2) to send the move2
	// [partitionID][BlockNumber][]ids to append header to
	var shouldGetSignedHeader []map[int64][]*dependencies.TxResponse
	// SentTx that are not received
	var sentTxs []map[string]*dependencies.TxResponse
	// Store freed txs to send later
	var freedTxsMaps []map[*dependencies.TxResponse]bool

	for i := int64(0); i < config.Partitioning.NumberPartitions; i++ {
		sentTxs = append(sentTxs, make(map[string]*dependencies.TxResponse))
		shouldGetSignedHeader = append(shouldGetSignedHeader, make(map[int64][]*dependencies.TxResponse))
		freedTxsMaps = append(freedTxsMaps, make(map[*dependencies.TxResponse]bool))
		outstandingTxs = append(outstandingTxs, config.Benchmark.OutstandingTxs)
		surplusTxs = append(surplusTxs, 0)
	}
	latencyLog := utils.NewLatencyLog()

	changeIdsSignAndSendTxBatch := func(txRes []*dependencies.TxResponse) float64 {
		if len(txRes) == 0 {
			return 0
		}
		start := time.Now()

		var wg sync.WaitGroup
		wg.Add(int(config.Partitioning.NumberPartitions))

		txsPerPartition := make(map[string][]*txs.Envelope)

		for _, tx := range txRes {
			logsReader.ChangeIDsMultiShard(tx, idMap, contractsMap)
			signedTx := tx.Sign()

			txHash := string(signedTx.Tx.Hash())
			txsPerPartition[tx.ChainID] = append(txsPerPartition[tx.ChainID], signedTx)
			sentTxs[tx.PartitionIndex][txHash] = tx
			latencyLog.Add(txHash, tx, start.UnixNano())
			// log.Infof("SENDING TX FROM %v, seq: %v, part: %v %v %v", signedTx.Tx.GetInputs()[0].Address, signedTx.Tx.GetInputs()[0].Sequence, tx.PartitionIndex, tx.MethodName, tx.OriginalIds)
		}

		for i := 1; i <= int(config.Partitioning.NumberPartitions); i++ {
			partition := strconv.Itoa(i)
			go sendTxBatch(&wg, clients[partition], txsPerPartition[partition])
		}
		wg.Wait()
		elapsed := time.Since(start).Seconds()
		return elapsed
	}

	// First txs
	log.Infof("Sending %v first txs", int(config.Partitioning.NumberPartitions)*config.Benchmark.OutstandingTxs)
	sendNTxs := int(config.Partitioning.NumberPartitions) * config.Benchmark.OutstandingTxs
	if sendNTxs > len(readyToSendTxs) {
		sendNTxs = len(readyToSendTxs)
	}
	timeTook := changeIdsSignAndSendTxBatch(readyToSendTxs[:sendNTxs])
	log.Infof("TOOK: %v", timeTook)
	readyToSendTxs = readyToSendTxs[sendNTxs:]
	cases := make([]reflect.SelectCase, len(blockChans))
	for i, ch := range blockChans {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}

	// txs to send per partition
	experimentStart := time.Now()

	moved2TxsToAdd := make(map[int][]*dependencies.TxResponse)

	for running {
		// var sendTxs []*dependencies.TxResponse
		// Clean some stuff
		var sendTxsPerPartition []*dependencies.TxResponse

		moveToExecuted := 0
		move2Executed := 0

		// Select a block from channels, get channel id and block
		partitionID, selectValue, _ := reflect.Select(cases)
		signedBlock := selectValue.Interface().(*rpcevents.SignedHeadersResult)

		for _, tx := range moved2TxsToAdd[partitionID] {
			sendTxsPerPartition = append(sendTxsPerPartition, tx)
		}

		delete(moved2TxsToAdd, partitionID)

		// If we are waiting for a header
		if txsToMod, ok := shouldGetSignedHeader[partitionID][signedBlock.SignedHeader.Height]; ok {
			for _, txToMod := range txsToMod {
				txToMod.Tx.SignedHeader = signedBlock.SignedHeader
				// Send Tx
				// sendTxsPerPartition = append(sendTxsPerPartition, txToMod)
				moved2TxsToAdd[txToMod.PartitionIndex] = append(moved2TxsToAdd[txToMod.PartitionIndex], txToMod)
				// log.Infof("Sending move2 to partition %v", txToMod.PartitionIndex+1)
			}
			delete(shouldGetSignedHeader[partitionID], signedBlock.SignedHeader.Height)
		}
		timeGotBlockAt := time.Now().UnixNano()
		// Go trough received transactions
		for _, tx := range signedBlock.TxExecutions {
			txHash := string(tx.TxHash)
			// Found tx
			if sentTx, ok := sentTxs[partitionID][txHash]; ok {
				latencyLog.Remove(txHash, sentTx, logs, timeGotBlockAt)
				// log.Infof("Executing: %v %v tokenID: %v at partition %v, block height: %v", sentTx.MethodName, sentTx.OriginalIds, idMap[sentTx.OriginalIds[0]],
				// 	sentTx.PartitionIndex+1, signedBlock.SignedHeader.Height)
				if tx.Exception != nil {
					log.Warnf("Exception happened %v executing %v %v", tx.Exception, sentTx.MethodName, sentTx.OriginalIds)
				}
				freedTxs := dependencyGraph.RemoveDependency(sentTx.OriginalIds)

				if sentTx.MethodName == "createPromoKitty" || sentTx.MethodName == "giveBirth" {
					// log.Infof("%v", tx.LogData)
					if len(tx.LogData) == 0 {
						log.Warnf("No log came in tx %v %v", sentTx.MethodName, sentTx.OriginalIds)
					} else {
						kittyID := logsReader.ExtractKittyID(tx.LogData[0])
						idMap[sentTx.OriginalBirthID] = logsReader.ExtractNewContractAddress(tx.LogData[0])
						// log.Infof("KITTY ID: %v", kittyId)
						if kittyID != sentTx.OriginalBirthID {
							// log.Warnf("IDs differ %v != %v", kittyID, sentTx.OriginalBirthID)
						}
					}
				} else if sentTx.MethodName == "moveTo" {
					// Ids are changed from here on
					// Get proofs to partition issuing move
					toPartition := sentTx.ChainID
					// log.Infof("Partition %v ADDR %v getting proof", toPartition, sentTx.Tx.Address)
					// TODO: How to parallelise this
					proofs, err := clients[toPartition][0].GetAccountProof(*sentTx.Tx.Address)
					// log.Infof("GOT things for account %v %v %v", sentTx.Tx.Address, proofs.AccountProof.Version, proofs.StorageProof.Version)
					checkFatalError(err)
					// Save that I need signed header
					dependencyGraph.AddFieldsToMove2(sentTx.OriginalIds[0], shouldGetSignedHeader, partitionID, proofs)
					moveToExecuted++
				} else if sentTx.MethodName == "move2" {
					move2Executed++
				}

				delete(sentTxs[partitionID], txHash)
				for freedTx := range freedTxs {
					// log.Infof("Sending blocked tx: %v (%v)", freedTx.MethodName, freedTx.OriginalIds)
					// Have to wait to get the right signed header
					if freedTx.MethodName != "move2" {
						freedTxsMaps[freedTx.PartitionIndex][freedTx] = true
					}
				}
			} else {
				log.Warnf("TX NOT SENT BUT RECEIVED!")
			}
		}

		outstandingTxs[partitionID] = len(signedBlock.TxExecutions) + surplusTxs[partitionID]
		surplusTxs[partitionID] = 0
		if outstandingTxs[partitionID] > config.Benchmark.OutstandingTxs {
			outstandingTxs[partitionID] = config.Benchmark.OutstandingTxs
		}
		// outstandingTxs[partitionID] = config.Benchmark.OutstandingTxs
		log.Infof("Sending %v txs", len(signedBlock.TxExecutions))

		// dependenciesSent := 0
		streamSent := 0
		if len(sentTxs[partitionID]) == 0 && dependencyGraph.Length == 0 && txStreamOpen == false {
			log.Warnf("Shutting down")
			running = false
		}

		dependenciesSent := 0
		freedTxsMapsLen := len(freedTxsMaps[partitionID])
		for tx := range freedTxsMaps[partitionID] {
			if len(sendTxsPerPartition) >= outstandingTxs[partitionID] {
				break
			}
			dependenciesSent++
			// timeSpentSending += changeIdsSignAndsendTx(tx)
			sendTxsPerPartition = append(sendTxsPerPartition, tx)
			delete(freedTxsMaps[partitionID], tx)
		}
		// var streamToSend []*dependencies.TxResponse
		streamTry := 0
		for (len(sendTxsPerPartition) < outstandingTxs[partitionID]) && txStreamOpen {
			if len(readyToSendTxs) == 0 {
				txStreamOpen = false
				log.Warnf("Stop sending streamed txs")
				logs.Log("stopped-tx-stream", "%d\n", signedBlock.SignedHeader.Time.UnixNano())
				// logs.Log("stopped-stream-txs", "%d\n", signedBlock.SignedHeader.Time.UnixNano())
				break
			}
			if streamTry >= len(readyToSendTxs) {
				log.Warnf("Stop sending stream tx for partition %v", partitionID)
				logs.Log("stopped-tx-stream-partition-"+signedBlock.SignedHeader.ChainID, "%d\n", signedBlock.SignedHeader.Time.UnixNano())
				break
			}
			// Found a tx to send
			if readyToSendTxs[streamTry].PartitionIndex == partitionID {
				sendTxsPerPartition = append(sendTxsPerPartition, readyToSendTxs[streamTry])
				streamSent++
				readyToSendTxs = append(readyToSendTxs[:streamTry], readyToSendTxs[streamTry+1:]...)
			}
			streamTry++
		}
		timeTaken := changeIdsSignAndSendTxBatch(sendTxsPerPartition)
		surplusTxs[partitionID] = config.Benchmark.OutstandingTxs - len(sentTxs[partitionID])
		if surplusTxs[partitionID] < 0 {
			surplusTxs[partitionID] = 0
		}
		log.Infof("Sending in fact: %v", len(sendTxsPerPartition))
		log.Infof("TOOK: %v", timeTaken)
		// readyToSendTxs = readyToSendTxs[streamSent:]
		log.Infof("[PARTITION %v] Sending: %v, dependency: %v/%v, stream: %v/%v surplus: %v, dependency graph: %v, txs executed: %v, timestamp: %v",
			partitionID, outstandingTxs[partitionID], dependenciesSent, freedTxsMapsLen, streamSent, len(readyToSendTxs),
			surplusTxs[partitionID], dependencyGraph.Length, len(signedBlock.TxExecutions), signedBlock.SignedHeader.Time.UnixNano())
		logs.Log("movedTo-moved2-partition-"+signedBlock.SignedHeader.ChainID, "%d %d %d\n", moveToExecuted, move2Executed, signedBlock.SignedHeader.Time.UnixNano())
		// logs.Flush()

		if time.Since(experimentStart).Seconds() > (config.Benchmark.ExperimentTime * time.Second).Seconds() {
			log.Warnf("Stopping experiment after %v hours", time.Since(experimentStart).Hours())
			running = false
		}
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

	// numberOfPartitions := config.Partitioning.NumberPartitions
	// Clients in partitions
	clients := make(map[string][]*def.Client)
	var blockChans []chan *rpcevents.SignedHeadersResult
	// Mapping for partition to created contracts address
	var contractsMap []*crypto.Address

	log.Infof("Building dependencies in memory")
	txsChan := logsReader.LogsLoader()
	logsReader.Advance(2)
	dependencyGraph := dependencies.NewDependencies()
	var readyToSendTxs []*dependencies.TxResponse
	var partitioning = partitioning.GetPartitioning(&config)
	bar := pb.StartNew(5203957)

	g := NewGraph()
	for tx := range txsChan {
		g.AddEdge(tx.OriginalIds)
		readyTxs := dependencyGraph.AddDependencyWithMoves(tx, partitioning)
		readyToSendTxs = append(readyToSendTxs, readyTxs...)
		bar.Add(1)
	}
	g.MetisWrite()
	bar.Finish()
	log.Infof("Ready to send %v txs", len(readyToSendTxs))

	for part, c := range config.Servers {
		for _, shardClient := range c.Addresses {
			clients[c.ChainID] = append(clients[c.ChainID], def.NewClient(shardClient, "", false, time.Duration(config.Benchmark.Timeout)*time.Second))
		}
		// Deploy Genes contract
		geneScienceAddress, err := utils.CreateContract(c.ChainID, &config, logsReader, clients[c.ChainID][0], config.Contracts.GenePath)
		checkFatalError(err)
		log.Infof("Deployed GeneScience at: %v", geneScienceAddress)
		// Deploy CK contract
		ckAddress, err := utils.CreateContract(c.ChainID, &config, logsReader, clients[c.ChainID][0], config.Contracts.Path, geneScienceAddress)
		checkFatalError(err)
		log.Infof("Deployed CK in partition %v at: %v", c.ChainID, ckAddress)
		// Set CK address to contractsMap[partition]
		contractsMap = append(contractsMap, ckAddress)

		blockChans = append(blockChans, make(chan *rpcevents.SignedHeadersResult))
		go utils.ListenBlockHeaders(c.ChainID, clients[c.ChainID][0], logs, blockChans[part])
	}

	clientEmitter(&config, logs, contractsMap, clients, logsReader, blockChans, readyToSendTxs, dependencyGraph)
}
