package main

import (
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"strconv"
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

func sendTxToClient(wg *sync.WaitGroup, client *def.Client, signedTxs []*txs.Envelope) {
	defer wg.Done()
	for _, tx := range signedTxs {
		_, err := client.BroadcastEnvelopeAsync(tx)
		if err != nil {
			log.Fatalf("ERROR: %v", err)
		}
	}
}

func sendTxBatch(wg *sync.WaitGroup, clients []*def.Client, signedTxs []*txs.Envelope) {
	defer wg.Done()
	// _, err := client.BroadcastEnvelopeBatchAsync(signedTxs)
	txsPerClient := make(map[crypto.Address][]*txs.Envelope)
	for _, tx := range signedTxs {
		txsPerClient[tx.Tx.GetInputs()[0].Address] = append(txsPerClient[tx.Tx.GetInputs()[0].Address], tx)
	}
	var wg2 sync.WaitGroup
	wg2.Add(len(txsPerClient))

	for cli := range txsPerClient {
		randomClient := rand.Intn(len(clients))
		go sendTxToClient(&wg2, clients[randomClient], txsPerClient[cli])
		// for _, tx := range txsPerClient[cli] {
		// 	_, err := clients[randomClient].BroadcastEnvelopeAsync(&tx)
		// 	if err != nil {
		// 		log.Warnf("ERROR: %v", err)
		// 	}
		// }
	}
	wg2.Wait()

	// for _, tx := range signedTxs {
	// 	_, err := clients[0].BroadcastEnvelopeAsync(tx)
	// 	if err != nil {
	// 		log.Fatalf("ERROR: %v", err)
	// 	}
	// }
}

func clientEmitter(config *config.Config, logs *utils.Log, contractsMap []*crypto.Address, clients map[string][]*def.Client,
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
			// log.Infof("SENDING %v %v to %v, from %v, seq: %v", tx.MethodName, tx.OriginalIds, tx.ChainID, tx.Tx.Input.Address, tx.Tx.Input.Sequence)
			// for _, id := range tx.OriginalIds {
			// 	log.Infof("IDS: %v : %v", id, idMap[id])
			// }
			txsPerPartition[tx.ChainID] = append(txsPerPartition[tx.ChainID], signedTx)
			sentTxs[tx.PartitionIndex][string(signedTx.Tx.Hash())] = tx
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
	log.Infof("Sending first txs")
	sendNTxs := int(config.Partitioning.NumberPartitions) * config.Benchmark.OutstandingTxs
	if sendNTxs > len(readyToSentTxs) {
		sendNTxs = len(readyToSentTxs)
	}
	timeTook := changeIdsSignAndSendTxBatch(readyToSentTxs[:sendNTxs])
	log.Infof("TOOK: %v", timeTook)
	readyToSentTxs = readyToSentTxs[sendNTxs:]
	cases := make([]reflect.SelectCase, len(blockChans))
	for i, ch := range blockChans {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}

	movedAccounts := 0
	running := true

	for running {
		var sendTxs []*dependencies.TxResponse
		sentMoved := 0
		// Select a block from channels, get channel id and block
		partitionID, selectValue, _ := reflect.Select(cases)
		signedBlock := selectValue.Interface().(*rpcquery.SignedHeadersResult)

		// If we are waiting for a header
		if txsToMod, ok := shouldGetSignedHeader[partitionID][signedBlock.SignedHeader.Height]; ok {
			for _, txToMod := range txsToMod {
				txToMod.Tx.SignedHeader = signedBlock.SignedHeader
				// Send Tx
				// changeIdsSignAndsendTx(txToMod)
				sendTxs = append(sendTxs, txToMod)
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
		if outstandingTxs[partitionID] > config.Benchmark.OutstandingTxs {
			outstandingTxs[partitionID] = config.Benchmark.OutstandingTxs
		}
		// outstandingTxs[partitionID] = config.Benchmark.OutstandingTxs
		log.Infof("Sending %v txs", len(signedBlock.TxExecutions))

		dependenciesSent := 0
		streamSent := 0
		numTxsSent := len(sentTxs[partitionID])
		if numTxsSent == 0 && dependencyGraph.Length == 0 && txStreamOpen == false {
			log.Warnf("Shutting down")
			running = false
		}

		for tx := range freedTxsMaps[partitionID] {
			if dependenciesSent >= outstandingTxs[partitionID] {
				break
			}
			dependenciesSent++
			// timeSpentSending += changeIdsSignAndsendTx(tx)
			sendTxs = append(sendTxs, tx)
			delete(freedTxsMaps[partitionID], tx)
		}
		// var streamToSend []*dependencies.TxResponse
		for dependenciesSent+streamSent+sentMoved < outstandingTxs[partitionID] && txStreamOpen {
			if streamSent == len(readyToSentTxs) {
				txStreamOpen = false
				log.Warnf("Stop sending streamed txs")
				logs.Log("stopped-stream-txs", "%d\n", signedBlock.SignedHeader.Time.UnixNano())
				break
			}
			sendTxs = append(sendTxs, readyToSentTxs[streamSent])
			streamSent++
		}
		timeTaken := changeIdsSignAndSendTxBatch(sendTxs)
		log.Infof("TOOK: %v", timeTaken)
		readyToSentTxs = readyToSentTxs[streamSent:]
		log.Infof("[PARTITION %v] Sending: %v: Last sentTxs: dependency: %v, stream: %v/%v dependency graph: %v, txs executed: %v",
			partitionID, outstandingTxs[partitionID], dependenciesSent, streamSent, len(readyToSentTxs), dependencyGraph.Length, len(signedBlock.TxExecutions))
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
	clients := make(map[string][]*def.Client)
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
		for _, shardClient := range c.Addresses {
			clients[c.ChainID] = append(clients[c.ChainID], def.NewClientWithLocalSigning(shardClient, time.Duration(config.Benchmark.Timeout)*time.Second, defaultAccount))
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

		blockChans = append(blockChans, make(chan *rpcquery.SignedHeadersResult))
		go utils.ListenBlockHeaders(c.ChainID, clients[c.ChainID][0], logs, blockChans[part])
	}

	clientEmitter(&config, logs, contractsMap, clients, logsReader, blockChans, readyToSentTxs, dependencyGraph)
}
