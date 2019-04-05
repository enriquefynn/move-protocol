package main

import (
	"io/ioutil"
	"os"
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

func clientEmitter(config *config.Config, contractsMap []*crypto.Address, clients []*def.Client,
	logsReader *logsreader.LogsReader, blockChans []chan *rpcquery.SignedHeadersResult) {

	txStreamOpen := true
	// Partitioning
	var partitioning = partitioning.GetPartitioning(config)
	// Txs streamer
	txsChan := logsReader.LogsLoader()
	// Mapping for partition -> kittens ids to address
	idMap := make(map[int64]*crypto.Address)

	// Number of simultaneous txs allowed
	outstandingTxs := config.Benchmark.OutstandingTxs

	// Signed Headers (Have to wait N + 2) to send the move2
	// [partitionID][BlockNumber][]ids to append header to
	var shouldGetSignedHeader []map[int64][]*dependencies.TxResponse
	// SentTx that are not received
	var sentTxs []map[string]*dependencies.TxResponse
	for range blockChans {
		sentTxs = append(sentTxs, make(map[string]*dependencies.TxResponse))
		shouldGetSignedHeader = append(shouldGetSignedHeader, make(map[int64][]*dependencies.TxResponse))
	}
	// sentTxs = append(sentTxs, make(map[string]*logsreader.TxResponse))
	totalOngoingTxs := 0

	// Dependency graph
	dependencyGraph := dependencies.NewDependencies()

	// Aux functions
	changeIdsSignAndsendTx := func(tx *dependencies.TxResponse) {
		logsReader.ChangeIDsMultiShard(tx, idMap, contractsMap)
		signedTx := tx.Sign()
		logrus.Infof("SENDING %v to %v", tx.MethodName, tx.ChainID)
		_, err := clients[tx.PartitionIndex].BroadcastEnvelopeAsync(signedTx)
		checkFatalError(err)
		sentTxs[tx.PartitionIndex][string(signedTx.Tx.Hash())] = tx
	}

	addToDependenciesAndSend := func(tx *dependencies.TxResponse) bool {
		totalOngoingTxs++
		moveTxPairs := logsReader.CreateMoveDecidePartitioning(tx, partitioning)
		if len(moveTxPairs) > 0 && len(moveTxPairs) != 2 {
			logrus.Fatal("Move should execute in 2 steps")
		}
		for _, moveTx := range moveTxPairs {
			dependencyGraph.AddDependency(moveTx)
		}
		shouldWait := dependencyGraph.AddDependency(tx)
		if !shouldWait {
			changeIdsSignAndsendTx(tx)
		}
		return shouldWait
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

	running := true
	for running {
		// Select a block from channels, get channel id and block
		logrus.Infof("Awaiting for block")
		partitionID, selectValue, _ := reflect.Select(cases)
		signedBlock := selectValue.Interface().(*rpcquery.SignedHeadersResult)

		// If we are waiting for a header
		for pp, p := range shouldGetSignedHeader {
			for hh, h := range p {
				logrus.Infof("Send to part %v height: %v: %v", pp, hh, h)
			}
		}
		if txsToMod, ok := shouldGetSignedHeader[partitionID][signedBlock.SignedHeader.Height]; ok {
			for _, txToMod := range txsToMod {
				txToMod.Tx.SignedHeader = signedBlock.SignedHeader
				// Send Tx
				logrus.Infof("Sending move2 tx")
				changeIdsSignAndsendTx(txToMod)
			}
			delete(shouldGetSignedHeader[partitionID], signedBlock.SignedHeader.Height)
		}
		// Go trough received transactions
		for _, tx := range signedBlock.TxExecutions {
			txHash := string(tx.TxHash)
			// Found tx
			if sentTx, ok := sentTxs[partitionID][txHash]; ok {
				logrus.Infof("Executing: %v %v at partition %v, block height: %v", sentTx.MethodName, sentTx.OriginalIds, sentTx.PartitionIndex, signedBlock.SignedHeader.Height)
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
						logrus.Warnf("IDs differ %v != %v", kittyID, sentTx.OriginalBirthID)
					}
				} else if sentTx.MethodName == "moveTo" {
					// Ids are changed from here on
					// Get proofs to partition issuing move
					toPartition := sentTx.PartitionIndex
					logrus.Infof("Partition %v ADDR %v getting proof", toPartition, sentTx.Tx.Address)
					// TODO: How to parallelise this
					proofs, err := clients[toPartition].GetAccountProof(*sentTx.Tx.Address)
					checkFatalError(err)
					// Save that I need signed header
					go dependencyGraph.AddFieldsToMove2(sentTx.OriginalIds[0], shouldGetSignedHeader, partitionID, signedBlock.SignedHeader.Height, proofs)
				}

				totalOngoingTxs--
				delete(sentTxs[partitionID], txHash)
				if len(freedTxs) != 0 {
					for freedTx := range freedTxs {
						// logrus.Infof("Sending blocked tx: %v (%v)", freedTx.MethodName, freedTx.OriginalIds)
						// Have to wait to get the right signed header
						if freedTx.MethodName != "move2" {
							changeIdsSignAndsendTx(freedTx)
						}
					}
				}
			}
		}
		added := 0
		numTxsSent := 0
		for pID := range blockChans {
			numTxsSent += len(sentTxs[pID])
		}
		if numTxsSent == 0 && dependencyGraph.Length == 0 {
			running = false
		}
		for numTxsSent < outstandingTxs && txStreamOpen {
			numTxsSent++
			txResponse, chOpen := <-txsChan
			if !chOpen {
				logrus.Warnf("No more txs in channel")
				txStreamOpen = false
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

	clientEmitter(&config, contractsMap, clients, logsReader, blockChans)
}
