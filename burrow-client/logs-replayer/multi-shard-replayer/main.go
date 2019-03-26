package main

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/execution/exec"

	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/deploy/def"

	"github.com/enriquefynn/sharding-runner/burrow-client/config"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/logsreader"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/partitioning"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/utils"
	"github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
)

func checkFatalError(err error) {
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}
}

type methodAndID struct {
	method  string
	ids     []int64
	birthID int64
}

func clientEmitter(config *config.Config, client *def.Client, logsReader *logsreader.LogsReader, blockChan chan *exec.BlockExecution) {
	// Partitioning
	var partitioning = partitioning.GetPartitioning(config)
	// Txs streamer
	txsChan := logsReader.LogsLoader()
	// Mapping for kittens ids to address
	idMap := make(map[int64]crypto.Address)

	// Number of simultaneous txs allowed
	outstandingTxs := config.Benchmark.OutstandingTxs

	// SentTx that are not received
	sentTxs := make(map[string]methodAndID)

	// Dependency graph
	dependencyGraph := utils.NewDependencies()

	sendTx := func(tx *logsreader.TxResponse) {
		logsReader.ChangeIDsMultiShard(tx, idMap)
		signedTx := tx.Sign()
		_, err := client.BroadcastEnvelopeAsync(signedTx)
		checkFatalError(err)
		sentTxs[string(signedTx.Tx.Hash())] = methodAndID{
			method:  tx.MethodName,
			ids:     tx.OriginalIds,
			birthID: tx.OriginalBirthID,
		}
	}

	// First txs
	for i := 0; i < outstandingTxs; i++ {
		txResponse, chOpen := <-txsChan
		if !chOpen {
			break
		}
		// logrus.Infof("SENDING: %v", txResponse.methodName)
		shouldWait := dependencyGraph.AddDependency(txResponse)
		if !shouldWait {
			sendTx(txResponse)
		}
	}

	for {
		sentTxsThisRound := 0
		if len(sentTxs) == 0 {
			break
		}
		block := <-blockChan
		logrus.Infof("RECEIVED BLOCK %v", block.Header.Height)
		logrus.Infof("Dependencies: %v", dependencyGraph.Length)
		// dependencyGraph.bfs()
		for _, tx := range block.TxExecutions {
			txHash := string(tx.TxHash)
			// Found tx
			if sentTx, ok := sentTxs[txHash]; ok {
				// logrus.Infof("Executing: %v %v", sentTx.method, sentTx.ids)
				if tx.Exception != nil {
					logrus.Fatalf("Exception happened %v executing %v %v", tx.Exception, sentTx.method, sentTx.ids)
				}

				freedTxs := dependencyGraph.RemoveDependency(sentTx.ids)

				if sentTx.method == "createPromoKitty" || sentTx.method == "giveBirth" {
					var event *exec.Event

					for _, ev := range tx.Events {
						if ev.Log != nil {
							event = ev
							break
						}
					}
					idMap[int64(sentTx.birthID)] = logsReader.ExtractNewContractAddress(event)
				}

				delete(sentTxs, txHash)
				if len(freedTxs) != 0 {
					for freedTx := range freedTxs {
						// logrus.Infof("Sending blocked tx: %v (%v)", freedTx.methodName, freedTx.originalIds)
						sendTx(freedTx)
						sentTxsThisRound++
					}
				}
			}
		}
		added := 0
		for len(sentTxs) < outstandingTxs {
			added++
			txResponse, chOpen := <-txsChan
			if !chOpen {
				logrus.Warnf("No more txs in channel")
				break
			}
			shouldWait := dependencyGraph.AddDependency(txResponse)
			if !shouldWait {
				// logrus.Infof("Sending tx: %v (%v)", txResponse.methodName, txResponse.originalIds)
				sendTx(txResponse)
				sentTxsThisRound++
			}
		}
		logrus.Infof("Last sentTxs %v, sent this round %v, received %v", len(sentTxs), sentTxsThisRound, len(block.TxExecutions))
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

	// Chain id: 1
	logsReader := logsreader.CreateLogsReader(config.Benchmark.ChainID, config.Contracts.ReplayTransactionsPath, config.Contracts.CKABI, config.Contracts.KittyABI)

	defaultAccount := acm.SigningAccounts([]*acm.PrivateAccount{acm.GeneratePrivateAccountFromSecret("0")})
	client := def.NewClientWithLocalSigning(config.Benchmark.Address, time.Duration(config.Benchmark.Timeout)*time.Second, defaultAccount)

	// Deploy Genes contract
	address, err := utils.CreateContract(&config, logsReader, client, config.Contracts.GenePath)
	checkFatalError(err)
	logrus.Infof("Deployed GeneScience at: %v", address)

	// Deploy CK contract
	address, err = utils.CreateContract(&config, logsReader, client, config.Contracts.Path, address)
	logsReader.Advance(2)
	checkFatalError(err)
	logrus.Infof("Deployed CK at: %v", address)
	logsReader.SetContractAddr(address)

	blockChan := make(chan *exec.BlockExecution)

	go utils.ListenBlockHeaders(client, logs)
	go utils.ListenBlocks(client, blockChan)

	clientEmitter(&config, client, logsReader, blockChan)
}
