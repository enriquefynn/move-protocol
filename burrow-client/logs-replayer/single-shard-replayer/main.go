package main

import (
	"io/ioutil"
	"os"
	"os/signal"
	"time"

	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/dependencies"
	"github.com/hyperledger/burrow/rpc/rpcquery"

	"github.com/enriquefynn/sharding-runner/burrow-client/config"
	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/deploy/def"

	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/logsreader"
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

func clientEmitter(config *config.Config, logs *utils.Log, contract *crypto.Address, client *def.Client,
	logsReader *logsreader.LogsReader, blockChan chan *rpcquery.SignedHeadersResult) {

	running := true
	txStreamOpen := true
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			logrus.Warn("Stopping reading transactions now")
			txStreamOpen = false
		}
	}()

	// Txs streamer
	txsChan := logsReader.LogsLoader()
	// Mapping for kittens ids
	idMap := make(map[int64]int64)

	// Number of simultaneous txs allowed
	outstandingTxs := config.Benchmark.OutstandingTxs

	// SentTx that are not received
	sentTxs := make(map[string]methodAndID)

	// Dependency graph
	dependencyGraph := dependencies.NewDependencies()

	sendTx := func(tx *dependencies.TxResponse) {
		logsReader.ChangeIDs(tx, idMap)
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
	freedTxsMap := make(map[*dependencies.TxResponse]bool)

	for running {
		block := <-blockChan
		logrus.Infof("RECEIVED BLOCK %v", block.SignedHeader.Height)
		logrus.Infof("Dependencies: %v", dependencyGraph.Length)
		// dependencyGraph.bfs()
		executed := 0
		for _, tx := range block.TxExecutions {
			txHash := string(tx.TxHash)
			// Found tx
			if sentTx, ok := sentTxs[txHash]; ok {
				executed++
				if tx.Exception != nil {
					logrus.Fatalf("Exception happened %v executing %v %v", tx.Exception, sentTx.method, sentTx.ids)
				}

				// logrus.Infof("Executed: %v %v", sentTx.method, sentTx.ids)
				freedTxs := dependencyGraph.RemoveDependency(sentTx.ids)

				if sentTx.method == "createPromoKitty" || sentTx.method == "giveBirth" {
					idMap[int64(sentTx.birthID)] = logsReader.ExtractIDTransfer(tx.Events[1])
				}

				delete(sentTxs, txHash)
				if len(freedTxs) != 0 {
					for freedTx := range freedTxs {
						freedTxsMap[freedTx] = true
						// logrus.Infof("Sending blocked tx: %v (%v)", freedTx.methodName, freedTx.originalIds)
						// sendTx(freedTx)
						// sentThisRound++
					}
				}
			}
		}
		if len(sentTxs) == 0 && dependencyGraph.Length == 0 && txStreamOpen == false {
			logrus.Warnf("Shutting down")
			running = false
		}
		// Try to send freed txs

		dependenciesSent := 0
		streamSent := 0
		if executed < outstandingTxs {
			outstandingTxs--
		} else {
			outstandingTxs++
		}
		for tx := range freedTxsMap {
			if dependenciesSent >= outstandingTxs {
				break
			}
			dependenciesSent++
			sendTx(tx)
			delete(freedTxsMap, tx)
		}
		for dependenciesSent+streamSent < outstandingTxs && txStreamOpen {
			txResponse, chOpen := <-txsChan
			if !chOpen {
				logrus.Warnf("No more txs in channel")
				break
			}
			shouldWait := dependencyGraph.AddDependency(txResponse)
			if !shouldWait {
				// logrus.Infof("Sending tx: %v (%v)", txResponse.methodName, txResponse.originalIds)
				sendTx(txResponse)
				streamSent++
			}
		}
		logrus.Infof("Sending: %v: Last sentTxs %v, sent this round: dependencies: %v stream: %v, txs executed %v", outstandingTxs,
			len(sentTxs), dependenciesSent, streamSent, executed)
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
	c := config.Servers[0]

	// Chain id: 1
	logsReader := logsreader.CreateLogsReader(config.Contracts.ReplayTransactionsPath, config.Contracts.CKABI, config.Contracts.KittyABI)

	defaultAccount := acm.SigningAccounts([]*acm.PrivateAccount{acm.GeneratePrivateAccountFromSecret("0")})
	client := def.NewClientWithLocalSigning(c.Address, time.Duration(config.Benchmark.Timeout)*time.Second, defaultAccount)

	// Deploy Genes contract
	// Deploy Genes contract
	geneScienceAddress, err := utils.CreateContract(c.ChainID, &config, logsReader, client, config.Contracts.GenePath)
	checkFatalError(err)
	logrus.Infof("Deployed GeneScience at: %v", geneScienceAddress)

	// Deploy CK contract
	ckAddress, err := utils.CreateContract(c.ChainID, &config, logsReader, client, config.Contracts.Path, geneScienceAddress)
	logsReader.Advance(2)
	checkFatalError(err)
	logrus.Infof("Deployed CK in partition %v at: %v", c.ChainID, ckAddress)
	logsReader.SetContractAddr(ckAddress)

	blockChan := make(chan *rpcquery.SignedHeadersResult)
	go utils.ListenBlockHeaders(c.ChainID, client, logs, blockChan)
	clientEmitter(&config, logs, ckAddress, client, logsReader, blockChan)
}
