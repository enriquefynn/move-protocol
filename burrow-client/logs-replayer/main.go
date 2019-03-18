package main

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/hyperledger/burrow/execution/exec"

	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/deploy/def"
	"github.com/hyperledger/burrow/txs"

	"github.com/enriquefynn/sharding-runner/burrow-client/utils"
	"github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
)

func clientReplayer(client *def.Client, tx *txs.Envelope, responseCh chan<- int, blockChan <-chan *exec.BlockExecution) {
	resp, err := client.BroadcastEnvelope(tx)
	fatalError(err)
	// logrus.Infof("Resp: %v", tx, resp)
	if resp.Exception != nil {
		logrus.Fatalf("Exception in tx: %v : %v", tx, resp.Exception)
	}
	// logrus.Infof("Resp: %v", resp.Events[1].Log.Data)
}

type methodAndID struct {
	method  string
	ids     []int
	birthID int
}

func clientEmitter(config *utils.Config, client *def.Client, logsReader *LogsReader, blockChan chan *exec.BlockExecution) {
	// txsChan := logsReader.LogsLoader()
	// txs := 0
	// for {
	// 	txs++
	// 	txResponse := <-txsChan
	// 	signedTx := txResponse.Sign()
	// 	if txs < 3000 {
	// 		_, err := client.BroadcastEnvelopeAsync(signedTx)
	// 		fatalError(err)
	// 		continue
	// 	}
	// 	logrus.Infof("Executing %v %v", txResponse.methodName, txResponse.originalIds)

	// 	r, err := client.BroadcastEnvelope(signedTx)
	// 	if r.Exception != nil {
	// 		logrus.Fatalf("Exception happened %v executing %v %v", r.Exception, txResponse.methodName, txResponse.originalIds)
	// 	}
	// 	fatalError(err)
	// }

	txsChan := logsReader.LogsLoader()
	idMap := make(map[int]int)

	// Number of simultaneous txs allowed
	outstandingTxs := config.Benchmark.OutstandingTxs

	// SentTx that are not received
	sentTxs := make(map[string]methodAndID)

	// Dependency graph
	dependencyGraph := NewDependencies()

	sendTx := func(tx *TxResponse) {
		signedTx := tx.Sign()
		_, err := client.BroadcastEnvelopeAsync(signedTx)
		fatalError(err)
		sentTxs[string(signedTx.Tx.Hash())] = methodAndID{
			method:  tx.methodName,
			ids:     tx.originalIds,
			birthID: tx.originalBirthID,
		}
	}

	// First txs
	for i := 0; i < outstandingTxs; i++ {
		txResponse := <-txsChan
		dependencyGraph.AddDependency(txResponse)
		sendTx(txResponse)
	}

	for {
		block := <-blockChan
		logrus.Infof("RECEIVED BLOCK %v", block.Header.Height)
		for _, tx := range block.TxExecutions {
			txHash := string(tx.TxHash)
			// Found tx
			if sentTx, ok := sentTxs[txHash]; ok {
				logrus.Infof("Executed: %v %v", sentTx.method, sentTx.ids)
				freedTxs := dependencyGraph.RemoveDependency(sentTx.ids)

				if sentTx.method == "createPromoKitty" || sentTx.method == "giveBirth" {
					idMap[sentTx.birthID] = logsReader.extractIDTransfer(tx.Events[1])
					if sentTx.birthID != logsReader.extractIDTransfer(tx.Events[1]) {
						logrus.Fatal("bad luck: ids differ")
					}
				}

				if tx.Exception != nil {
					logrus.Fatalf("Exception happened %v executing %v %v", tx.Exception, sentTx.method, sentTx.ids)
				}

				delete(sentTxs, txHash)
				if freedTxs != nil {
					for _, freedTx := range freedTxs {
						logrus.Infof("Sending blocked tx: %v (%v)", freedTx.methodName, freedTx.originalIds)
						sendTx(freedTx)
					}
				} else {
					txResponse := <-txsChan
					shouldWait := dependencyGraph.AddDependency(txResponse)
					if !shouldWait {
						logrus.Infof("Sending tx: %v (%v)", txResponse.methodName, txResponse.originalIds)
						sendTx(txResponse)
					}
				}
			}
		}
	}
}

func main() {
	config := utils.Config{}
	configFile, err := ioutil.ReadFile(os.Args[1])
	checkFatalError(err)
	err = yaml.Unmarshal(configFile, &config)
	checkFatalError(err)
	// Chain id: 1
	logsReader := CreateLogsReader(config.Benchmark.ChainID, config.Contracts.ReplayTransactionsPath, config.Contracts.CKABI)

	defaultAccount := acm.SigningAccounts([]*acm.PrivateAccount{acm.GeneratePrivateAccountFromSecret("0")})
	client := def.NewClientWithLocalSigning(config.Benchmark.Address, time.Duration(config.Benchmark.Timeout)*time.Second, defaultAccount)

	// Deploy Genes contract
	address, err := createContract(&config, logsReader, client, config.Contracts.GenePath)
	checkFatalError(err)
	logrus.Infof("Deployed GeneScience at: %v", address)

	// Deploy CK contract
	address, err = createContract(&config, logsReader, client, config.Contracts.Path, address)
	logsReader.Advance(2)
	checkFatalError(err)
	logrus.Infof("Deployed CK at: %v", address)
	logsReader.SetContractAddr(address)

	blockChan := make(chan *exec.BlockExecution)

	go listenBlockHeaders(client)
	go listenBlocks(client, blockChan)

	clientEmitter(&config, client, logsReader, blockChan)
}
