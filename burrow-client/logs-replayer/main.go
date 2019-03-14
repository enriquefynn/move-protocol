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
	method string
	ids    []int
}

func clientEmitter(config *utils.Config, client *def.Client, logsReader *LogsReader, blockChan chan *exec.BlockExecution) {
	idMap := make(map[int]int)

	outstandingTxs := config.Benchmark.OutstandingTxs

	sentTxs := make(map[string]methodAndID)
	for i := 0; i < outstandingTxs; i++ {
		txResponse, err := logsReader.LoadNextLog()
		fatalError(err)
		signedTx := txResponse.Sign()
		fatalError(err)
		_, err = client.BroadcastEnvelopeAsync(signedTx)
		fatalError(err)
		sentTxs[string(signedTx.Tx.Hash())] = methodAndID{
			method: txResponse.methodName,
			ids:    txResponse.originalIds,
		}
	}

	for {
		block := <-blockChan
		logrus.Infof("RECEIVED BLOCK %v", block.Header.Height)
		for _, tx := range block.TxExecutions {
			txHash := string(tx.TxHash)
			// Found tx
			if sentTx, ok := sentTxs[txHash]; ok {
				if sentTx.method == "createPromoKitty" {
					idMap[sentTx.ids[0]] = logsReader.extractIDTransfer(tx.Events[1])
				} else if sentTx.method == "giveBirth" {
					idMap[sentTx.ids[0]] = logsReader.extractIDTransfer(tx.Events[1])
				} else if sentTx.method == "breed" {

				} else if sentTx.method == "approve" {

				} else if sentTx.method == "transferFrom" {

				} else if sentTx.method == "transfer" {
				}
				delete(sentTxs, txHash)
				txResponse, err := logsReader.LoadNextLog()
				if err != nil {
					logrus.Infof("Stopping reading txs: %v", err)
					break
				}
				signedTx := txResponse.Sign()
				_, err = client.BroadcastEnvelopeAsync(signedTx)
				fatalError(err)
				sentTxs[string(signedTx.Tx.Hash())] = methodAndID{
					method: txResponse.methodName,
					ids:    txResponse.originalIds,
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
