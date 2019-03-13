package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/deploy/def"
	"github.com/hyperledger/burrow/rpc/rpcevents"
	"github.com/hyperledger/burrow/txs"

	"github.com/enriquefynn/sharding-runner/burrow-client/utils"
	"github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
)

func checkFatalError(err error) {
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}
}

func clientReplayer(client *def.Client, tx *txs.Envelope, txIdx int, responseCh chan<- int) {
	resp, err := client.BroadcastEnvelope(tx)
	fatalError(err)
	if resp.Exception != nil {
		logrus.Fatalf("Exception in tx: %v : %v", tx, resp.Exception)
	}
	logrus.Infof("Resp: %v", resp.Events[1].Log.Data)
}

func clientEmitter(client *def.Client, logsReader *LogsReader) {
	// kittyMap := make(map[uint]uint)
	responseCh := make(chan<- int)
	txs := 0
	txIdx := 0
	startTime := time.Now()

	for {
		tx, signingAccount, err := logsReader.LoadNextLog()
		if err != nil {
			logrus.Infof("Stopping reading txs: %v", err)
			break
		}
		// txData := tx.Tx.Payload.Any().CallTx.Data
		// logrus.Infof("INPUT: %v", txData)
		fatalError(err)
		signedTx, err := logsReader.SignTx(tx, signingAccount)
		clientReplayer(client, signedTx, txIdx, responseCh)
		txIdx++

		// logrus.Infof("FROM: %v", to)
		// go client.BroadcastEnvelope(tx)
		// time.Sleep(1000000)

		duration := time.Now().Sub(startTime).Seconds()
		txs++
		if duration >= 1 {
			logrus.Infof("[CLI] Txs/s: %v", txs)
			txs = 0
			startTime = time.Now()
		}
	}
}

func createContract(config *utils.Config, accounts *LogsReader, client *def.Client, path string, args ...interface{}) (*crypto.Address, error) {
	contractEnv, err := accounts.CreateContract(path, args...)
	if err != nil {
		return nil, err
	}

	receipt, err := client.BroadcastEnvelope(contractEnv)
	if err != nil {
		return nil, err
	}
	if receipt.Exception != nil {
		return nil, err
	}
	contract := receipt.Receipt.ContractAddress
	account, err := client.GetAccount(contract)
	if len(account.Code) == 0 {
		return nil, fmt.Errorf("Contract creation failed : %v", account)
	}
	return &contract, nil
}

func listenBlockHeaders(client *def.Client) {
	end := rpcevents.StreamBound()

	request := &rpcevents.BlocksRequest{
		BlockRange: rpcevents.NewBlockRange(rpcevents.AbsoluteBound(1), end),
	}
	clientEvents, err := client.Query()
	signedHeaders, err := clientEvents.ListSignedHeaders(context.Background(), request)
	checkFatalError(err)
	// firstBlock, err := signedHeaders.Recv()
	// checkFatalError(err)
	startTime := time.Now()
	totalTxs := int64(0)
	commence := false
	for {
		resp, err := signedHeaders.Recv()
		checkFatalError(err)
		if !commence && resp.SignedHeader.NumTxs > 0 {
			logrus.Infof("Commence at: %v %v", resp.SignedHeader.Height, resp.SignedHeader.Time)
			commence = true
			startTime = resp.SignedHeader.Time
		}
		if commence {
			elapsedTime := resp.SignedHeader.Time.Sub(startTime)
			startTime = resp.SignedHeader.Time
			totalTxs += resp.SignedHeader.NumTxs
			logrus.Infof("[SRV] Txs: %v, elapsed time: %v", totalTxs, elapsedTime)
			logrus.Infof("[SRV] Tx/s: %v", float64(resp.SignedHeader.NumTxs)/elapsedTime.Seconds())
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

	go listenBlockHeaders(client)
	clientEmitter(client, logsReader)
}
