package main

import (
	"context"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/enriquefynn/burrow-client/utils"
	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/deploy/def"
	"github.com/hyperledger/burrow/rpc/rpcevents"
	"github.com/hyperledger/burrow/txs/payload"
	"github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
)

var (
	wg         sync.WaitGroup
	shouldExit = false
)

func checkFatalError(err error) {
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}
}

func clientClosedLoop(signingAccount []acm.AddressableSigner, account *acm.Account, client *def.Client, config utils.Config) {
	defer wg.Done()

	contractAddress, err := crypto.AddressFromHexString(config.Contracts.Address)
	checkFatalError(err)

	addr := signingAccount[0].GetAddress()

	for !shouldExit {
		account.Sequence++

		payloadTx := utils.CallContractTx(addr, &contractAddress, account.Sequence, nil)

		txExecution, err := client.SignTxOnBehalfOf(payload.Payload(&payloadTx), signingAccount)
		checkFatalError(err)
		// logrus.Infof("TxExecution: %v\n", txExecution)
		_, err = client.BroadcastEnvelope(txExecution)
		checkFatalError(err)
		// logrus.Infof("Tx included in block height: %v", txRecpt.Height)
	}
}

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	config := utils.Config{}
	configFile, err := ioutil.ReadFile(os.Args[1])
	checkFatalError(err)
	err = yaml.Unmarshal(configFile, &config)
	checkFatalError(err)

	defaultAccount := acm.SigningAccounts([]*acm.PrivateAccount{acm.GeneratePrivateAccountFromSecret("0")})

	client := def.NewClientWithLocalSigning(config.Benchmark.Address, time.Duration(config.Benchmark.Timeout)*time.Second, defaultAccount)
	logrus.Infof("Generating %v accounts\n", config.Benchmark.Clients)
	signingAccounts, accounts := utils.GetSignedAndUpdatedAccounts(client, config.Benchmark.Clients)

	if config.Contracts.Deploy {
		accounts[0].Sequence++
		tx := utils.DeployContract(signingAccounts[0][0].GetAddress(), accounts[0].Sequence, config.Contracts.Path)
		txExecution, err := client.SignTx(payload.Payload(&tx))
		checkFatalError(err)
		txReceipt, err := client.BroadcastEnvelope(txExecution)
		checkFatalError(err)
		config.Contracts.Address = txReceipt.Receipt.ContractAddress.String()
		logrus.Infof("Created contract: %v in block height: %v", config.Contracts.Address, txReceipt.Height)
	}

	wg.Add(config.Benchmark.Clients)
	for clientID := 0; clientID < config.Benchmark.Clients; clientID++ {
		go clientClosedLoop(signingAccounts[clientID], accounts[clientID], client, config)
	}
	go func() {
		for range c {
			logrus.Warn("Ctr-C: exiting...")
			shouldExit = true
		}
	}()

	go func() {
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
		for !shouldExit {
			resp, err := signedHeaders.Recv()
			checkFatalError(err)
			// logrus.Infof("Block received: %v", resp.SignedHeader)
			elapsedTime := resp.SignedHeader.Time.Sub(startTime)
			if elapsedTime < 0 {
				continue
			}
			totalTxs += resp.SignedHeader.NumTxs
			logrus.Infof("Txs: %v, elapsed time: %v", totalTxs, elapsedTime)
			logrus.Infof("Tx/s: %v", float64(totalTxs)/elapsedTime.Seconds())
		}
	}()
	wg.Wait()
}
