package main

import (
	"context"
	"fmt"
	"time"

	"github.com/enriquefynn/sharding-runner/burrow-client/utils"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/deploy/def"
	"github.com/hyperledger/burrow/execution/exec"
	"github.com/hyperledger/burrow/rpc/rpcevents"
	"github.com/sirupsen/logrus"
)

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

func listenBlocks(client *def.Client, blockCh chan<- *exec.BlockExecution) {
	end := rpcevents.StreamBound()

	request := &rpcevents.BlocksRequest{
		BlockRange: rpcevents.NewBlockRange(rpcevents.AbsoluteBound(1), end),
	}
	clientEvents, err := client.Events()
	stream, err := clientEvents.Stream(context.Background(), request)
	checkFatalError(err)
	rpcevents.ConsumeBlockExecutions(stream, func(blk *exec.BlockExecution) error {
		blockCh <- blk
		return nil
	})
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

func checkFatalError(err error) {
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}
}
