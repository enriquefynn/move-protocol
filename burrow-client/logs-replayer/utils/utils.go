package utils

import (
	"context"
	"fmt"
	"time"

	"github.com/hyperledger/burrow/logging"

	// "github.com/tendermint/tendermint/types"

	"github.com/enriquefynn/sharding-runner/burrow-client/config"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/logsreader"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/deploy/def"
	"github.com/hyperledger/burrow/rpc/rpcevents"
	"github.com/sirupsen/logrus"
)

func ListenBlockHeaders(partition string, client *def.Client, logs *Log, blockChan chan<- *rpcevents.SignedHeadersResult) {
	logrus.Infof("Getting blocks for partition %v %v", partition, client.ChainAddress)
	defer func() { logs.Close() }()
	end := rpcevents.StreamBound()

	request := &rpcevents.BlocksRequest{
		BlockRange: rpcevents.NewBlockRange(rpcevents.AbsoluteBound(1), end),
	}
	logger := logging.NewNoopLogger()
	clientEvents, err := client.Events(logger)
	signedHeaders, err := clientEvents.StreamSignedHeaders(context.Background(), request)
	checkFatalError(err)

	commence := false
	lastTime := int64(0)
	for {
		start := time.Now()
		resp, err := signedHeaders.Recv()
		tookToReceive := time.Since(start).Seconds()
		checkFatalError(err)
		if !commence && resp.SignedHeader.TotalTxs >= 2 {
			commence = true
		}
		if commence {
			blockChan <- resp
			deltaTime := float64(resp.SignedHeader.Time.UnixNano()-lastTime) / float64(1e9)
			logrus.Infof("---------GOT BLOCK %v from partition %v, totalTx: %v, Elapsed time: %v, root hash: %v, storage hash: %v, took: %v",
				resp.SignedHeader.Height, partition, resp.SignedHeader.TotalTxs, deltaTime, resp.SignedHeader.Hash(),
				resp.SignedHeader.AppHash, tookToReceive)
			logs.Log("tput-partition-"+partition, "%d %d %d %v\n", resp.SignedHeader.TotalTxs, resp.SignedHeader.Time.UnixNano(), resp.SignedHeader.Height, resp.SignedHeader.ProposerAddress)
			lastTime = resp.SignedHeader.Time.UnixNano()
			// logs.Flush()
		}
	}
}

func debugf(format string, args ...interface{}) {
	logrus.Infof(format, args...)
}

func ListenBlockHeaders2(partition string, client *def.Client, logs *Log, blockChan chan<- *rpcevents.SignedHeadersResult) {
	logrus.Infof("Getting blocks for partition %v %v", partition, client.ChainAddress)
	defer func() { logs.Close() }()
	end := rpcevents.StreamBound()

	request := &rpcevents.BlocksRequest{
		BlockRange: rpcevents.NewBlockRange(rpcevents.AbsoluteBound(1), end),
	}
	logger := logging.NewNoopLogger()
	clientEvents, err := client.Events(logger)
	signedHeaders, err := clientEvents.StreamSignedHeaders(context.Background(), request)
	checkFatalError(err)

	lastTime := int64(0)
	for {
		start := time.Now()
		resp, err := signedHeaders.Recv()
		if err != nil {
			logrus.Fatalf("ERROR: %v", err)
		}
		tookToReceive := time.Since(start).Seconds()
		checkFatalError(err)
		blockChan <- resp
		deltaTime := float64(resp.SignedHeader.Time.UnixNano()-lastTime) / float64(1e9)
		debugf("---------GOT BLOCK %v from partition %v, totalTx: %v, Elapsed time: %v, root hash: %v, storage hash: %v, took: %v",
			resp.SignedHeader.Height, partition, resp.SignedHeader.TotalTxs, deltaTime, resp.SignedHeader.Hash(),
			resp.SignedHeader.AppHash, tookToReceive)
		logs.Log("tput-partition-"+partition, "%d %d %d %v\n", resp.SignedHeader.TotalTxs, resp.SignedHeader.Time.UnixNano(), resp.SignedHeader.Height, resp.SignedHeader.ProposerAddress)
		lastTime = resp.SignedHeader.Time.UnixNano()
		// logs.Flush()
	}
}

func CreateContract(chainID string, config *config.Config, accounts *logsreader.LogsReader, client *def.Client, path string, args ...interface{}) (*crypto.Address, error) {
	contractEnv, err := accounts.CreateContract(chainID, path, args...)
	if err != nil {
		return nil, err
	}
	logger := logging.NewNoopLogger()
	receipt, err := client.BroadcastEnvelope(contractEnv, logger)
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
