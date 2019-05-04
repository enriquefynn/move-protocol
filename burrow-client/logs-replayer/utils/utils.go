package utils

import (
	"context"
	"fmt"

	"github.com/hyperledger/burrow/rpc/rpcquery"
	// "github.com/tendermint/tendermint/types"

	"github.com/enriquefynn/sharding-runner/burrow-client/config"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/logsreader"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/deploy/def"
	"github.com/hyperledger/burrow/rpc/rpcevents"
	"github.com/sirupsen/logrus"
)

func ListenBlockHeaders(partition string, client *def.Client, logs *Log, blockChan chan<- *rpcquery.SignedHeadersResult) {
	defer func() { logs.Close() }()
	end := rpcevents.StreamBound()

	request := &rpcevents.BlocksRequest{
		BlockRange: rpcevents.NewBlockRange(rpcevents.AbsoluteBound(1), end),
	}
	clientEvents, err := client.Query()
	signedHeaders, err := clientEvents.ListSignedHeaders(context.Background(), request)
	checkFatalError(err)

	commence := false
	lastTime := int64(0)
	for {
		resp, err := signedHeaders.Recv()
		checkFatalError(err)
		if !commence && resp.SignedHeader.TotalTxs >= 2 {
			commence = true
		}
		if commence {
			blockChan <- resp
			deltaTime := float64(resp.SignedHeader.Time.UnixNano()-lastTime) / float64(1e9)
			logrus.Infof("---------GOT BLOCK %v from partition %v, totalTx: %v, Elapsed time: %v, root hash: %v, storage hash: %v",
				resp.SignedHeader.Height, partition, resp.SignedHeader.TotalTxs, deltaTime, resp.SignedHeader.Hash(),
				resp.SignedHeader.AppHash)
			logs.Log("tput-partition-"+partition, "%d %d %v\n", resp.SignedHeader.TotalTxs, resp.SignedHeader.Time.UnixNano(), resp.SignedHeader.ProposerAddress)
			lastTime = resp.SignedHeader.Time.UnixNano()
			// logs.Flush()
		}
	}
}

func CreateContract(chainID string, config *config.Config, accounts *logsreader.LogsReader, client *def.Client, path string, args ...interface{}) (*crypto.Address, error) {
	contractEnv, err := accounts.CreateContract(chainID, path, args...)
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
