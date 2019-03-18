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

type Node struct {
	child map[int]*Node
	tx    *TxResponse
}

type Dependencies struct {
	idDep map[int]*Node
}

func NewDependencies() *Dependencies {
	return &Dependencies{
		idDep: make(map[int]*Node),
	}
}

// AddDependency add a dependency and return if is allowed to send tx
func (dp *Dependencies) AddDependency(tx *TxResponse) bool {
	newNode := &Node{
		tx:    tx,
		child: make(map[int]*Node),
	}

	shouldWait := false
	for _, dependency := range tx.originalIds {
		if _, ok := dp.idDep[dependency]; !ok {
			logrus.Infof("Adding dependency: %v -> %v (%v)", dependency, newNode.tx.methodName, newNode.tx.originalIds)
			dp.idDep[dependency] = newNode
		} else {
			shouldWait = true
			father := dp.idDep[dependency]
			// "recursive" insert dependency
			for {
				if father.child[dependency] == nil {
					logrus.Infof("Adding dependency: %v (%v) -> %v (%v)", father, father.tx.methodName, newNode.tx.methodName, newNode.tx.originalIds)
					father.child[dependency] = newNode
					break
				}
				father = father.child[dependency]
			}
		}
	}
	return shouldWait
}

func (dp *Dependencies) RemoveDependency(dependencies []int) []*TxResponse {
	var returnedDep []*TxResponse

	if len(dependencies) == 1 {
		dependency := dependencies[0]

		blockedTx := dp.idDep[dependency].child[dependency]
		// Delete response
		delete(dp.idDep, dependency)
		if blockedTx == nil {
			// No need to execute dependencies
			return returnedDep
		}
		// Should wait for it
		dp.idDep[dependency] = blockedTx
		// Can execute next?
		// Only waiting for 1 dep, good!
		if len(blockedTx.tx.originalIds) == 1 {
			// Should send
			returnedDep = append(returnedDep, blockedTx.tx)
		} else {
			// Is a breed (should check other dep)
			for _, otherID := range blockedTx.tx.originalIds {
				if otherID != dependency {
					// Can send?
					if dp.idDep[otherID] == blockedTx {
						// Should send
						returnedDep = append(returnedDep, blockedTx.tx)
					}
				}
			}
		}
	} else if len(dependencies) == 2 {
		dependency1 := dependencies[0]
		dependency2 := dependencies[1]
		// Tx has 2 dependencies
		blocked1 := dp.idDep[dependency1].child[dependency1]
		blocked2 := dp.idDep[dependency2].child[dependency2]

		delete(dp.idDep, dependency1)
		delete(dp.idDep, dependency2)
		// has dependency in [0]
		if blocked1 != nil {
			dp.idDep[dependency1] = blocked1
			returnedDep = append(returnedDep, blocked1.tx)
		}
		if blocked2 != nil {
			dp.idDep[dependency2] = blocked2
			// If is not the same should send another tx
			if blocked1 != blocked2 {
				returnedDep = append(returnedDep, blocked2.tx)
			}
		}
	} else {
		dependency1 := dependencies[0]
		dependency2 := dependencies[1]
		dependency3 := dependencies[2]
		// Tx has 3 dependencies
		blocked1 := dp.idDep[dependency1].child[dependency1]
		blocked2 := dp.idDep[dependency2].child[dependency2]
		blocked3 := dp.idDep[dependency3].child[dependency3]

		delete(dp.idDep, dependency1)
		delete(dp.idDep, dependency2)
		delete(dp.idDep, dependency3)
		// has dependency in [0]
		if blocked1 != nil {
			dp.idDep[dependency1] = blocked1
			returnedDep = append(returnedDep, blocked1.tx)
		}
		if blocked2 != nil {
			dp.idDep[dependency2] = blocked2
			// If is not the same should send another tx
			if blocked1 != blocked2 {
				returnedDep = append(returnedDep, blocked2.tx)
			}
		}
		if blocked3 != nil {
			dp.idDep[dependency3] = blocked3
			// If is not the same should send another tx
			if blocked1 != blocked3 && blocked2 != blocked3 {
				returnedDep = append(returnedDep, blocked3.tx)
			}
		}

	}

	return returnedDep
}

// TRYING QUEUE method
type txQueue struct {
	queue []*TxResponse
}

func (tq *txQueue) Add(tx *TxResponse) {

}

func (tq *txQueue) Remove() {

}
