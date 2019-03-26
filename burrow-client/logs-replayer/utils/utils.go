package utils

import (
	"context"
	"fmt"

	"github.com/enriquefynn/sharding-runner/burrow-client/config"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/logsreader"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/partitioning"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/deploy/def"
	"github.com/hyperledger/burrow/execution/exec"
	"github.com/hyperledger/burrow/rpc/rpcevents"
	"github.com/sirupsen/logrus"
)

func ListenBlockHeaders(client *def.Client, logs *Log) {
	defer func() { logs.Close() }()
	end := rpcevents.StreamBound()

	request := &rpcevents.BlocksRequest{
		BlockRange: rpcevents.NewBlockRange(rpcevents.AbsoluteBound(1), end),
	}
	clientEvents, err := client.Query()
	signedHeaders, err := clientEvents.ListSignedHeaders(context.Background(), request)
	checkFatalError(err)

	commence := false
	closing := 0
	for {
		resp, err := signedHeaders.Recv()
		checkFatalError(err)
		if !commence && resp.SignedHeader.NumTxs > 0 {
			commence = true
		}
		if commence {
			logs.Log("tput", "%d %d\n", resp.SignedHeader.TotalTxs, resp.SignedHeader.Time.UnixNano())
			logs.Flush()
			if resp.SignedHeader.NumTxs == 0 {
				closing++
				if closing == 3 {
					break
				}
			}
		}
	}
}

func ListenBlocks(client *def.Client, blockCh chan<- *exec.BlockExecution) {
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

func CreateContract(config *config.Config, accounts *logsreader.LogsReader, client *def.Client, path string, args ...interface{}) (*crypto.Address, error) {
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
	child map[int64]*Node
	tx    *logsreader.TxResponse
}

type Dependencies struct {
	Length int
	idDep  map[int64]*Node
}

func (dp *Dependencies) bfs() {
	// visited := make(map[*Node]bool)
	for k := range dp.idDep {
		fmt.Printf("%v ", k)
	}
	fmt.Printf("\n")
}

func NewDependencies() *Dependencies {
	return &Dependencies{
		idDep: make(map[int64]*Node),
	}
}

// AddDependency add a dependency and return if is allowed to send tx
func (dp *Dependencies) AddDependency(tx *logsreader.TxResponse, partitioning partitioning.Partitioning) bool {
	var partitioningObjects []int64

	if len(tx.OriginalIds) == 3 {
		partitioningObjects = tx.OriginalIds[:2]
	} else {
		partitioningObjects = tx.OriginalIds
	}
	shouldPartition := !partitioning.IsSame(partitioningObjects...)

	if shouldPartition {
		whichPartition := partitioning.WhereToMove(partitioningObjects...)
		logrus.Infof("Moving to %v", whichPartition)
	}

	newNode := &Node{
		tx:    tx,
		child: make(map[int64]*Node),
	}
	dp.Length++

	shouldWait := false
	for _, dependency := range tx.OriginalIds {
		if _, ok := dp.idDep[dependency]; !ok {
			// logrus.Infof("Adding dependency: %v -> %v (%v)", dependency, newNode.tx.methodName, newNode.tx.originalIds)
			dp.idDep[dependency] = newNode
		} else {
			shouldWait = true
			father := dp.idDep[dependency]
			// "recursive" insert dependency
			for {
				if father.child[dependency] == nil {
					// logrus.Infof("Adding dependency: %v (%v) -> %v (%v)", father, father.tx.methodName, newNode.tx.methodName, newNode.tx.originalIds)
					father.child[dependency] = newNode
					break
				}
				father = father.child[dependency]
			}
		}
	}
	return shouldWait
}

func (dp *Dependencies) canSend(cameFromID int64, blockedTx *Node) bool {
	for _, otherID := range blockedTx.tx.OriginalIds {
		if otherID != cameFromID {
			// Can send?
			if dp.idDep[otherID] != blockedTx {
				return false
			}
		}
	}
	return true
}

func (dp *Dependencies) RemoveDependency(dependencies []int64) map[*logsreader.TxResponse]bool {
	dp.Length--
	returnedDep := make(map[*logsreader.TxResponse]bool)

	for _, dependency := range dependencies {
		blockedTx := dp.idDep[dependency].child[dependency]
		// Delete response
		delete(dp.idDep, dependency)
		if blockedTx != nil {
			// Should wait for it
			dp.idDep[dependency] = blockedTx
			// Can execute next?
			if dp.canSend(dependency, blockedTx) {
				// logrus.Infof("RETURNING %v", blockedTx.tx)
				returnedDep[blockedTx.tx] = true
			}
		}
	}
	return returnedDep
}
