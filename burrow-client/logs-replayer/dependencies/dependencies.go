package dependencies

import (
	"fmt"

	// "github.com/tendermint/tendermint/types"

	"github.com/hyperledger/burrow/dependencies"
	"github.com/hyperledger/burrow/rpc/rpcquery"
	"github.com/tendermint/tendermint/types"
)

type Node struct {
	child map[int64]*Node
	tx    *TxResponse
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
func (dp *Dependencies) AddDependency(tx *TxResponse) bool {
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

func (dp *Dependencies) RemoveDependency(dependencies []int64) map[*TxResponse]bool {
	dp.Length--
	returnedDep := make(map[*TxResponse]bool)

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

type ProofToGoTxResponse struct {
	Proof *rpcquery.AccountProofs
	Tx    *dependencies.TxResponse
}

func (dp *Dependencies) AddSignedHeaderToMove(id int64, signedHeader *types.SignedHeader) {
	dep := dp.idDep[id]
	if dep.tx.MethodName != "move2" {
		panic("Dependency should be move2")
	}
	dep.tx.Tx.SignedHeader = signedHeader
}
