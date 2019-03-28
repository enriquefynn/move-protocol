package logsreader

import (
	"math/big"
	"strconv"

	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/partitioning"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/execution/exec"
	"github.com/hyperledger/burrow/txs/payload"
	"github.com/sirupsen/logrus"
)

func (lr *LogsReader) ChangeIDsMultiShard(txResponse *TxResponse, idMap map[int64]*crypto.Address, contractsMap []*crypto.Address) {
	txResponse.Tx.Address = contractsMap[int64(txResponse.PartitionIndex)]

	if txResponse.MethodName == "createPromoKitty" {
		txInput, err := lr.abi.Methods["createPromoKitty"].Inputs.Pack(txResponse.BigIntArgument, txResponse.AddressArgument[0])
		fatalError(err)
		txResponse.Tx.Data = append(lr.abi.Methods["createPromoKitty"].Id(), txInput...)

		// giveBirth(uint256 _matronId)
	} else if txResponse.MethodName == "giveBirth" {
		matronID := txResponse.OriginalIds[0]
		matronAddress := idMap[matronID]

		txInput, err := lr.abi.Methods["giveBirth"].Inputs.Pack(matronAddress)
		fatalError(err)
		txResponse.Tx.Data = append(lr.abi.Methods["giveBirth"].Id(), txInput...)

		// breed(uint256 _matronId, uint256 _sireId)
	} else if txResponse.MethodName == "breed" {
		matronID := txResponse.OriginalIds[0]
		matronAddress := idMap[matronID]
		sireID := txResponse.OriginalIds[1]
		sireAddress := idMap[sireID]

		txInput, err := lr.abi.Methods["breed"].Inputs.Pack(matronAddress, sireAddress)
		txResponse.Tx.Data = append(lr.abi.Methods["breed"].Id(), txInput...)
		fatalError(err)

		// Kitty methods
		// approveSiring(address _addr, uint256 _sireId)
		// transfer(address _to, uint256 _tokenId)
		// approve(address _to, uint256 _tokenId)
		// transferFrom(address _from, address _to, uint256 _tokenId)
	} else {
		// Update interacting contract
		tokenID := txResponse.OriginalIds[0]
		txResponse.Tx.Address = idMap[tokenID]
		// logrus.Infof("Calling: %v", kittyContractAddr)

		if txResponse.MethodName == "approveSiring" {
			txInput, err := lr.kittyABI.Methods["approveSiring"].Inputs.Pack(txResponse.AddressArgument[0])
			fatalError(err)
			txResponse.Tx.Data = append(lr.kittyABI.Methods["approveSiring"].Id(), txInput...)
		} else if txResponse.MethodName == "transfer" {
			txInput, err := lr.kittyABI.Methods["transfer"].Inputs.Pack(txResponse.AddressArgument[0])
			fatalError(err)
			txResponse.Tx.Data = append(lr.kittyABI.Methods["transfer"].Id(), txInput...)
		} else if txResponse.MethodName == "approve" {
			txInput, err := lr.kittyABI.Methods["approve"].Inputs.Pack(txResponse.AddressArgument[0])
			fatalError(err)
			txResponse.Tx.Data = append(lr.kittyABI.Methods["approve"].Id(), txInput...)

		} else if txResponse.MethodName == "transferFrom" {
			txInput, err := lr.kittyABI.Methods["transferFrom"].Inputs.Pack(txResponse.AddressArgument[1])
			fatalError(err)
			txResponse.Tx.Data = append(lr.kittyABI.Methods["transferFrom"].Id(), txInput...)

		} else {
			logrus.Fatalf("Method not found %v", txResponse.MethodName)
		}
	}
}

func (lr *LogsReader) ExtractNewContractAddress(event *exec.Event) *crypto.Address {
	addr := crypto.MustAddressFromBytes(event.Log.Data[12:32])
	return &addr
}

func NewTxResponse(methodName string, chainID, originalID int64, signer *SeqAccount, to, from crypto.Address, amount uint64, data []byte) *TxResponse {
	newTx := payload.CallTx{
		Input: &payload.TxInput{
			Address: from,
			Amount:  amount,
		},
		Address:  &to,
		Fee:      1,
		GasLimit: 4100000000,
		Data:     data,
	}
	return &TxResponse{
		ChainID:     strconv.Itoa(int(chainID)),
		Tx:          &newTx,
		Signer:      signer,
		MethodName:  methodName,
		OriginalIds: []int64{originalID},
	}

}
func (lr *LogsReader) CreateMoveDecidePartitioning(tx *TxResponse, partitioning partitioning.Partitioning, idMap map[int64]*crypto.Address) []*TxResponse {
	var txResponses []*TxResponse
	var partitioningObjects []int64

	if len(tx.OriginalIds) == 3 {
		// Last one is the kitty id, should not be considered (create in same partition as matron)
		partitioningObjects = tx.OriginalIds[:2]
	} else {
		partitioningObjects = tx.OriginalIds
	}
	shouldMove := !partitioning.IsSame(partitioningObjects...)

	partitionToGo := partitioning.WhereToMove(partitioningObjects...)
	// set the partition to go tx
	tx.PartitionIndex = int(partitionToGo - 1)
	tx.ChainID = strconv.Itoa(tx.PartitionIndex + 1)
	if !shouldMove {
		// No need to move
		return nil
	}

	for _, id := range partitioningObjects {
		originalPartition := partitioning.Get(id)
		// Should move this id
		if originalPartition != partitionToGo {
			// Move input
			txInput, err := lr.kittyABI.Methods["moveTo"].Inputs.Pack(big.NewInt(partitionToGo))
			fatalError(err)
			txData := append(lr.kittyABI.Methods["moveTo"].Id(), txInput...)

			accountToMove := idMap[id]
			// move from originalPartition to partitionToGo
			moveToTxResponse := NewTxResponse("moveTo", originalPartition, id, tx.Signer, *accountToMove, tx.Tx.Input.Address, tx.Tx.Input.Amount, txData)
			// move from originalPartition to partitionToGo
			move2TxResponse := NewTxResponse("move2", partitionToGo, id, tx.Signer, *accountToMove, tx.Tx.Input.Address, tx.Tx.Input.Amount, txData)

			txResponses = append(txResponses, moveToTxResponse)
			txResponses = append(txResponses, move2TxResponse)

			logrus.Infof("Moving %v from %v to %v", accountToMove, originalPartition, partitionToGo)

		}
	}
	return txResponses

}
