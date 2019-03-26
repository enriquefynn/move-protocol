package logsreader

import (
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/execution/exec"
	"github.com/sirupsen/logrus"
)

func (lr *LogsReader) ChangeIDsMultiShard(txResponse *TxResponse, idMap map[int64]crypto.Address) {
	txResponse.Tx.Data = lr.abi.Methods[txResponse.MethodName].Id()

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
		kittyContractAddr := idMap[tokenID]
		txResponse.Tx.Address = &kittyContractAddr
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

func (lr *LogsReader) ExtractNewContractAddress(event *exec.Event) crypto.Address {
	return crypto.MustAddressFromBytes(event.Log.Data[12:32])
}
