package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"reflect"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/hyperledger/burrow/crypto"

	"github.com/ethereum/go-ethereum/common"
	pb "gopkg.in/cheggaaa/pb.v1"

	lutils "github.com/enriquefynn/sharding-runner/go-ethereum-client/tx-extractor/utils"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/params"
	"github.com/sirupsen/logrus"
)

// Return the id for the contract to be put in "to"
func tryCreateContract(simSender *lutils.SimulatedSender, txsRW *lutils.TxsRW, from crypto.Address, to common.Address,
	tx *types.Transaction, receiptStatus uint64, hasCode bool) []byte {
	// createContractAndCall(&simulatedAccounts, &txsFile, simulatedFrom, txValue, txGasPrice, txGas, receipt.Status)
	shouldCreate, contractID := simSender.ShouldCreateContract(to)
	contractIDBytes := common.BigToAddress(big.NewInt(contractID)).Bytes()
	if hasCode && shouldCreate {
		logrus.Infof("Should deploy contract %x in tx: %x", to, tx.Hash())
		txsRW.SaveTxCreateContract(from.Bytes(), to.Bytes(), contractIDBytes, tx.Value(), tx.GasPrice(), tx.Gas(), receiptStatus)
	}
	return contractIDBytes
}

// CRYPTO KITTIES: 0x06012c8cf97bead5deae237070f9587f8e7a266d
// Created at block: 4605167

// Block nr. where there's a contract call: 4659942

func main() {
	jsonABI, err := os.Open("cryptoKittiesABI.json")
	lutils.FatalError(err)
	ckABI, err := abi.JSON(jsonABI)
	lutils.FatalError(err)

	config := params.MainnetChainConfig
	ctx := context.Background()
	client, err := ethclient.Dial(os.Args[1])
	lutils.FatalError(err)

	// finalBlock := 7235717
	lastClientBlock, _ := client.BlockByNumber(ctx, nil)
	finalBlockNumber := lastClientBlock.Number()

	contractAddr := common.HexToAddress("0x06012c8cf97bead5deae237070f9587f8e7a266d")
	lutils.FatalError(err)
	startedContractBlock := int64(4605167)
	bar := pb.StartNew(int(finalBlockNumber.Int64() - startedContractBlock))

	logrus.Printf("Testing cryptoKitties at Contract: %x at block %v until block %v", contractAddr,
		startedContractBlock, finalBlockNumber)

	signer := types.MakeSigner(config, big.NewInt(startedContractBlock))

	// mainAccount := acm.SigningAccounts([]*acm.PrivateAccount{acm.GeneratePrivateAccountFromSecret("0")})[0]
	txsFile := lutils.CreateTxsRW(os.Args[2])

	simulatedAccounts := lutils.NewSimulatedSender()

	var contractSimulatedAddr common.Address

	for blkN := big.NewInt(startedContractBlock); blkN.Cmp(finalBlockNumber) == -1; blkN = blkN.Add(blkN, common.Big1) {
		block, err := client.BlockByNumber(ctx, blkN)
		lutils.FatalError(err)
		for _, tx := range block.Transactions() {
			receipt, err := client.TransactionReceipt(ctx, tx.Hash())
			lutils.FatalError(err)
			from, err := signer.Sender(tx)
			lutils.FatalError(err)

			txValue, txGasPrice, txGas, txData := tx.Value(), tx.GasPrice(), tx.Gas(), tx.Data()

			for _, log := range receipt.Logs {
				if reflect.DeepEqual(log.Address, contractAddr) && tx.To() != nil {
					simulatedFrom := simulatedAccounts.GetOrMake(from)
					// logrus.Infof("%x LOG TO CONTRACT %x %x", tx.Hash(), log.Topics, log.Data)
					senderCode, err := client.CodeAt(ctx, *tx.To(), nil)
					lutils.FatalError(err)
					// Called by contract, should deploy it!
					contractID := tryCreateContract(simulatedAccounts, txsFile, simulatedFrom.GetAddress(), *tx.To(), tx, receipt.Status, len(senderCode) != 0)
					txsFile.SaveTx(simulatedFrom.GetAddress().Bytes(), contractID, txData, txValue, txGasPrice, txGas, receipt.Status)
					goto end
				}
			}
			// transaction from the contract shouldn't happen
			if reflect.DeepEqual(from.Bytes(), contractAddr.Bytes()) {
				logrus.Fatal("Contracts shouldn't call anything")

				// transaction creating the main contract
			} else if reflect.DeepEqual(receipt.ContractAddress.Bytes(), contractAddr.Bytes()) {
				simulatedFrom := simulatedAccounts.GetOrMake(from)
				logrus.Info("Creating main contract")
				_, err := client.CodeAt(ctx, contractAddr, nil)
				lutils.FatalError(err)
				contractID := tryCreateContract(simulatedAccounts, txsFile, simulatedFrom.GetAddress(), contractAddr, tx, receipt.Status, true)
				contractSimulatedAddr = common.BytesToAddress(contractID)
				// transaction to the contract
			} else if tx.To() != nil && reflect.DeepEqual(tx.To().Bytes(), contractAddr.Bytes()) {
				simulatedFrom := simulatedAccounts.GetOrMake(from)
				// Calling a method
				if len(txData) >= 4 {
					signature := make([]byte, 4)
					copy(signature, txData[:4])
					method, err := ckABI.MethodById(txData[:4])
					if err != nil {
						if receipt.Status == 1 {
							lutils.FatalError(fmt.Errorf("Should have failed when calling a non-existant method: %x", tx.Hash()))
						}
						// Should create contracts and set txData param for those
					} else if method.Name == "setGeneScienceAddress" || method.Name == "setSaleAuctionAddress" ||
						method.Name == "setNewAddress" || method.Name == "setSiringAuctionAddress" ||
						method.Name == "setCEO" || method.Name == "setCFO" || method.Name == "setCOO" {
						// 4 + (32-20)
						newContractAddr := common.BytesToAddress(txData[16:])
						senderCode, err := client.CodeAt(ctx, newContractAddr, nil)
						lutils.FatalError(err)
						// Create contract to set in function txData param
						var newAddress common.Address
						if len(senderCode) != 0 {
							logrus.Infof("Creating contract for method: %v at %x original tx: %x", method.Name, tx.Data(), tx.Hash())
							contractID := tryCreateContract(simulatedAccounts, txsFile, simulatedFrom.GetAddress(), newContractAddr, tx, receipt.Status, true)

							// Get mapped contract
							newAddress = common.BytesToAddress(contractID)
						} else {
							// Get mapped address
							newAddress = common.BytesToAddress(simulatedAccounts.GetOrMake(newContractAddr).GetAddress().Bytes())
						}
						newMethod, err := method.Inputs.Pack(newAddress)
						lutils.FatalError(err)
						txData = append(signature, newMethod...)
					} else {
						if receipt.Status == 1 {
							logrus.Warnf("Tx %x should have failed (did not emit any log) or call the method %v", tx.Hash(), method.Name)
						}
					}
				}
				txsFile.SaveTx(simulatedFrom.GetAddress().Bytes(), contractSimulatedAddr.Bytes(), txData, txValue, txGasPrice, txGas, receipt.Status)
			}

			// end of transaction
		end:
		}
		// if blkN.Cmp(big.NewInt(startedContractBlock+147)) == 0 {
		// 	txsFile.Close()
		// 	break
		// }
		bar.Increment()
	}
	bar.FinishPrint("The End!")
}
