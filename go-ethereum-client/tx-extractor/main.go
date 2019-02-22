package main

import (
	"context"
	"math/big"
	"os"
	"reflect"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/ethereum/go-ethereum/common"
	pb "gopkg.in/cheggaaa/pb.v1"

	lutils "github.com/enriquefynn/sharding-runner/go-ethereum-client/tx-extractor/utils"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/params"
	"github.com/sirupsen/logrus"
)

// CRYPTO KITTIES: 0x06012c8cf97bead5deae237070f9587f8e7a266d
// Created at block: 4605167

func main() {
	jsonABI, err := os.Open("cryptoKittiesABI.json")
	lutils.FatalError(err)
	ckABI, err := abi.JSON(jsonABI)
	lutils.FatalError(err)
	logrus.Infof("ABI: %x", ckABI.Events["Birth"].Id())

	// finalBlock := 7235717
	finalBlock := big.NewInt(7047866)

	config := params.MainnetChainConfig
	ctx := context.Background()
	client, err := ethclient.Dial(os.Args[1])
	lutils.FatalError(err)

	contractAddr := common.HexToAddress("0x06012c8cf97bead5deae237070f9587f8e7a266d")
	lutils.FatalError(err)
	startedContractBlock := int64(4605167)
	bar := pb.StartNew(int(finalBlock.Int64() - startedContractBlock))

	logrus.Printf("Testing cryptoKitties at Contract: %x at block %v", contractAddr, startedContractBlock)

	signer := types.MakeSigner(config, big.NewInt(startedContractBlock))

	// mainAccount := acm.SigningAccounts([]*acm.PrivateAccount{acm.GeneratePrivateAccountFromSecret("0")})[0]
	txsFile := lutils.CreateTxsRW(os.Args[2])

	simulatedAccounts := lutils.NewSimulatedSender()

	var contractSimulatedAddr common.Address

	for blkN := big.NewInt(startedContractBlock); blkN.Cmp(finalBlock) == -1; blkN = blkN.Add(blkN, common.Big1) {
		block, err := client.BlockByNumber(ctx, blkN)
		lutils.FatalError(err)
		for _, tx := range block.Transactions() {
			receipt, err := client.TransactionReceipt(ctx, tx.Hash())
			lutils.FatalError(err)
			from, err := signer.Sender(tx)
			lutils.FatalError(err)

			simulatedFrom := simulatedAccounts.GetOrMake(from)
			txValue, txGasPrice, txGas, txData := tx.Value().Uint64(), tx.GasPrice().Uint64(), tx.Gas(), tx.Data()

			for _, log := range receipt.Logs {
				if reflect.DeepEqual(log.Address, contractAddr) && tx.To() != nil {
					logrus.Infof("%x LOG TO CONTRACT %x %x", tx.Hash(), log.Topics, log.Data)
					senderCode, err := client.CodeAt(ctx, *tx.To(), nil)
					lutils.FatalError(err)
					// Called by contract, should deploy it!
					shouldCreate, contractID := simulatedAccounts.ShouldCreateContract(*tx.To())
					if len(senderCode) != 0 && shouldCreate {
						logrus.Infof("Should deploy contract %x", tx.To())
						txsFile.SaveTx(simulatedFrom.GetAddress().Bytes(), nil, senderCode, txValue, txGasPrice, txGas, receipt.Status)
					}
					logrus.Infof("Should call its method to original %x now: %x", tx.To(), contractID)
					txsFile.SaveTx(simulatedFrom.GetAddress().Bytes(), common.BigToAddress(big.NewInt(contractID)).Bytes(), txData, txValue, txGasPrice, txGas, receipt.Status)
					goto end
				}
			}
			// transaction from the contract shouldn't happen
			if reflect.DeepEqual(from.Bytes(), contractAddr.Bytes()) {
				logrus.Fatal("Contracts shouldn't call anything")

				// transaction creating the contract
			} else if reflect.DeepEqual(receipt.ContractAddress.Bytes(), contractAddr.Bytes()) {
				logrus.Info("Creating contract")
				senderCode, err := client.CodeAt(ctx, contractAddr, nil)
				_, contractID := simulatedAccounts.ShouldCreateContract(contractAddr)
				contractSimulatedAddr = common.BigToAddress(big.NewInt(contractID))

				lutils.FatalError(err)
				txsFile.SaveTx(simulatedFrom.GetAddress().Bytes(), nil, senderCode, txValue, txGasPrice, txGas, receipt.Status)
				// transaction to the contract
			} else if tx.To() != nil && reflect.DeepEqual(tx.To().Bytes(), contractAddr.Bytes()) {
				// Calling a method
				if len(txData) >= 4 {
					signature := make([]byte, 4)
					copy(signature, txData[:4])
					logrus.Infof("SIGNATURE: %x\n", signature)
					method, err := ckABI.MethodById(txData[:4])
					lutils.FatalError(err)
					// Should create contracts and set txData param for those
					if method.Name == "setGeneScienceAddress" || method.Name == "setSaleAuctionAddress" ||
						method.Name == "setNewAddress" || method.Name == "setSiringAuctionAddress" ||
						method.Name == "setCEO" || method.Name == "setCFO" || method.Name == "setCOO" {
						// 4 + (32-20)
						newContractAddr := common.BytesToAddress(txData[16:])
						senderCode, err := client.CodeAt(ctx, newContractAddr, nil)
						lutils.FatalError(err)
						// Create contract to set in function txData param
						if len(senderCode) != 0 {
							txsFile.SaveTx(simulatedFrom.GetAddress().Bytes(), nil, senderCode, txValue, txGasPrice, txGas, receipt.Status)
							_, contractID := simulatedAccounts.ShouldCreateContract(newContractAddr)
							newContract := common.BigToAddress(big.NewInt(contractID))
							newMethod, err := method.Inputs.Pack(newContract)
							lutils.FatalError(err)
							newMethod = append(signature, newMethod...)
							txData = newMethod
						} else {
							simulatedToInMethod := simulatedAccounts.GetOrMake(newContractAddr)
							newEthAddr := common.BytesToAddress(simulatedToInMethod.GetAddress().Bytes())
							newMethod, err := method.Inputs.Pack(newEthAddr)
							lutils.FatalError(err)
							txData = newMethod
							logrus.Infof("Calling To: %v", simulatedToInMethod)
						}
					}
					logrus.Infof("Called method: %v", method.Name)
				}
				logrus.Infof("TXdata: %x\n", txData)
				txsFile.SaveTx(simulatedFrom.GetAddress().Bytes(), contractSimulatedAddr.Bytes(), txData, tx.Value().Uint64(), tx.GasPrice().Uint64(), tx.Gas(), receipt.Status)
			}

			// end of transaction
		end:
		}
		if blkN.Cmp(big.NewInt(startedContractBlock+147)) == 0 {
			txsFile.Close()
			break
		}
		bar.Increment()
	}
	bar.FinishPrint("The End!")
}
