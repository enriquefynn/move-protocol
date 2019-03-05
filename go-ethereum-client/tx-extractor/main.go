package main

import (
	"math/big"
	"os"
	"reflect"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/hyperledger/burrow/crypto"
	pb "gopkg.in/cheggaaa/pb.v1"

	"github.com/ethereum/go-ethereum/common"

	lutils "github.com/enriquefynn/sharding-runner/go-ethereum-client/tx-extractor/utils"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
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

	mainContractAddr := common.HexToAddress("0x06012c8cf97bead5deae237070f9587f8e7a266d")
	startedContractBlock := uint64(4605167)

	// mainAccount := acm.SigningAccounts([]*acm.PrivateAccount{acm.GeneratePrivateAccountFromSecret("0")})[0]
	txsFile := lutils.CreateTxsRW(os.Args[1])

	simulatedAccounts := lutils.NewSimulatedSender()

	var contractSimulatedAddr common.Address

	var (
		config      = params.MainnetChainConfig
		signer      = types.MakeSigner(config, big.NewInt(int64(startedContractBlock)))
		cache       = 4098
		cacheConfig = core.CacheConfig{
			Disabled:      true,
			TrieTimeLimit: 5 * time.Minute,
		}
		engine   = ethash.NewFullFaker()
		ethDb, _ = ethdb.NewLDBDatabase(os.Args[2], cache, 256)
		vmConfig = vm.Config{}
	)
	blockchain, err := core.NewBlockChain(ethDb, &cacheConfig, config, engine, vmConfig, func(*types.Block) bool { return true })
	lutils.FatalError(err)

	finalBlockNumber := blockchain.CurrentBlock().Number().Uint64()

	bar := pb.StartNew(int(finalBlockNumber - startedContractBlock))
	logrus.Printf("Testing cryptoKitties at Contract: %x at block %v until block %v", mainContractAddr,
		startedContractBlock, finalBlockNumber)

	blockchainState, err := blockchain.State()
	lutils.FatalError(err)
	for blkN := startedContractBlock; blkN != finalBlockNumber; blkN++ {
		// 0: idk 1: fail 2: success
		txStatusMap := make(map[common.Hash]uint64)
		// ignore transaction
		ignoreTx := make(map[common.Hash]bool)
		block := blockchain.GetBlockByNumber(blkN)

		// Search for Logs:
		// If there is a log and is not the creating tx we can ignore the tx when searching for the tx, later
		receipts := blockchain.GetReceiptsByHash(block.Hash())
		for _, receipt := range receipts {
			tx := block.Transaction(receipt.TxHash)
			txStatusMap[receipt.TxHash] = receipt.Status + 1

			for _, log := range receipt.Logs {
				txValue, txGasPrice, txGas, txData := tx.Value(), tx.GasPrice(), tx.Gas(), tx.Data()
				from, err := signer.Sender(tx)
				lutils.FatalError(err)
				if reflect.DeepEqual(log.Address, mainContractAddr) {
					// Mark to ignore when searching for txs
					ignoreTx[tx.Hash()] = true
					simulatedFrom := simulatedAccounts.GetOrMake(from)
					// Creating a contract
					if tx.To() == nil {
						contractIDBytes := tryCreateContract(simulatedAccounts, txsFile, simulatedFrom.GetAddress(), receipt.ContractAddress, tx, receipt.Status, true)
						if reflect.DeepEqual(receipt.ContractAddress, mainContractAddr) {
							contractSimulatedAddr = common.BytesToAddress(contractIDBytes)
							logrus.Infof("Creating main contract: %x addr: %x", tx.Hash(), receipt.ContractAddress)
						} else {
							logrus.Infof("Creating contract that calls CK on init %x addr: %x on %x", tx.Hash(), receipt.ContractAddress, contractIDBytes)
						}
					} else {
						// logrus.Infof("%x LOG TO CONTRACT %x %x", tx.Hash(), log.Topics, log.Data)

						senderCodeSize := blockchainState.GetCodeSize(*tx.To())
						// Called by contract, should deploy it!
						contractID := tryCreateContract(simulatedAccounts, txsFile, simulatedFrom.GetAddress(), *tx.To(), tx, receipt.Status, senderCodeSize != 0)
						txsFile.SaveTx(simulatedFrom.GetAddress().Bytes(), contractID, txData, txValue, txGasPrice, txGas, receipt.Status)
					}
					break
				}
			}
		}

		for _, tx := range block.Transactions() {
			// ignore txs that create contracts
			if tx.To() == nil {
				continue
			}
			// Already did in the log || is not interesting
			if ignoreTx[tx.Hash()] || !reflect.DeepEqual(tx.To().Bytes(), mainContractAddr.Bytes()) {
				continue
			}

			if txStatusMap[tx.Hash()] == 0 {
				logrus.Fatalf("Tx didn't emit any receipt %x, investigate", tx.Hash())
			}

			from, err := signer.Sender(tx)
			lutils.FatalError(err)
			txStatus := txStatusMap[tx.Hash()] - 1
			txValue, txGasPrice, txGas, txData := tx.Value(), tx.GasPrice(), tx.Gas(), tx.Data()

			if reflect.DeepEqual(from.Bytes(), mainContractAddr.Bytes()) {
				logrus.Fatal("Contracts shouldn't call anything")

				// transaction to the contract
			} else if tx.To() != nil && reflect.DeepEqual(tx.To().Bytes(), mainContractAddr.Bytes()) {
				simulatedFrom := simulatedAccounts.GetOrMake(from)
				// Calling a method
				if len(txData) >= 4 {
					signature := make([]byte, 4)
					copy(signature, txData[:4])
					method, err := ckABI.MethodById(txData[:4])
					if err != nil {
						if txStatus == 1 {
							logrus.Fatalf("Should have failed when calling a non-existant method: %x", tx.Hash())
						}
						// Should create contracts and set txData param for those
					} else if method.Name == "setGeneScienceAddress" || method.Name == "setSaleAuctionAddress" ||
						method.Name == "setNewAddress" || method.Name == "setSiringAuctionAddress" ||
						method.Name == "setCEO" || method.Name == "setCFO" || method.Name == "setCOO" {
						// 4 + (32-20)
						newContractAddr := common.BytesToAddress(txData[16:])
						senderCodeLen := blockchainState.GetCodeSize(newContractAddr)
						// Create contract to set in function txData param
						var newAddress common.Address
						if senderCodeLen != 0 {
							logrus.Infof("Creating contract for method: %v at %x original tx: %x", method.Name, tx.Data(), tx.Hash())
							contractID := tryCreateContract(simulatedAccounts, txsFile, simulatedFrom.GetAddress(), newContractAddr, tx, txStatus, true)

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
						if txStatus == 1 {
							logrus.Warnf("Tx %x should have failed (did not emit any log) or call the method %v", tx.Hash(), method.Name)
						}
					}
				}
				txsFile.SaveTx(simulatedFrom.GetAddress().Bytes(), contractSimulatedAddr.Bytes(), txData, txValue, txGasPrice, txGas, txStatus)
			}
		}
		bar.Increment()
	}
	txsFile.Close()
	bar.FinishPrint("The End!")
}
