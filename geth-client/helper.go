package main

import (
	"io/ioutil"
	"log"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

func newMoveTx(nonce uint64, to common.Address, amount *big.Int, gasLimit uint64, gasPrice *big.Int, proof *common.AccountResult) *types.Transaction {
	data := []byte{0, 0, 0, 1}
	proofBytes, err := rlp.EncodeToBytes(proof)
	if err != nil {
		log.Fatalf("Error encoding proof: %v", err)
	}
	data = append(data, proofBytes...)
	tx := types.NewTransaction(nonce, to, amount, gasLimit, gasPrice, data)
	return tx
}

func createContract(nonce uint64, amount *big.Int, gasLimit uint64, gasPrice *big.Int, contractPath string) (*types.Transaction, error) {
	file, err := ioutil.ReadFile(contractPath)
	if err != nil {
		return nil, err
	}
	contractCode := common.FromHex(string(file))
	tx := types.NewContractCreation(nonce, amount, gasLimit, gasPrice, contractCode)
	return tx, nil
}
