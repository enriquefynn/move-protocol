package main

import (
	"encoding/hex"
	"io/ioutil"

	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/txs/payload"
	"github.com/sirupsen/logrus"
)

func DeployContract(address crypto.Address, sequence uint64, dataPath string) payload.CallTx {
	f, err := ioutil.ReadFile(dataPath)
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}
	data, err := hex.DecodeString(string(f))
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}

	payloadTx := payload.CallTx{
		Input: &payload.TxInput{
			Address:  address,
			Amount:   1,
			Sequence: sequence,
		},
		Fee:      1,
		GasLimit: 100000000,
		Data:     data,
	}
	return payloadTx
}
