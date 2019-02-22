package utils

import (
	"encoding/hex"
	"io/ioutil"
	"strconv"

	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/binary"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/deploy/def"
	"github.com/hyperledger/burrow/txs/payload"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Contracts struct {
		Deploy  bool   `yaml:"deploy"`
		Path    string `yaml:"path"`
		Address string `yaml:"address"`
	}
	Benchmark struct {
		Clients int    `yaml:"clients"`
		Timeout int    `yaml:"timeout"`
		Address string `yaml:"address"`
	}
}

type Statistics struct {
	ClientAddress string
	Calls         int
}

func (s *Statistics) Reset() {
	s.Calls = 0
}

func NewStatistics(clientAddress string) *Statistics {
	return &Statistics{
		ClientAddress: clientAddress,
		Calls:         0,
	}
}

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

func CallContractTx(callingAddress crypto.Address, contractAddress *crypto.Address, sequence uint64, data binary.HexBytes, shouldFail byte) payload.CallTx {
	payloadTx := payload.CallTx{
		Input: &payload.TxInput{
			Address:  callingAddress,
			Amount:   10000,
			Sequence: sequence,
		},
		Fee:      1,
		GasLimit: 10000,
		Data:     data,
		Address:  contractAddress,
		// ShouldFail: shouldFail,
	}
	return payloadTx
}

func GetSignedAccounts(nAccounts int) [][]acm.AddressableSigner {
	signingAccounts := make([][]acm.AddressableSigner, nAccounts)
	tmpAccounts := make([]*acm.PrivateAccount, 1)
	for accIdx := 0; accIdx < nAccounts; accIdx++ {
		tmpAccounts[0] = acm.GeneratePrivateAccountFromSecret(strconv.Itoa(accIdx))
		signingAccounts[accIdx] = acm.SigningAccounts(tmpAccounts)
	}
	return signingAccounts
}

func GetSignedAndUpdatedAccounts(client *def.Client, nAccounts int) ([][]acm.AddressableSigner, []*acm.Account) {
	signingAccounts := GetSignedAccounts(nAccounts)
	accounts := make([]*acm.Account, nAccounts)

	for accIdx := 0; accIdx < nAccounts; accIdx++ {
		acc, err := client.GetAccount(signingAccounts[accIdx][0].GetAddress())
		if err != nil {
			logrus.Fatalf("Error: %v", err)
		}
		accounts[accIdx] = acc
	}
	return signingAccounts, accounts
}
