package utils

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/hyperledger/burrow/acm"

	"github.com/sirupsen/logrus"
)

const contractMappingName = "contractMapping.txt"

var nilBytes []byte = nil

type TxsRW struct {
	path                string
	file                *os.File
	readWriter          *bufio.ReadWriter
	contractMappingFile *os.File
	contractMappingRW   *bufio.ReadWriter
}

func FatalError(err error) {
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}
}

func CreateTxsRW(path string) *TxsRW {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR, 0600)
	if os.IsNotExist(err) {
		file, err = os.Create(path)
	}
	FatalError(err)
	contractMappingFile, err := os.OpenFile(contractMappingName, os.O_APPEND|os.O_RDWR, 0600)
	if os.IsNotExist(err) {
		logrus.Infof("No file named %v", contractMappingName)
		// contractMappingFile, err = os.Create(contractMappingName)
	} else {
		FatalError(err)
	}

	writer := bufio.NewWriter(file)
	reader := bufio.NewReader(file)
	readWriter := bufio.NewReadWriter(reader, writer)

	contractMappingW := bufio.NewWriter(contractMappingFile)
	contractMappingR := bufio.NewReader(contractMappingFile)
	contractMappingRW := bufio.NewReadWriter(contractMappingR, contractMappingW)
	return &TxsRW{
		path:                path,
		file:                file,
		readWriter:          readWriter,
		contractMappingFile: contractMappingFile,
		contractMappingRW:   contractMappingRW,
	}
}
func (t *TxsRW) Close() {
	t.readWriter.Flush()
	t.file.Close()
	t.contractMappingRW.Flush()
	t.contractMappingFile.Close()
}

func (t *TxsRW) SaveTx(from, to, data []byte, amount, gas *big.Int, gasPrice, shouldFail uint64) {
	fmt.Fprintf(t.readWriter, "%x %x %x %d %d %d %v\n", from, to, data, amount, gas, gasPrice, shouldFail)
}

func (t *TxsRW) SaveTxCreateContract(from, to, contractId []byte, amount, gas *big.Int, gasPrice, shouldFail uint64) {
	fmt.Fprintf(t.readWriter, "%x %x %x %d %d %d %v\n", from, nilBytes, nilBytes, amount, gas, gasPrice, shouldFail)
	// Create contract file:
	_, err := os.Create("contracts/" + hex.EncodeToString(contractId) + ".txt")
	fmt.Fprintf(t.contractMappingRW, "%x %x\n", to, contractId)
	FatalError(err)
}

type Transaction struct {
	From            []byte
	To              []byte
	Data            []byte
	Amount          *big.Int
	Gas             *big.Int
	GasPrice        uint64
	ShouldNotRevert bool
}

func (t *TxsRW) LoadTx() (*Transaction, error) {
	tx := Transaction{}

	line, err := t.readWriter.ReadString('\n')
	if err != nil {
		return nil, err
	}
	splitLine := strings.Split(line, " ")

	if splitLine[0] != "" {
		tx.From = common.HexToAddress(splitLine[0]).Bytes()
	}
	if splitLine[1] != "" {
		tx.To = common.HexToAddress(splitLine[1]).Bytes()
	}
	if splitLine[2] != "" {
		tx.Data, err = hex.DecodeString(splitLine[2])
		if err != nil {
			return nil, err
		}
	}
	tx.Amount = new(big.Int)
	var failed bool
	tx.Amount, failed = tx.Amount.SetString(splitLine[3], 10)
	if !failed {
		return nil, err
	}
	tx.Gas = new(big.Int)
	tx.Gas, failed = tx.Amount.SetString(splitLine[4], 10)
	if !failed {
		return nil, err
	}
	tx.GasPrice, err = strconv.ParseUint(splitLine[5], 10, 64)
	if err != nil {
		return nil, err
	}
	shouldFail, err := strconv.ParseUint(splitLine[6][:len(splitLine[6])-1], 10, 64)
	if err != nil {
		return nil, err
	}
	tx.ShouldNotRevert = (shouldFail == 1)
	return &tx, err
}

type SimulatedSender struct {
	senders          map[[20]byte]*acm.PrivateAccount
	lastSenderID     int
	createdContracts map[[20]byte]int64
	lastContractID   int64
}

func NewSimulatedSender() *SimulatedSender {
	// acc := acm.SigningAccounts([]*acm.PrivateAccount{acm.GeneratePrivateAccountFromSecret("0")})[0]
	return &SimulatedSender{
		senders:          make(map[[20]byte]*acm.PrivateAccount),
		lastSenderID:     0,
		createdContracts: make(map[[20]byte]int64),
	}
}

func (s *SimulatedSender) GetOrMake(address common.Address) *acm.PrivateAccount {
	if val, ok := s.senders[address]; ok {
		return val
	}
	acc := acm.GeneratePrivateAccountFromSecret(strconv.Itoa(s.lastSenderID))
	s.senders[address] = acc
	s.lastSenderID++
	return s.senders[address]
}

func (s *SimulatedSender) ShouldCreateContract(address common.Address) (bool, int64) {
	contractID := s.createdContracts[address]
	if contractID == 0 {
		s.lastContractID++
		s.createdContracts[address] = s.lastContractID
		return true, s.lastContractID
	}
	return false, contractID
}
