package utils

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/hyperledger/burrow/acm"

	"github.com/sirupsen/logrus"
)

type TxsRW struct {
	path       string
	file       *os.File
	readWriter *bufio.ReadWriter
}

func (t *TxsRW) Open() {

}

func FatalError(err error) {
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}
}

func CreateTxsRW(path string) *TxsRW {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR, 0600)
	FatalError(err)

	writer := bufio.NewWriter(file)
	reader := bufio.NewReader(file)
	readWriter := bufio.NewReadWriter(reader, writer)
	return &TxsRW{
		path:       path,
		file:       file,
		readWriter: readWriter,
	}
}
func (t *TxsRW) Close() {
	t.readWriter.Flush()
	t.file.Close()
}

func (t *TxsRW) SaveTx(from, to, data []byte, amount, gas, gasPrice, shouldFail uint64) {
	fmt.Fprintf(t.readWriter, "%x %x %x %d %d %d %v\n", from, to, data, amount, gas, gasPrice, shouldFail)
}

func (t *TxsRW) LoadTx() (from, to, data []byte, amount, gas, gasPrice, shouldFail uint64, err error) {
	line, err := t.readWriter.ReadString('\n')
	if err != nil {
		return
	}
	splitLine := strings.Split(line, " ")

	if splitLine[0] == "" {
		from = nil
	} else {
		from = common.HexToAddress(splitLine[0]).Bytes()
	}
	if splitLine[1] == "" {
		to = nil
	} else {
		to = common.HexToAddress(splitLine[1]).Bytes()
	}
	if splitLine[2] == "" {
		data = nil
	} else {
		data = common.HexToAddress(splitLine[2]).Bytes()
	}
	amount, err = strconv.ParseUint(splitLine[3], 10, 64)
	if err != nil {
		return
	}
	gas, err = strconv.ParseUint(splitLine[4], 10, 64)
	if err != nil {
		return
	}
	gasPrice, err = strconv.ParseUint(splitLine[5], 10, 64)
	if err != nil {
		return
	}
	shouldFail, err = strconv.ParseUint(splitLine[6][:len(splitLine[6])-1], 10, 64)
	return
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
