package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"strconv"
	"strings"

	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/txs/payload"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/hyperledger/burrow/txs"
)

func fatalError(err error) {
	if err != nil {
		// logrus.Fatalf("Error: %v", err)
	}
}

type LogsReader struct {
	logsPath     string
	logsFile     *os.File
	logsReader   *bufio.Reader
	abi          abi.ABI
	contractAddr *crypto.Address
	Accounts
}

func CreateLogsReader(chainID string, path string, abiPath string) *LogsReader {
	file, err := os.Open(path)
	fatalError(err)
	// CK ABI
	ckABIjson, err := os.Open(abiPath)
	fatalError(err)
	ckABI, err := abi.JSON(ckABIjson)

	reader := bufio.NewReader(file)
	return &LogsReader{
		logsPath:   path,
		logsFile:   file,
		logsReader: reader,
		abi:        ckABI,
		Accounts: Accounts{
			chainID:    chainID,
			accountMap: make(map[common.Address]*SeqAccount),
			allowedMap: make(map[crypto.Address]map[int64]crypto.Address),
		},
	}
}

func (lr *LogsReader) SetContractAddr(addr *crypto.Address) {
	lr.contractAddr = addr
}

func (lr *LogsReader) Advance(n int) {
	for i := 0; i < n; i++ {
		lr.logsReader.ReadString('\n')
	}
}

var l int = 3

func (lr *LogsReader) LoadNextLog() (*payload.CallTx, *SeqAccount, error) {
	// logrus.Infof("LINE: %v", l)
	l++
	var fromAcc *SeqAccount
	tx := &payload.CallTx{
		Address:  lr.contractAddr,
		Fee:      1,
		GasLimit: 4100000000,
	}

	line, err := lr.logsReader.ReadString('\n')
	if err != nil {
		return nil, nil, err
	}
	splitLine := strings.Split(line, " ")
	// 0      1           2            3         4          5          6         7        8        9          10
	// Birth owner <addr [20]byte> kittyId <kID uint32> matronId <mID uint32> sireId <sID uint32> genes <genes uint256>
	if splitLine[0] == "Birth" {
		owner := common.HexToAddress(splitLine[2])
		simulatedOwner := lr.getOrCreateAccount(owner)
		matronID, err := strconv.ParseInt(splitLine[6], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		sireID, err := strconv.Atoi(splitLine[8])
		if err != nil {
			return nil, nil, err
		}
		genes, succ := big.NewInt(0).SetString(splitLine[10], 10)
		if !succ {
			return nil, nil, err
		}

		// Should call createPromoKitty(uint256 _genes, address _owner)
		if matronID == 0 && sireID == 0 {
			// From contract owner
			fromAcc = lr.getOrCreateAccount(common.BigToAddress(common.Big0))
			simulatedOwner = lr.getOrCreateAccount(common.BigToAddress(common.Big0))
			// logrus.Infof("createPromoKitty")
			txInput, err := lr.abi.Methods["createPromoKitty"].Inputs.Pack(genes, simulatedOwner.account.GetAddress())
			if err != nil {
				return nil, nil, err
			}
			tx.Data = append(lr.abi.Methods["createPromoKitty"].Id(), txInput...)
		} else {
			// From simulated owner (givin birth)
			fromAcc = simulatedOwner
			// logrus.Infof("giveBirth")
			// Should call giveBirth(uint256 _matronId)
			txInput, err := lr.abi.Methods["giveBirth"].Inputs.Pack(big.NewInt(matronID))
			if err != nil {
				return nil, nil, err
			}
			tx.Data = append(lr.abi.Methods["giveBirth"].Id(), txInput...)
		}
		// Consume Transfer event
		lr.logsReader.ReadString('\n')
		l++

		// 0          1           2             3          4        5         6                7          8
		// Pregnant owner <addr [20]byte>  matronId <mID uint32> sireId <sID uint32> cooldownEndBlock <cooldownEndBlock uint32>
	} else if splitLine[0] == "Pregnant" {
		// logrus.Infof("breed")
		owner := common.HexToAddress(splitLine[2])
		simulatedOwner := lr.getOrCreateAccount(owner)
		matronID, err := strconv.ParseInt(splitLine[4], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		sireID, err := strconv.ParseInt(splitLine[6], 10, 64)
		if err != nil {
			return nil, nil, err
		}

		// Should call breed(uint256 _matronId, uint256 _sireId)
		txInput, err := lr.abi.Methods["breed"].Inputs.Pack(big.NewInt(matronID), big.NewInt(sireID))
		tx.Data = append(lr.abi.Methods["breed"].Id(), txInput...)
		if err != nil {
			return nil, nil, err
		}
		fromAcc = simulatedOwner
		//              0      1    2    3    4      5        6
		// Approval/Transfer from <addr> to <addr> tokenId <tokenID>
	} else if splitLine[0] == "Transfer" || splitLine[0] == "Approval" {
		fr := common.HexToAddress(splitLine[2])
		to := common.HexToAddress(splitLine[4])
		simulatedTo := lr.getOrCreateAccount(to)
		simulatedFrom := lr.getOrCreateAccount(fr)

		tokenID, err := strconv.ParseInt(splitLine[6], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		if splitLine[0] == "Approval" {
			fromAcc = simulatedFrom
			// logrus.Infof("approve")
			lr.addAllowed(simulatedFrom.account.GetAddress(), simulatedTo.account.GetAddress(), tokenID)
			txInput, err := lr.abi.Methods["approve"].Inputs.Pack(simulatedTo.account.GetAddress(), big.NewInt(tokenID))
			tx.Data = append(lr.abi.Methods["approve"].Id(), txInput...)
			if err != nil {
				return nil, nil, err
			}
		} else {
			// Should call transferFrom(address _from, address _to, uint256 _tokenId)
			if lr.isAllowed(simulatedFrom.account.GetAddress(), tokenID) {
				fromAllowed := lr.allowedMap[simulatedFrom.account.GetAddress()][tokenID].Bytes()
				fromAcc = lr.getOrCreateAccount(common.BytesToAddress(fromAllowed))
				// logrus.Infof("transferFrom")
				txInput, err := lr.abi.Methods["transferFrom"].Inputs.Pack(simulatedFrom.account.GetAddress(), simulatedTo.account.GetAddress(), big.NewInt(tokenID))
				if err != nil {
					return nil, nil, err
				}
				tx.Data = append(lr.abi.Methods["transferFrom"].Id(), txInput...)
				lr.deleteAllowed(simulatedTo.account.GetAddress(), tokenID)
				// Should call transfer(address _to, uint256 _tokenId))
			} else {
				fromAcc = simulatedFrom
				// logrus.Infof("transfer")
				txInput, err := lr.abi.Methods["transfer"].Inputs.Pack(simulatedTo.account.GetAddress(), big.NewInt(tokenID))
				if err != nil {
					return nil, nil, err
				}
				tx.Data = append(lr.abi.Methods["transfer"].Id(), txInput...)
			}
		}
	} else {
		return nil, nil, fmt.Errorf("Error, unknown event %v", splitLine[0])
	}

	// txPayload := payload.Payload(tx)
	// fromAcc.sequence++
	tx.Input = &payload.TxInput{
		Address: fromAcc.account.GetAddress(),
		Amount:  1,
		// Sequence: fromAcc.sequence,
	}

	// env := txs.Enclose(lr.chainID, txPayload)
	// err = env.Sign(fromAcc.account)

	return tx, fromAcc, err
}

func (lr *LogsReader) SignTx(tx *payload.CallTx, from *SeqAccount) (*txs.Envelope, error) {
	txPayload := payload.Payload(tx)
	from.sequence++
	tx.Input.Sequence = from.sequence
	env := txs.Enclose(lr.chainID, txPayload)
	err := env.Sign(from.account)
	return env, err
}

type SeqAccount struct {
	account  acm.AddressableSigner
	sequence uint64
}

type Accounts struct {
	chainID       string
	accountMap    map[common.Address]*SeqAccount
	lastAccountID int
	allowedMap    map[crypto.Address]map[int64]crypto.Address
}

func (ac *Accounts) getOrCreateAccount(addr common.Address) *SeqAccount {
	if val, ok := ac.accountMap[addr]; ok {
		return val
	}
	acc := acm.GeneratePrivateAccountFromSecret(strconv.Itoa(ac.lastAccountID))
	ac.accountMap[addr] = &SeqAccount{
		account: acm.SigningAccounts([]*acm.PrivateAccount{acc})[0],
	}
	ac.lastAccountID++
	return ac.accountMap[addr]

}

func (ac *Accounts) addAllowed(from, to crypto.Address, tokenID int64) {
	if _, ok := ac.allowedMap[from]; !ok {
		ac.allowedMap[from] = make(map[int64]crypto.Address)
	}
	ac.allowedMap[from][tokenID] = to
}

func (ac *Accounts) deleteAllowed(addr crypto.Address, tokenID int64) {
	delete(ac.allowedMap[addr], tokenID)
}

func (ac *Accounts) isAllowed(addr crypto.Address, tokenID int64) bool {
	val, ok := ac.allowedMap[addr]
	if !ok {
		return false
	}
	_, ok = val[tokenID]
	if ok {
		return true
	}
	return false
}

// CreateContract creates a contract with the binary in path
func (lr *LogsReader) CreateContract(codePath string, args ...interface{}) (*txs.Envelope, error) {
	var byteArgs []byte
	var err error
	if len(args) != 0 {
		byteArgs, err = lr.abi.Constructor.Inputs.Pack(args...)
		if err != nil {
			return nil, err
		}
	}
	acc := lr.getOrCreateAccount(common.BigToAddress(common.Big0))
	contractContents, err := ioutil.ReadFile(codePath)
	if err != nil {
		return nil, err
	}

	contractHex, err := hex.DecodeString(string(contractContents))
	if err != nil {
		return nil, err
	}

	tx := payload.Payload(&payload.CallTx{
		Input: &payload.TxInput{
			Address:  acc.account.GetAddress(),
			Amount:   1,
			Sequence: acc.sequence + 1,
		},
		Data:     append(contractHex, byteArgs...),
		Address:  nil,
		Fee:      1,
		GasLimit: 4100000000,
	})

	env := txs.Enclose(lr.chainID, tx)
	err = env.Sign(acc.account)
	acc.sequence++

	return env, err
}
