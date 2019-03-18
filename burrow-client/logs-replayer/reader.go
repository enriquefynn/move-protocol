package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"io/ioutil"
	"math/big"
	"os"
	"strconv"
	"strings"

	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/execution/exec"
	"github.com/hyperledger/burrow/txs/payload"
	"github.com/sirupsen/logrus"

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
			tokenMap:   make(map[int]common.Address),
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

type TxResponse struct {
	chainID         string
	tx              *payload.CallTx
	signer          *SeqAccount
	methodName      string
	originalIds     []int
	originalBirthID int
}

func (tr *TxResponse) Sign() *txs.Envelope {
	txPayload := payload.Payload(tr.tx)
	tr.signer.sequence++
	tr.tx.Input.Sequence = tr.signer.sequence
	env := txs.Enclose(tr.chainID, txPayload)
	env.Sign(tr.signer.account)
	return env
}

func debugf(format string, a ...interface{}) {
	logrus.Infof(format, a...)
}

func (lr *LogsReader) LogsLoader() chan *TxResponse {
	txsChan := make(chan *TxResponse)

	go func() {
		for {
			txResponse := TxResponse{
				chainID: lr.chainID,
			}

			txResponse.tx = &payload.CallTx{
				Address:  lr.contractAddr,
				Fee:      1,
				GasLimit: 4100000000,
			}

			line, err := lr.logsReader.ReadString('\n')
			if err != nil {
				close(txsChan)
			}
			splitLine := strings.Split(line, " ")
			// 0      1           2            3         4          5          6         7        8        9          10
			// Birth owner <addr [20]byte> kittyId <kID uint32> matronId <mID uint32> sireId <sID uint32> genes <genes uint256>
			if splitLine[0] == "Birth" {
				owner := common.HexToAddress(splitLine[2])
				kittyID, _ := strconv.Atoi(splitLine[4])
				simulatedOwner := lr.getOrCreateAccount(owner)
				matronID, _ := strconv.ParseInt(splitLine[6], 10, 64)
				sireID, _ := strconv.Atoi(splitLine[8])
				genes, _ := big.NewInt(0).SetString(splitLine[10], 10)

				// Should call createPromoKitty(uint256 _genes, address _owner)
				if matronID == 0 && sireID == 0 {
					// From contract owner
					txResponse.signer = lr.getOrCreateAccount(common.BigToAddress(common.Big0))
					debugf("createPromoKitty %v", kittyID)
					txResponse.methodName = "createPromoKitty"
					txInput, err := lr.abi.Methods["createPromoKitty"].Inputs.Pack(genes, simulatedOwner.account.GetAddress())
					fatalError(err)
					txResponse.tx.Data = append(lr.abi.Methods["createPromoKitty"].Id(), txInput...)
					txResponse.originalIds = []int{int(kittyID)}
					txResponse.originalBirthID = int(kittyID)
				} else {
					// From simulated owner (givin birth)
					txResponse.signer = simulatedOwner
					debugf("giveBirth %v from: %v and %v owner: %v", kittyID, matronID, sireID, simulatedOwner.account.GetAddress())
					// Should call giveBirth(uint256 _matronId)
					txResponse.methodName = "giveBirth"
					txInput, err := lr.abi.Methods["giveBirth"].Inputs.Pack(big.NewInt(matronID))
					fatalError(err)
					txResponse.tx.Data = append(lr.abi.Methods["giveBirth"].Id(), txInput...)
					txResponse.originalIds = []int{int(matronID), int(sireID), int(kittyID)}
					txResponse.originalBirthID = int(kittyID)
				}
				// Consume Transfer event
				lr.logsReader.ReadString('\n')
				lr.tokenMap[kittyID] = owner

				// 0          1           2             3          4        5         6                7          8
				// Pregnant owner <addr [20]byte>  matronId <mID uint32> sireId <sID uint32> cooldownEndBlock <cooldownEndBlock uint32>
			} else if splitLine[0] == "Pregnant" {
				owner := common.HexToAddress(splitLine[2])
				simulatedOwner := lr.getOrCreateAccount(owner)
				matronID, _ := strconv.ParseInt(splitLine[4], 10, 64)
				sireID, _ := strconv.ParseInt(splitLine[6], 10, 64)
				if bytes.Compare(lr.tokenMap[int(matronID)].Bytes(), owner.Bytes()) != 0 {
					logrus.Fatal("Trying to breed non-owned token")
				}

				// Should call approveSiring(address _addr, uint256 _sireId)
				if bytes.Compare(lr.tokenMap[int(sireID)].Bytes(), owner.Bytes()) != 0 {
					// logrus.Infof("approveSiring")
					approveSiringTx := TxResponse{
						chainID: lr.chainID,
					}

					approveSiringTx.tx = &payload.CallTx{
						Address:  lr.contractAddr,
						Fee:      1,
						GasLimit: 4100000000,
					}
					simulatedSireOwner := lr.getOrCreateAccount(lr.tokenMap[int(sireID)])
					approveSiringTx.signer = simulatedSireOwner
					debugf("approveSiring %v", sireID)
					approveSiringTx.methodName = "approveSiring"
					txInput, err := lr.abi.Methods["approveSiring"].Inputs.Pack(simulatedOwner.account.GetAddress(), big.NewInt(sireID))
					approveSiringTx.tx.Data = append(lr.abi.Methods["approveSiring"].Id(), txInput...)
					approveSiringTx.tx.Input = &payload.TxInput{
						Address: simulatedSireOwner.account.GetAddress(),
						Amount:  1,
						// Sequence: fromAcc.sequence,
					}
					fatalError(err)
					approveSiringTx.originalIds = []int{int(sireID)}
					txsChan <- &approveSiringTx
				}
				// Should call breed(uint256 _matronId, uint256 _sireId)
				debugf("breed %v %v", matronID, sireID)
				txResponse.methodName = "breed"
				txInput, err := lr.abi.Methods["breed"].Inputs.Pack(big.NewInt(matronID), big.NewInt(sireID))
				txResponse.tx.Data = append(lr.abi.Methods["breed"].Id(), txInput...)
				fatalError(err)
				txResponse.signer = simulatedOwner
				txResponse.originalIds = []int{int(matronID), int(sireID)}

				//              0      1    2    3    4      5        6
				// Approval/Transfer from <addr> to <addr> tokenId <tokenID>
			} else if splitLine[0] == "Transfer" || splitLine[0] == "Approval" {
				fr := common.HexToAddress(splitLine[2])
				to := common.HexToAddress(splitLine[4])
				simulatedTo := lr.getOrCreateAccount(to)
				simulatedFrom := lr.getOrCreateAccount(fr)

				tokenID, _ := strconv.ParseInt(splitLine[6], 10, 64)
				if splitLine[0] == "Approval" {
					txResponse.signer = simulatedFrom
					lr.addAllowed(simulatedFrom.account.GetAddress(), simulatedTo.account.GetAddress(), tokenID)
					debugf("approve %v", tokenID)
					txResponse.methodName = "approve"
					txInput, err := lr.abi.Methods["approve"].Inputs.Pack(simulatedTo.account.GetAddress(), big.NewInt(tokenID))
					txResponse.tx.Data = append(lr.abi.Methods["approve"].Id(), txInput...)
					fatalError(err)
				} else {
					// Should call transferFrom(address _from, address _to, uint256 _tokenId)
					if lr.isAllowed(simulatedFrom.account.GetAddress(), tokenID) {
						fromAllowed := lr.allowedMap[simulatedFrom.account.GetAddress()][tokenID].Bytes()
						txResponse.signer = lr.getOrCreateAccount(common.BytesToAddress(fromAllowed))
						debugf("transferFrom %v", tokenID)
						txResponse.methodName = "transferFrom"
						txInput, err := lr.abi.Methods["transferFrom"].Inputs.Pack(simulatedFrom.account.GetAddress(), simulatedTo.account.GetAddress(), big.NewInt(tokenID))
						fatalError(err)
						txResponse.tx.Data = append(lr.abi.Methods["transferFrom"].Id(), txInput...)
						lr.deleteAllowed(simulatedTo.account.GetAddress(), tokenID)
						// Should call transfer(address _to, uint256 _tokenId))
					} else {
						txResponse.signer = simulatedFrom
						debugf("transfer %v -> %v %v", simulatedFrom.account.GetAddress(), simulatedTo.account.GetAddress(), tokenID)
						txResponse.methodName = "transfer"
						txInput, err := lr.abi.Methods["transfer"].Inputs.Pack(simulatedTo.account.GetAddress(), big.NewInt(tokenID))
						fatalError(err)
						txResponse.tx.Data = append(lr.abi.Methods["transfer"].Id(), txInput...)
					}
					lr.tokenMap[int(tokenID)] = to
				}
				txResponse.originalIds = []int{int(tokenID)}
			} else {
				logrus.Fatalf("Error, unknown event %v", splitLine[0])
			}

			// txPayload := payload.Payload(tx)
			// fromAcc.sequence++
			txResponse.tx.Input = &payload.TxInput{
				Address: txResponse.signer.account.GetAddress(),
				Amount:  1,
				// Sequence: fromAcc.sequence,
			}

			// env := txs.Enclose(lr.chainID, txPayload)
			// err = env.Sign(fromAcc.account)

			txsChan <- &txResponse
			// return &txResponse, err
		}
	}()
	return txsChan
}

func (lr *LogsReader) extractIDTransfer(event *exec.Event) int {
	kittyID := common.Big0
	kittyID.SetBytes(event.Log.Data[len(event.Log.Data)-32:])
	return int(kittyID.Int64())
}

func (lr *LogsReader) extractIDPregnant(event *exec.Event) []int {
	matronID := big.NewInt(0)
	sireID := big.NewInt(0)
	// 32*3 matronId
	// 32*2 sireId
	matronID.SetBytes(event.Log.Data[len(event.Log.Data)-96 : len(event.Log.Data)-64])
	sireID.SetBytes(event.Log.Data[len(event.Log.Data)-64 : len(event.Log.Data)-32])
	return []int{int(matronID.Int64()), int(sireID.Int64())}
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
	tokenMap      map[int]common.Address
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
