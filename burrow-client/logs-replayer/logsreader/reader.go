package logsreader

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
		logrus.Fatalf("Error: %v", err)
	}
}

type LogsReader struct {
	logsPath     string
	logsFile     *os.File
	logsReader   *bufio.Reader
	abi          abi.ABI
	kittyABI     abi.ABI
	contractAddr *crypto.Address
	Accounts
}

func CreateLogsReader(chainID string, path string, abiPath string, kittyABIPath string) *LogsReader {
	file, err := os.Open(path)
	fatalError(err)
	// CK ABI
	ckABIjson, err := os.Open(abiPath)
	fatalError(err)
	ckABI, err := abi.JSON(ckABIjson)
	fatalError(err)

	kittyABIjson, err := os.Open(kittyABIPath)
	fatalError(err)
	kittyABI, err := abi.JSON(kittyABIjson)
	fatalError(err)

	reader := bufio.NewReader(file)
	return &LogsReader{
		logsPath:   path,
		logsFile:   file,
		logsReader: reader,
		abi:        ckABI,
		kittyABI:   kittyABI,
		Accounts: Accounts{
			chainID:    chainID,
			accountMap: make(map[common.Address]*SeqAccount),
			allowedMap: make(map[crypto.Address]map[int64]common.Address),
			tokenMap:   make(map[int64]common.Address),
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
	ChainID         string
	Tx              *payload.CallTx
	Signer          *SeqAccount
	MethodName      string
	OriginalIds     []int64
	OriginalBirthID int64
	AddressArgument []common.Address
	BigIntArgument  *big.Int
}

func (tr *TxResponse) Sign() *txs.Envelope {
	txPayload := payload.Payload(tr.Tx)
	tr.Signer.sequence++
	tr.Tx.Input.Sequence = tr.Signer.sequence
	env := txs.Enclose(tr.ChainID, txPayload)
	env.Sign(tr.Signer.account)
	return env
}

func (lr *LogsReader) ChangeIDs(txResponse *TxResponse, idMap map[int64]int64) {
	txResponse.Tx.Data = lr.abi.Methods[txResponse.MethodName].Id()

	if txResponse.MethodName == "createPromoKitty" {
		txInput, err := lr.abi.Methods["createPromoKitty"].Inputs.Pack(txResponse.BigIntArgument, txResponse.AddressArgument[0])
		fatalError(err)
		txResponse.Tx.Data = append(lr.abi.Methods["createPromoKitty"].Id(), txInput...)

		// giveBirth(uint256 _matronId)
	} else if txResponse.MethodName == "giveBirth" {
		matronID := txResponse.OriginalIds[0]
		if newID, ok := idMap[matronID]; ok {
			matronID = newID
		}

		txInput, err := lr.abi.Methods["giveBirth"].Inputs.Pack(big.NewInt(matronID))
		fatalError(err)
		txResponse.Tx.Data = append(lr.abi.Methods["giveBirth"].Id(), txInput...)

		// approveSiring(address _addr, uint256 _sireId)
		//      transfer(address _to, uint256 _tokenId)
		//       approve(address _to, uint256 _tokenId)
	} else if txResponse.MethodName == "approveSiring" {
		tokenID := txResponse.OriginalIds[0]
		if newID, ok := idMap[tokenID]; ok {
			tokenID = newID
		}

		txInput, err := lr.abi.Methods["approveSiring"].Inputs.Pack(txResponse.AddressArgument[0], big.NewInt(tokenID))
		fatalError(err)
		txResponse.Tx.Data = append(lr.abi.Methods["approveSiring"].Id(), txInput...)
	} else if txResponse.MethodName == "transfer" {
		tokenID := txResponse.OriginalIds[0]
		if newID, ok := idMap[tokenID]; ok {
			tokenID = newID
		}

		txInput, err := lr.abi.Methods["transfer"].Inputs.Pack(txResponse.AddressArgument[0], big.NewInt(tokenID))
		fatalError(err)
		txResponse.Tx.Data = append(lr.abi.Methods["transfer"].Id(), txInput...)
	} else if txResponse.MethodName == "approve" {
		tokenID := txResponse.OriginalIds[0]
		if newID, ok := idMap[tokenID]; ok {
			tokenID = newID
		}

		txInput, err := lr.abi.Methods["approve"].Inputs.Pack(txResponse.AddressArgument[0], big.NewInt(tokenID))
		fatalError(err)
		txResponse.Tx.Data = append(lr.abi.Methods["approve"].Id(), txInput...)
		// breed(uint256 _matronId, uint256 _sireId)
	} else if txResponse.MethodName == "breed" {
		matronID := txResponse.OriginalIds[0]
		if newID, ok := idMap[matronID]; ok {
			matronID = newID
		}
		sireID := txResponse.OriginalIds[1]
		if newID, ok := idMap[sireID]; ok {
			sireID = newID
		}
		txInput, err := lr.abi.Methods["breed"].Inputs.Pack(big.NewInt(matronID), big.NewInt(sireID))
		txResponse.Tx.Data = append(lr.abi.Methods["breed"].Id(), txInput...)
		fatalError(err)

		// transferFrom(address _from, address _to, uint256 _tokenId)
	} else if txResponse.MethodName == "transferFrom" {
		tokenID := txResponse.OriginalIds[0]
		if newID, ok := idMap[tokenID]; ok {
			tokenID = newID
		}
		txInput, err := lr.abi.Methods["transferFrom"].Inputs.Pack(txResponse.AddressArgument[0], txResponse.AddressArgument[1], big.NewInt(tokenID))
		fatalError(err)
		txResponse.Tx.Data = append(lr.abi.Methods["transferFrom"].Id(), txInput...)

	} else {
		logrus.Fatalf("Method not found %v", txResponse.MethodName)
	}
}

func debugf(format string, a ...interface{}) {
	// Dirty hack
	// logrus.Infof(format, a...)
}

func (lr *LogsReader) LogsLoader() chan *TxResponse {
	txsChan := make(chan *TxResponse)

	go func() {
		for {
			txResponse := TxResponse{
				ChainID: lr.chainID,
			}

			txResponse.Tx = &payload.CallTx{
				Address:  lr.contractAddr,
				Fee:      1,
				GasLimit: 4100000000,
			}

			line, err := lr.logsReader.ReadString('\n')
			if len(line) == 0 || err != nil {
				close(txsChan)
				break
			}
			splitLine := strings.Split(line, " ")
			// 0      1           2            3         4          5          6         7        8        9          10
			// Birth owner <addr [20]byte> kittyId <kID uint32> matronId <mID uint32> sireId <sID uint32> genes <genes uint256>
			if splitLine[0] == "Birth" {
				owner := common.HexToAddress(splitLine[2])
				kittyID, _ := strconv.ParseInt(splitLine[4], 10, 64)
				simulatedOwner := lr.getOrCreateAccount(owner)
				matronID, _ := strconv.ParseInt(splitLine[6], 10, 64)
				sireID, _ := strconv.ParseInt(splitLine[8], 10, 64)
				genes, _ := big.NewInt(0).SetString(splitLine[10], 10)

				// Should call createPromoKitty(uint256 _genes, address _owner)
				if matronID == 0 && sireID == 0 {
					// From contract owner
					txResponse.Signer = lr.getOrCreateAccount(common.BigToAddress(common.Big0))
					debugf("createPromoKitty %v", kittyID)
					txResponse.MethodName = "createPromoKitty"
					txResponse.BigIntArgument = genes
					txResponse.AddressArgument = []common.Address{common.BytesToAddress(simulatedOwner.account.GetAddress().Bytes())}
					txResponse.OriginalIds = []int64{kittyID}
					txResponse.OriginalBirthID = kittyID
				} else {
					// From simulated owner (givin birth)
					txResponse.Signer = simulatedOwner
					debugf("giveBirth %v from: %v and %v owner: %v", kittyID, matronID, sireID, simulatedOwner.account.GetAddress())
					// Should call giveBirth(uint256 _matronId)
					txResponse.MethodName = "giveBirth"
					txResponse.OriginalIds = []int64{matronID, sireID, kittyID}
					txResponse.OriginalBirthID = kittyID
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
				if bytes.Compare(lr.tokenMap[matronID].Bytes(), owner.Bytes()) != 0 {
					logrus.Fatal("Trying to breed non-owned token")
				}

				// Should call approveSiring(address _addr, uint256 _sireId)
				if bytes.Compare(lr.tokenMap[sireID].Bytes(), owner.Bytes()) != 0 {
					approveSiringTx := TxResponse{
						ChainID: lr.chainID,
					}

					approveSiringTx.Tx = &payload.CallTx{
						Address:  lr.contractAddr,
						Fee:      1,
						GasLimit: 4100000000,
					}
					simulatedSireOwner := lr.getOrCreateAccount(lr.tokenMap[sireID])
					approveSiringTx.Signer = simulatedSireOwner
					debugf("approveSiring %v", sireID)
					approveSiringTx.MethodName = "approveSiring"
					approveSiringTx.AddressArgument = []common.Address{common.BytesToAddress(simulatedOwner.account.GetAddress().Bytes())}
					approveSiringTx.Tx.Input = &payload.TxInput{
						Address: simulatedSireOwner.account.GetAddress(),
						Amount:  1,
					}
					approveSiringTx.OriginalIds = []int64{sireID}
					txsChan <- &approveSiringTx
				}
				// Should call breed(uint256 _matronId, uint256 _sireId)
				debugf("breed %v %v", matronID, sireID)
				txResponse.MethodName = "breed"
				txResponse.Signer = simulatedOwner
				txResponse.OriginalIds = []int64{matronID, sireID}

				//              0      1    2    3    4      5        6
				// Approval/Transfer from <addr> to <addr> tokenId <tokenID>
			} else if splitLine[0] == "Transfer" || splitLine[0] == "Approval" {
				fr := common.HexToAddress(splitLine[2])
				to := common.HexToAddress(splitLine[4])
				simulatedTo := lr.getOrCreateAccount(to)
				simulatedFrom := lr.getOrCreateAccount(fr)

				tokenID, _ := strconv.ParseInt(splitLine[6], 10, 64)
				if splitLine[0] == "Approval" {
					txResponse.Signer = simulatedFrom
					lr.addAllowed(simulatedFrom.account.GetAddress(), to, tokenID)
					debugf("approve from: %v to: %v token: %v", simulatedFrom.account.GetAddress(), simulatedTo.account.GetAddress(), tokenID)
					txResponse.MethodName = "approve"
					txResponse.AddressArgument = []common.Address{common.BytesToAddress(simulatedTo.account.GetAddress().Bytes())}
				} else {
					// Should call transferFrom(address _from, address _to, uint256 _tokenId)
					if lr.isAllowed(simulatedFrom.account.GetAddress(), tokenID) {
						fromAllowed := lr.allowedMap[simulatedFrom.account.GetAddress()][tokenID]
						txResponse.Signer = lr.getOrCreateAccount(fromAllowed)
						debugf("transferFrom sender: %v from: %v to: %v token: %v", txResponse.Signer.account.GetAddress(), simulatedFrom.account.GetAddress(), simulatedTo.account.GetAddress(), tokenID)
						txResponse.MethodName = "transferFrom"
						txResponse.AddressArgument = []common.Address{common.BytesToAddress(simulatedFrom.account.GetAddress().Bytes()),
							common.BytesToAddress(simulatedTo.account.GetAddress().Bytes())}
						lr.deleteAllowed(simulatedTo.account.GetAddress(), tokenID)
						// Should call transfer(address _to, uint256 _tokenId))
					} else {
						txResponse.Signer = simulatedFrom
						debugf("transfer %v -> %v %v", simulatedFrom.account.GetAddress(), simulatedTo.account.GetAddress(), tokenID)
						txResponse.MethodName = "transfer"
						txResponse.AddressArgument = []common.Address{common.BytesToAddress(simulatedTo.account.GetAddress().Bytes())}
					}
					lr.tokenMap[tokenID] = to
				}
				txResponse.OriginalIds = []int64{tokenID}
			} else {
				logrus.Fatalf("Error, unknown event %v", splitLine[0])
			}

			txResponse.Tx.Input = &payload.TxInput{
				Address: txResponse.Signer.account.GetAddress(),
				Amount:  1,
				// Sequence: fromAcc.sequence,
			}
			txsChan <- &txResponse
		}
	}()
	return txsChan
}

func (lr *LogsReader) ExtractIDTransfer(event *exec.Event) int64 {
	kittyID := common.Big0
	kittyID.SetBytes(event.Log.Data[len(event.Log.Data)-32:])
	return kittyID.Int64()
}

func (lr *LogsReader) ExtractIDPregnant(event *exec.Event) []int {
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
	allowedMap    map[crypto.Address]map[int64]common.Address
	tokenMap      map[int64]common.Address
}

func (ac *Accounts) getOrCreateAccount(addr common.Address) *SeqAccount {
	if val, ok := ac.accountMap[addr]; ok {
		return val
	}
	// logrus.Infof("Account id: %v", ac.lastAccountID)
	acc := acm.GeneratePrivateAccountFromSecret(strconv.Itoa(ac.lastAccountID))
	ac.accountMap[addr] = &SeqAccount{
		account: acm.SigningAccounts([]*acm.PrivateAccount{acc})[0],
	}
	ac.lastAccountID++
	return ac.accountMap[addr]

}

func (ac *Accounts) addAllowed(from crypto.Address, to common.Address, tokenID int64) {
	if _, ok := ac.allowedMap[from]; !ok {
		ac.allowedMap[from] = make(map[int64]common.Address)
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
