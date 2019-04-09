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

	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/dependencies"
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
	dependencies.Accounts
}

func CreateLogsReader(path string, abiPath string, kittyABIPath string) *LogsReader {
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
		Accounts:   dependencies.NewAccounts(),
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

func (lr *LogsReader) ChangeIDs(txResponse *dependencies.TxResponse, idMap map[int64]int64) {
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

func (lr *LogsReader) LogsLoader() chan *dependencies.TxResponse {
	txsChan := make(chan *dependencies.TxResponse)

	go func() {
		for {
			txResponse := dependencies.TxResponse{
				Tx: &payload.CallTx{
					Address:  lr.contractAddr,
					Fee:      1,
					GasLimit: 4100000000,
				},
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
				simulatedOwner := lr.GetOrCreateAccount(owner)
				matronID, _ := strconv.ParseInt(splitLine[6], 10, 64)
				sireID, _ := strconv.ParseInt(splitLine[8], 10, 64)
				genes, _ := big.NewInt(0).SetString(splitLine[10], 10)

				// Should call createPromoKitty(uint256 _genes, address _owner)
				if matronID == 0 && sireID == 0 {
					// From contract owner
					txResponse.Signer = lr.GetOrCreateAccount(common.BigToAddress(common.Big0))
					debugf("createPromoKitty %v", kittyID)
					txResponse.MethodName = "createPromoKitty"
					txResponse.BigIntArgument = genes
					txResponse.AddressArgument = []common.Address{common.BytesToAddress(simulatedOwner.Account.GetAddress().Bytes())}
					txResponse.OriginalIds = []int64{kittyID}
					txResponse.OriginalBirthID = kittyID
				} else {
					// From simulated owner (giving birth)
					txResponse.Signer = simulatedOwner
					debugf("giveBirth %v from: %v and %v owner: %v", kittyID, matronID, sireID, simulatedOwner.Account.GetAddress())
					// Should call giveBirth(uint256 _matronId)
					txResponse.MethodName = "giveBirth"
					txResponse.OriginalIds = []int64{matronID, sireID, kittyID}
					txResponse.OriginalBirthID = kittyID
				}
				// Consume Transfer event
				lr.logsReader.ReadString('\n')
				lr.TokenOwnerMap[kittyID] = owner

				// 0          1           2             3          4        5         6                7          8
				// Pregnant owner <addr [20]byte>  matronId <mID uint32> sireId <sID uint32> cooldownEndBlock <cooldownEndBlock uint32>
			} else if splitLine[0] == "Pregnant" {
				owner := common.HexToAddress(splitLine[2])
				simulatedOwner := lr.GetOrCreateAccount(owner)
				matronID, _ := strconv.ParseInt(splitLine[4], 10, 64)
				sireID, _ := strconv.ParseInt(splitLine[6], 10, 64)
				if bytes.Compare(lr.TokenOwnerMap[matronID].Bytes(), owner.Bytes()) != 0 {
					logrus.Fatalf("Trying to breed non-owned token %x %x", lr.TokenOwnerMap[matronID].Bytes(), owner.Bytes())
				}

				// Should call approveSiring(address _addr, uint256 _sireId)
				if bytes.Compare(lr.TokenOwnerMap[sireID].Bytes(), owner.Bytes()) != 0 {
					approveSiringTx := dependencies.TxResponse{
						Tx: &payload.CallTx{
							Address:  lr.contractAddr,
							Fee:      1,
							GasLimit: 4100000000,
						},
					}
					simulatedSireOwner := lr.GetOrCreateAccount(lr.TokenOwnerMap[sireID])
					approveSiringTx.Signer = simulatedSireOwner
					debugf("approveSiring %v", sireID)
					approveSiringTx.MethodName = "approveSiring"
					approveSiringTx.AddressArgument = []common.Address{common.BytesToAddress(simulatedOwner.Account.GetAddress().Bytes())}
					approveSiringTx.Tx.Input = &payload.TxInput{
						Address: simulatedSireOwner.Account.GetAddress(),
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
				simulatedTo := lr.GetOrCreateAccount(to)
				simulatedFrom := lr.GetOrCreateAccount(fr)

				tokenID, _ := strconv.ParseInt(splitLine[6], 10, 64)
				if splitLine[0] == "Approval" {
					txResponse.Signer = simulatedFrom
					lr.AddAllowed(simulatedFrom.Account.GetAddress(), to, tokenID)
					debugf("approve from: %v to: %v token: %v", simulatedFrom.Account.GetAddress(), simulatedTo.Account.GetAddress(), tokenID)
					txResponse.MethodName = "approve"
					txResponse.AddressArgument = []common.Address{common.BytesToAddress(simulatedTo.Account.GetAddress().Bytes())}
				} else {
					// Should call transferFrom(address _from, address _to, uint256 _tokenId)
					if lr.IsAllowed(simulatedFrom.Account.GetAddress(), tokenID) {
						fromAllowed := lr.AllowedMap[simulatedFrom.Account.GetAddress()][tokenID]
						txResponse.Signer = lr.GetOrCreateAccount(fromAllowed)
						debugf("transferFrom sender: %v from: %v to: %v token: %v", txResponse.Signer.Account.GetAddress(), simulatedFrom.Account.GetAddress(), simulatedTo.Account.GetAddress(), tokenID)
						txResponse.MethodName = "transferFrom"
						txResponse.AddressArgument = []common.Address{common.BytesToAddress(simulatedFrom.Account.GetAddress().Bytes()),
							common.BytesToAddress(simulatedTo.Account.GetAddress().Bytes())}
						lr.DeleteAllowed(simulatedTo.Account.GetAddress(), tokenID)
						// Should call transfer(address _to, uint256 _tokenId))
					} else {
						txResponse.Signer = simulatedFrom
						debugf("transfer %v -> %v %v", simulatedFrom.Account.GetAddress(), simulatedTo.Account.GetAddress(), tokenID)
						txResponse.MethodName = "transfer"
						txResponse.AddressArgument = []common.Address{common.BytesToAddress(simulatedTo.Account.GetAddress().Bytes())}
					}
					lr.TokenOwnerMap[tokenID] = to
				}
				txResponse.OriginalIds = []int64{tokenID}
			} else {
				logrus.Fatalf("Error, unknown event %v", splitLine[0])
			}

			txResponse.Tx.Input = &payload.TxInput{
				Address: txResponse.Signer.Account.GetAddress(),
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

// CreateContract creates a contract with the binary in path
func (lr *LogsReader) CreateContract(chainID, codePath string, args ...interface{}) (*txs.Envelope, error) {
	var byteArgs []byte
	var err error
	if len(args) != 0 {
		byteArgs, err = lr.abi.Constructor.Inputs.Pack(args...)
		if err != nil {
			return nil, err
		}
	}
	acc := lr.GetOrCreateAccount(common.BigToAddress(common.Big0))
	contractContents, err := ioutil.ReadFile(codePath)
	if err != nil {
		return nil, err
	}

	contractHex, err := hex.DecodeString(string(contractContents))
	if err != nil {
		return nil, err
	}

	partitionID, _ := strconv.Atoi(chainID)
	partitionID--
	acc.PartitionIDSequence[partitionID]++
	tx := payload.Payload(&payload.CallTx{
		Input: &payload.TxInput{
			Address:  acc.Account.GetAddress(),
			Amount:   1,
			Sequence: acc.PartitionIDSequence[partitionID],
		},
		Data:     append(contractHex, byteArgs...),
		Address:  nil,
		Fee:      1,
		GasLimit: 4100000000,
	})

	env := txs.Enclose(chainID, tx)
	err = env.Sign(acc.Account)

	return env, err
}
