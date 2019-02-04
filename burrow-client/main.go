package main

import (
	"io/ioutil"
	"os"
	"strconv"

	"github.com/hyperledger/burrow/acm"
	"github.com/hyperledger/burrow/crypto"
	"github.com/hyperledger/burrow/deploy/def"
	"github.com/hyperledger/burrow/txs/payload"
	"github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
)

// func clientClosedLoop(key *ecdsa.PrivateKey, client *ethclient.Client, blockChan <-chan *types.Block) {
// 	ctx := context.Background()
// address := crypto.PubkeyToAddress(key.PublicKey)

// nonce, err := client.NonceAt(ctx, address, nil)
// if err != nil {
// 	log.Fatalf("Error getting nonce %v", err)
// }
// waitForTx := false

// var txHash common.Hash
// for {
// 	if !waitForTx {
// 		fmt.Printf("Sending tx client: %x nonce: %v\n", address, nonce)
// 		waitForTx = true
// 		tx := types.NewTransaction(nonce, common.HexToAddress("0xe1ab8145f7e55dc933d51a18c793f901a3a0b276"), big.NewInt(1), 1e5, big.NewInt(1), []byte{})
// 		signedTx, err := types.SignTx(tx, types.HomesteadSigner{}, key)
// 		txHash = signedTx.Hash()
// 		if err != nil {
// 			log.Fatalf("Error signing transaction: %v", err)
// 		}

// 		// log.Printf("HEY %v %v", client, signedTx)
// 		err = client.SendTransaction(ctx, signedTx)
// 		if err != nil {
// 			log.Printf("Could not send transaction: %v, increasing nonce", err)
// 		}
// 		nonce++
// 	}
// 	block := <-blockChan
// 	if block.Transaction(txHash) != nil {
// 		fmt.Printf("Tx in block for %x %v\n", address, block.Number())
// 		waitForTx = false
// 	} else {
// 		fmt.Printf("Tx not in block for %x\n", address)
// 	}
// }
// }

type Config struct {
	Contracts struct {
		Deploy  bool   `yaml:"deploy"`
		Path    string `yaml:"path"`
		Address string `yaml:"address"`
	}
	Benchmark struct {
		Clients int
	}
}

func checkFatalError(err error) {
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}
}

func clientClosedLoop(signingAccount []acm.AddressableSigner, client *def.Client, config Config) {
	contractAddress, err := crypto.AddressFromHexString(config.Contracts.Address)
	checkFatalError(err)

	logrus.Infof("PubKey: %v\n", signingAccount[0].GetPublicKey())

	addr := signingAccount[0].GetAddress()
	account, _ := client.GetAccount(addr)
	logrus.Infof("Account: %v\n", account)
	sequence := account.GetSequence() + 1

	// args := def.CallArg{}
	// tx, err := client.Call(&args)

	payloadTx := payload.CallTx{
		Input: &payload.TxInput{
			Address:  addr,
			Amount:   1,
			Sequence: sequence,
		},
		Fee:     1,
		Address: &contractAddress,
	}
	// if err != nil {
	// 	log.Fatalf("Error: %v\n", err)
	// }

	txExecution, err := client.SignTx(payload.Payload(&payloadTx))
	if err != nil {
		logrus.Fatalf("Error: %v\n", err)
	}
	logrus.Infof("TxExecution: %v\n", txExecution)
	_, err = client.BroadcastEnvelope(txExecution)
	if err != nil {
		logrus.Fatalf("Error: %v\n", err)
	}

	// fmt.Printf("Tx: %v\n", tx)
}

func main() {
	config := Config{}
	configFile, err := ioutil.ReadFile(os.Args[1])
	checkFatalError(err)
	err = yaml.Unmarshal(configFile, &config)
	checkFatalError(err)

	signingAccounts := make([][]acm.AddressableSigner, config.Benchmark.Clients)
	accounts := make([]*acm.Account, config.Benchmark.Clients)

	logrus.Infof("Generating %v accounts\n", config.Benchmark.Clients)
	tmpAccounts := make([]*acm.PrivateAccount, 1)
	for accIdx := 0; accIdx < config.Benchmark.Clients; accIdx++ {
		tmpAccounts[0] = acm.GeneratePrivateAccountFromSecret(strconv.Itoa(accIdx))
		signingAccounts[accIdx] = acm.SigningAccounts(tmpAccounts)
	}
	client := def.NewClientWithLocalSigning("127.0.0.1:10997", signingAccounts[0])
	for accIdx := 0; accIdx < config.Benchmark.Clients; accIdx++ {
		acc, err := client.GetAccount(signingAccounts[accIdx][0].GetAddress())
		checkFatalError(err)
		accounts[accIdx] = acc
	}

	// client2 := def.NewClientWithLocalSigning("127.0.0.1:10997", signingAccount)
	// logrus.Infof("PubKey: %v\n", signingAccount[0].GetPublicKey())

	// cli := def.NewClient("127.0.0.1:10997", "a", false)
	// val, _ := cli.GetValidatorSet()
	// fmt.Printf("Validator set: %v\n", val.Set)

	// query, _ := cli.Query()

	// infoclient.Block(cli, 0)

	if config.Contracts.Deploy {
		tx := DeployContract(signingAccounts[0][0].GetAddress(), accounts[0].Sequence+1, config.Contracts.Path)
		txExecution, err := client.SignTx(payload.Payload(&tx))
		checkFatalError(err)
		txReceipt, err := client.BroadcastEnvelope(txExecution)
		checkFatalError(err)
		config.Contracts.Address = txReceipt.Receipt.ContractAddress.String()
		logrus.Infof("Created contract: %v", config.Contracts.Address)
	}

	// clientClosedLoop(signingAccounts[0], client, config)

	// infoClient := client.NewJSONRPCClient("127.0.0.1:26658")
	// logrus.Infof("Client: %v\n", infoClient)
	// blockHeader, err := infoclient.SignedHeader(infoClient, 1)
	// if err != nil {
	// 	logrus.Fatalf("Error: %v\n", err)
	// } else if blockHeader.BlockMeta == nil || blockHeader.Commit == nil {
	// 	logrus.Fatal("Error: Null block\n")
	// }

	// // Block header
	// logrus.Infof("Block Header: %v\nSignatures: %v\n", blockHeader.BlockMeta.BlockMeta.Header, blockHeader.Commit)

	// addr := signingAccount[0].GetAddress()
	// account, _ := client2.GetAccount(addr)
	// logrus.Infof("Account: %v\n", account)
	// sequence := account.GetSequence() + 1

	// tx := DeployContract(signingAccount[0].GetAddress(), sequence, "binaries/scalable_coin.bin/ScalableCoin.bin")
	// txExecution, err := client2.SignTx(payload.Payload(&tx))
	// if err != nil {
	// 	logrus.Fatalf("Error: %v\n", err)
	// }
	// // logrus.Infof("TxExecution: %v\n", txExecution)
	// tx2, err := client2.BroadcastEnvelope(txExecution)
	// fmt.Printf("%v\n", tx2.GetReceipt().ContractAddress)

	// if err != nil {
	// 	logrus.Fatalf("Error: %v\n", err)
	// }
	// end := rpcevents.StreamBound()

	// request := &rpcevents.BlocksRequest{
	// 	BlockRange: rpcevents.NewBlockRange(rpcevents.AbsoluteBound(1), end),
	// }
	// clientEvents, err := client2.Query()
	// signedHeaders, err := clientEvents.ListSignedHeaders(context.Background(), request)
	// if err != nil {
	// 	logrus.Fatalf("Error: %v", err)
	// }
	// for {
	// 	resp, err := signedHeaders.Recv()
	// 	if err != nil {
	// 		logrus.Fatalf("Error: %v", err)
	// 	}
	// 	logrus.Infof("Block received: %v", resp.SignedHeader)

	// }

}
