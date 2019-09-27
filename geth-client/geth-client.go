package main

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

var (
	key, _ = crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000001")
	// key2, _  = crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000002")
	addrTest            = common.HexToAddress("0x0000000000000000000000000000000000000000")
	contractPath        = "../contracts/scalableCoin/binaries/ScalableCoin.bin"
	scalableCoinABIPath = "../contracts/scalableCoin/binaries/ScalableCoin.abi"
	accountABIPath      = "../contracts/scalableCoin/binaries/Account.abi"
)

func clientClosedLoop(key *ecdsa.PrivateKey, client *ethclient.Client, blockChan <-chan *types.Block) {
	abiFile, err := os.Open(scalableCoinABIPath)
	if err != nil {
		log.Fatalf("ERROR OPENING ABI")
	}
	abiScalableCoin, err := abi.JSON(abiFile)
	if err != nil {
		log.Fatalf("ERROR CREATING ABI")
	}
	abiFile, err = os.Open(accountABIPath)
	if err != nil {
		log.Fatalf("ERROR OPENING ABI")
	}
	abiAccount, err := abi.JSON(abiFile)
	if err != nil {
		log.Fatalf("ERROR CREATING ABI")
	}

	ctx := context.Background()
	address := crypto.PubkeyToAddress(key.PublicKey)

	nonce, err := client.NonceAt(ctx, address, nil)
	if err != nil {
		log.Fatalf("Error getting nonce %v", err)
	}
	waitForTx := false

	var txHash common.Hash

	// Create main contract
	tx, err := createContract(nonce, big.NewInt(0), 4e6, big.NewInt(10), contractPath)
	if err != nil {
		log.Fatalf("ERROR CREATING MAIN CONTRACT: %v", err)
	}
	signedTx, err := types.SignTx(tx, types.HomesteadSigner{}, key)
	if err != nil {
		log.Fatalf("Error signing transaction: %v", err)
	}
	txHash = signedTx.Hash()
	err = client.SendTransaction(ctx, signedTx)
	if err != nil {
		log.Printf("Could not send transaction: %v, increasing nonce", err)
	}
	contractAddress := crypto.CreateAddress(address, nonce)

	nonce++
	block := <-blockChan

	receipt, err := client.TransactionReceipt(ctx, txHash)
	fmt.Printf("CONTRACT ADDR: %x %x\n", contractAddress, receipt.ContractAddress)

	// scalableCoin, err := contract.NewScalableCoin(contractAddress, client)
	// if err != nil {
	// 	log.Fatalf("ERROR: %v", err)
	// }
	// code, err := client.CodeAt(ctx, contractAddress, block.Number())
	// fmt.Printf("CODE: %x err %v\n", code, err)

	for {
		if !waitForTx {
			fmt.Println("Getting new account")
			txData := abiScalableCoin.Methods["newAccount"].ID()
			fmt.Printf("===> %x\n", txData)
			tx := types.NewTransaction(nonce, contractAddress, big.NewInt(1), 4e6, big.NewInt(1), txData)

			signedTx, err := types.SignTx(tx, types.HomesteadSigner{}, key)
			if err != nil {
				log.Fatalf("Error signing transaction: %v", err)
			}
			txHash = signedTx.Hash()
			err = client.SendTransaction(ctx, signedTx)
			if err != nil {
				log.Printf("Could not send transaction: %v, increasing nonce", err)
			}
			nonce++
		}
		block = <-blockChan

		receipt, err := client.TransactionReceipt(ctx, txHash)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
		}
		fmt.Printf("STATUS: %v\n", receipt.Status)
		addrID := receipt.Logs[0].Data
		accountAddress := common.BytesToAddress(addrID[12:32])
		accountID := new(big.Int)
		accountID.SetBytes((addrID[32:]))
		fmt.Printf("------------------>: %x %x\n", accountAddress, accountID.Int64())

		code, err := client.CodeAt(ctx, accountAddress, block.Number())
		if len(code) == 0 {
			log.Fatalf("NO CODE ON ACCOUNT")
		}

		// MOVE
		txData, err := abiAccount.Methods["moveTo"].Inputs.Pack(common.Big2)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
		}
		txData = append(abiAccount.Methods["moveTo"].ID(), txData...)
		tx := types.NewTransaction(nonce, accountAddress, common.Big0, 4e6, big.NewInt(1), txData)
		signedTx, err := types.SignTx(tx, types.HomesteadSigner{}, key)
		if err != nil {
			log.Fatalf("Error signing transaction: %v", err)
		}
		txHash = signedTx.Hash()
		err = client.SendTransaction(ctx, signedTx)
		if err != nil {
			log.Printf("Could not send transaction: %v, increasing nonce", err)
		}
		nonce++
		block = <-blockChan

		proof, err := client.GetMovedProof(ctx, accountAddress, block.Number())
		if err != nil {
			log.Fatalf("Could not get proof %v", err)
		}

		// SEND MOVE2
		tx = newMoveTx(nonce, accountAddress, common.Big0, 4e6, big.NewInt(1), &proof)
		signedTx, err = types.SignTx(tx, types.HomesteadSigner{}, key)
		err = client.SendTransaction(ctx, signedTx)
		if err != nil {
			log.Printf("Could not send move tx: %v", err)
		}
		block = <-blockChan
		receipt, err = client.TransactionReceipt(ctx, signedTx.Hash())
		if err != nil {
			log.Fatalf("Error getting receipt %v", err)
		}
		fmt.Printf("Used gas: %v\n", receipt.GasUsed)

		os.Exit(1)
	}
}

func main() {
	client, err := ethclient.Dial(os.Args[1])
	if err != nil {
		log.Fatalf("Failed to connec to ethereum client: %v", err)
	}

	ctx := context.Background()
	headerBlock := make(chan *types.Header)
	var blockChannels [1]chan *types.Block

	for i := range blockChannels {
		blockChannels[i] = make(chan *types.Block)
	}

	go clientClosedLoop(key, client, blockChannels[0])
	// go clientClosedLoop(key2, client, blockChannels[1])

	headerSub, err := client.SubscribeNewHead(ctx, headerBlock)

	if err != nil {
		log.Fatalf("Error subscribing to head: %v\n", err)
	}

	for {
		select {
		case head := <-headerBlock:
			// blk, _ := client.BlockByNumber(ctx, head.Number)
			block, err := client.BlockByHash(ctx, head.Hash())
			fmt.Printf("Block: %v %v\n", block.Number(), len(block.Transactions()))
			for _, tx := range block.Transactions() {
				fmt.Printf("Tx: %x\n", tx.Hash())
			}
			if err != nil {
				log.Fatalf("Error getting block: %v\n", err)
			}
			for i := range blockChannels {
				blockChannels[i] <- block
			}

		case err := <-headerSub.Err():
			log.Fatalf("Error in channel %v\n", err)
		}
	}
	// for i = 0; i < 50; i++ {
	// 	blk, err := client.BlockByNumber(ctx, big.NewInt(int64(i)))
	// 	if err != nil {
	// 		log.Fatalf("error: %v", err)
	// 	}
	// 	fmt.Printf("Block %v %x:\n", i, blk.Hash())
	// 	for _, tx := range blk.Transactions() {
	// 		fmt.Printf("Tx: %x\n", tx.Hash())
	// 	}
	// }
}
