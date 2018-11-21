package main

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

var (
	key, _  = crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000001")
	key2, _ = crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000002")
)

func clientClosedLoop(key *ecdsa.PrivateKey, client *ethclient.Client, blockChan <-chan *types.Block) {
	ctx := context.Background()
	address := crypto.PubkeyToAddress(key.PublicKey)

	nonce, err := client.NonceAt(ctx, address, nil)
	if err != nil {
		log.Fatalf("Error getting nonce %v", err)
	}
	waitForTx := false

	for {
		var txHash common.Hash
		if !waitForTx {
			fmt.Printf("Sending tx client: %x nonce: %v\n", address, nonce)
			waitForTx = true
			tx := types.NewTransaction(nonce, common.HexToAddress("0xe1ab8145f7e55dc933d51a18c793f901a3a0b276"), big.NewInt(1), 1e5, big.NewInt(1), []byte{})
			signedTx, err := types.SignTx(tx, types.HomesteadSigner{}, key)
			txHash = signedTx.Hash()
			if err != nil {
				log.Fatalf("Error signing transaction: %v", err)
			}

			// log.Printf("HEY %v %v", client, signedTx)
			err = client.SendTransaction(ctx, signedTx)
			if err != nil {
				log.Printf("Could not send transaction: %v, increasing nonce", err)
			}
			nonce++
		}
		block := <-blockChan
		if block.Transaction(txHash) != nil {
			fmt.Printf("Tx in block for %x %v\n", address, block.Number())
			waitForTx = false
		} else {
			fmt.Printf("Tx not in block for %x\n", address)
		}
	}
}

func main() {
	client, err := ethclient.Dial(os.Args[1])
	if err != nil {
		log.Fatalf("Failed to connec to ethereum client: %v", err)
	}

	ctx := context.Background()
	headerBlock := make(chan *types.Header)
	var blockChannels [2]chan *types.Block

	for i := range blockChannels {
		blockChannels[i] = make(chan *types.Block)
	}

	go clientClosedLoop(key, client, blockChannels[0])
	go clientClosedLoop(key2, client, blockChannels[1])

	headerSub, err := client.SubscribeNewHead(ctx, headerBlock)

	if err != nil {
		log.Fatalf("Error subscribing to head: %v\n", err)
	}

	for {
		select {
		case head := <-headerBlock:
			// blk, _ := client.BlockByNumber(ctx, head.Number)
			block, err := client.BlockByHash(ctx, head.Hash())
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
