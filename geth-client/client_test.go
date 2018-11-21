package main

import (
	"fmt"
	"testing"
)

func TestKeys(t *testing.T) {
	// client, err := ethclient.Dial(os.Args[1])
	// if err != nil {
	// 	log.Fatalf("Failed to connec to ethereum client: %v", err)
	// }

	// // ctx, cancel := context.NewContext()

	// tx := types.NewTransaction(0, common.HexToAddress("0x7c824702319db5633b39cb689ab78fda489518d2"), big.NewInt(1), 1e10, big.NewInt(1), []byte{})

	// signTx, err := types.SignTx(tx, types.HomesteadSigner{}, key)

	// client.SendTransaction(nil, tx)
	fmt.Printf("Addr: %x\n", address)
}
