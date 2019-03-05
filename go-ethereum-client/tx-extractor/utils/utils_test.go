package utils

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/enriquefynn/sharding-runner/go-ethereum-client/tx-extractor/utils"
)

func TestRoba(t *testing.T) {
	rw := utils.CreateTxsRW("./test_rw.txt")
	addr := common.HexToAddress("0xffffffffffffffffffffffffffffffffffffffff")
	rw.SaveTx(addr.Bytes(), nil, addr.Bytes(), 0, 1, 2, 3)
	rw.Close()

	rw = utils.CreateTxsRW("./test_rw.txt")
	for {
		from, to, data, amount, gas, gasPrice, shouldFail, err := rw.LoadTx()
		fmt.Printf("%x %x %x %d %d %d %d\n", from, to, data, amount, gas, gasPrice, shouldFail)
		if err != nil {
			break
		}
	}
}
