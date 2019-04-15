package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/enriquefynn/sharding-runner/burrow-client/config"
	"github.com/sirupsen/logrus"
)

func main() {
	nCli, err := strconv.Atoi(os.Args[1])
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}

	accounts := config.GetSignedAccounts(nCli)
	for i := 0; i < nCli; i++ {
		fmt.Printf(`
[[GenesisDoc.Accounts]]
  Address = "%v"
  Amount = 99999999999999
`, accounts[i][0].GetAddress())
	}
}
