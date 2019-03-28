package main

import (
	"fmt"
	"os"
	"strconv"

	"../utils"
	"github.com/sirupsen/logrus"
)

func main() {
	nCli, err := strconv.Atoi(os.Args[1])
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}

	accounts := utils.GetSignedAccounts(nCli)
	for _, acc := range accounts {
		fmt.Printf(`
[[GenesisDoc.Accounts]]
  Address = "%v"
  Amount = 99999999999999
`, acc[0].GetAddress())
	}
}
