package main

import (
	"io/ioutil"
	"testing"

	"github.com/enriquefynn/sharding-runner/burrow-client/config"
	"github.com/enriquefynn/sharding-runner/burrow-client/logs-replayer/utils"
	"github.com/hyperledger/burrow/crypto"
	yaml "gopkg.in/yaml.v2"
)

func TestGetOp(t *testing.T) {
	config := config.Config{}
	configFile, err := ioutil.ReadFile("./benchmark8p.yaml")
	err = yaml.Unmarshal(configFile, &config)

	logs, err := utils.NewLog(config.Logs.Dir)
	scalableCoin := NewScalableCoinAPI(&config, logs)

	scalableCoin.GetOp(crypto.ZeroAddress)

	if err != nil {
		t.Errorf("Error %v", err)
	}
}
