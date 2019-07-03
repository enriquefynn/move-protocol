module github.com/enriquefynn/sharding-runner

go 1.12

require (
	github.com/ethereum/go-ethereum v1.8.27
	github.com/hyperledger/burrow v1.0.0
	github.com/sirupsen/logrus v1.4.2
	google.golang.org/appengine v1.4.0 // indirect
	gopkg.in/yaml.v2 v2.2.2
)

replace github.com/hyperledger/burrow => ../../hyperledger/burrow

replace github.com/tendermint/tendermint => ../../enriquefynn/tendermint
