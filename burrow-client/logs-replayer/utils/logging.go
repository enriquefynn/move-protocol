package utils

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"github.com/hyperledger/burrow/dependencies"
	"github.com/sirupsen/logrus"
)

func fatalError(err error) {
	if err != nil {
		logrus.Fatalf("Error: %v", err)
	}
}

type Log struct {
	logDir string
	logs   map[string]struct {
		file   *os.File
		writer *bufio.Writer
	}
	sync.RWMutex
}

func NewLog(logDir string) (*Log, error) {
	log := &Log{
		logDir: logDir,
		logs: make(map[string]struct {
			file   *os.File
			writer *bufio.Writer
		}),
	}
	return log, nil
}

func (l *Log) Log(logName, format string, args ...interface{}) {
	l.Lock()
	defer l.Unlock()
	log, ok := l.logs[logName]
	if !ok {
		file, err := os.Create(l.logDir + logName + ".txt")
		if err != nil {
			fatalError(err)
		}
		l.logs[logName] = struct {
			file   *os.File
			writer *bufio.Writer
		}{
			file:   file,
			writer: bufio.NewWriter(file),
		}
		log = l.logs[logName]
	}
	fmt.Fprintf(log.writer, format, args...)
}

func (l *Log) Close() {
	for _, log := range l.logs {
		log.writer.Flush()
		log.file.Close()
	}
}
func (l *Log) Flush() {
	for _, log := range l.logs {
		log.writer.Flush()
	}
}

type Latencies struct {
	outgoingTxs map[string]int64
	awaitingTx  map[int64]int64
}

func NewLatencyLog() *Latencies {
	return &Latencies{
		outgoingTxs: make(map[string]int64),
		awaitingTx:  make(map[int64]int64),
	}
}
func (lat *Latencies) Add(txsHash string, tx *dependencies.TxResponse, now int64) {
	// now := time.Now().Nanosecond()
	// for i, tx := range txs {
	if tx.MethodName == "moveTo" {
		lat.awaitingTx[tx.OriginalIds[0]] = now
	} else if tx.MethodName != "move2" {
		lat.outgoingTxs[txsHash] = now
	}
	// }
}

func (lat *Latencies) Remove(txHash string, tx *dependencies.TxResponse, log *Log, now int64) {
	var latency int64
	var requiredMove bool
	// for i, tx := range txs {
	// if tx.MethodName == "moveTo" {
	// lat.idLatency[tx.OriginalIds[0]] = now - lat.awaitingTx[tx.OriginalIds[0]]
	// delete(lat.awaitingTx, tx.OriginalIds[0])
	// } else if tx.MethodName == "move2" {
	// lat.idLatency[tx.OriginalIds[0]] += (now - lat.awaitingTx[tx.OriginalIds[0]])
	// delete(lat.awaitingTx, tx.OriginalIds[0])
	// } else {
	if tx.MethodName == "breed" {
		delete(lat.outgoingTxs, txHash)
		timeMoved1, id1Move := lat.awaitingTx[tx.OriginalIds[0]]
		timeMoved2, id2Move := lat.awaitingTx[tx.OriginalIds[1]]
		if id1Move {
			delete(lat.awaitingTx, tx.OriginalIds[0])
			requiredMove = true
			latency = (timeMoved1 - lat.outgoingTxs[txHash])
		}
		if id2Move {
			delete(lat.awaitingTx, tx.OriginalIds[1])
			requiredMove = true
			latency = (timeMoved2 - lat.outgoingTxs[txHash])
		} else {
			latency = (now - lat.outgoingTxs[txHash])
		}
	}
	log.Log("latencies", "%v %d %d %v\n", tx.MethodName, now, latency, requiredMove)
	// }
	// }
}
