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
func (lat *Latencies) Add(txHash string, tx *dependencies.TxResponse, now int64) {
	if tx.MethodName == "moveTo" {
		lat.awaitingTx[tx.OriginalIds[0]] = now
	} else if tx.MethodName != "move2" {
		lat.outgoingTxs[txHash] = now
	}
}

func (lat *Latencies) Remove(txHash string, tx *dependencies.TxResponse, log *Log, finalTime int64) {
	initialTime := lat.outgoingTxs[txHash]
	delete(lat.outgoingTxs, txHash)
	var requiredMove bool
	if tx.MethodName == "breed" {
		delete(lat.outgoingTxs, txHash)
		timeMoved1, id1Move := lat.awaitingTx[tx.OriginalIds[0]]
		timeMoved2, id2Move := lat.awaitingTx[tx.OriginalIds[1]]
		if id1Move {
			delete(lat.awaitingTx, tx.OriginalIds[0])
			requiredMove = true
			initialTime = timeMoved1
		} else if id2Move {
			delete(lat.awaitingTx, tx.OriginalIds[1])
			requiredMove = true
			initialTime = timeMoved2
		}
	}
	if tx.MethodName != "moveTo" || tx.MethodName != "move2" {
		log.Log("latencies", "%v %d %d %v\n", tx.MethodName, initialTime, finalTime, requiredMove)
	}
}
