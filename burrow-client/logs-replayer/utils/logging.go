package utils

import (
	"bufio"
	"fmt"
	"os"

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
