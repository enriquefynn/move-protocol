package main

import (
	"bufio"
	"fmt"
	"os"
)

type Log struct {
	logs map[string]struct {
		file   *os.File
		writer *bufio.Writer
	}
}

func NewLog() (*Log, error) {
	log := &Log{
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
		file, err := os.Create(logName + ".txt")
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
	fmt.Fprintf(log.writer, "args", args...)
}

func (l *Log) Close() {
	for k, log := range l.logs {
		log.writer.Flush()
		log.file.Close()
	}
}
