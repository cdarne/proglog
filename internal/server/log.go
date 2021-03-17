package server

import (
	"fmt"
	"sync"
)

// A Log stores a list of Record
type Log struct {
	mu      sync.Mutex
	records []Record
}

// A Record holds a value and an offset
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

// ErrOffsetNotFound means that an offset is off limit
var ErrOffsetNotFound = fmt.Errorf("offset not found")

// NewLog creates a new Log
func NewLog() *Log {
	return &Log{}
}

// Append a Record to the Log. It returns the offset the Record has been inserted to
func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

// Reads a the Log from a specific offset
func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}
