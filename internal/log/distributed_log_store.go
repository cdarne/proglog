package log

import (
	api "github.com/cdarne/proglog/api/v1"
	"github.com/hashicorp/raft"
)

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	record, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = record.Value
	out.Index = record.Offset
	out.Type = raft.LogType(record.Type)
	out.Term = record.Term
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		_, err := l.Append(&api.Record{Value: record.Data, Term: record.Term, Type: uint32(record.Type)})
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}
