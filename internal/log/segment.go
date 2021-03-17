package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/cdarne/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(directory string, baseOffset uint64, config Config) (s *segment, err error) {
	s = &segment{
		baseOffset: baseOffset,
		config:     config,
	}
	storeFile, err := os.OpenFile(path.Join(directory, fmt.Sprintf("%d%s", baseOffset, ".store")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(path.Join(directory, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, config); err != nil {
		return nil, err
	}
	if offset, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(offset) + 1
	}
	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	curentOffset := s.nextOffset
	record.Offset = curentOffset
	output, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(output)
	if err != nil {
		return 0, err
	}
	// index offsets are relative to base offset
	relativeOffset := uint32(s.nextOffset - s.baseOffset)
	if err = s.index.Write(relativeOffset, pos); err != nil {
		return 0, err
	}
	s.nextOffset++
	return curentOffset, nil
}

func (s *segment) Read(offset uint64) (*api.Record, error) {
	relativeOffset := int64(offset - s.baseOffset)
	_, pos, err := s.index.Read(relativeOffset)
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
