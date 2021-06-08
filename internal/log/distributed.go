package log

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	api "github.com/cdarne/proglog/api/v1"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{config: config}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *DistributedLog) setupLog(dataDir string) (err error) {
	logDir := filepath.Join(dataDir, "log")
	if err = os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	l.log, err = NewLog(logDir, l.config)
	return err
}

func (l *DistributedLog) setupRaft(dataDir string) error {
	fsm := &fsm{log: l.log}
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0775); err != nil {
		return err
	}
	logConfig := l.config
	logConfig.Segment.InitialOffset = 1
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}
	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft"), retain, os.Stderr)
	if err != nil {
		return err
	}
	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(l.config.Raft.StreamLayer, maxPool, timeout, os.Stderr)

	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	l.raft, err = raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}
	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return err
	}
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{ID: config.LocalID, Address: raft.ServerAddress(l.config.Raft.BindAddr)}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}
	return err
}

func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(AppendRequestType, &api.ProduceRequest{Record: record})
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	future := l.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		return nil, err
	}
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

func (l *DistributedLog) Join(id, addr string) error {
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || serverAddr == srv.Address {
			// server has already joined
			return nil
		}
	}
	// remove existing server
	removeFuture := l.raft.RemoveServer(serverID, 0, 0)
	if err := removeFuture.Error(); err != nil {
		return err
	}
	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	return addFuture.Error()
}

func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutChan := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutChan:
			return fmt.Errorf("leader timed out")
		case <-ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

func (l *DistributedLog) Close() error {
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return l.log.Close()
}

func (l *DistributedLog) GetServers() ([]*api.Server, error) {
	future := l.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var servers []*api.Server
	for _, server := range future.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:       string(server.ID),
			RpcAddr:  string(server.Address),
			IsLeader: l.raft.Leader() == server.Address,
		})
	}
	return servers, nil
}
