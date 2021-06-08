package log_test

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	api "github.com/cdarne/proglog/api/v1"
	"github.com/cdarne/proglog/internal/log"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestMultipleNodes(t *testing.T) {
	var logs []*log.DistributedLog
	var leader *log.DistributedLog
	nodeCount := 3
	ports := dynaport.Get(nodeCount)

	for i, port := range ports {
		isLeader := i == 0
		dataDir, err := ioutil.TempDir("", "distributed-log-test")
		require.NoError(t, err)
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		require.NoError(t, err)

		id := fmt.Sprintf("test-%d", i)
		config := testLogConfig(id, ln, isLeader)
		l, err := log.NewDistributedLog(dataDir, config)
		require.NoError(t, err)

		if isLeader {
			leader = l
			err = leader.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		} else {
			err = leader.Join(id, ln.Addr().String())
			require.NoError(t, err)
		}
		logs = append(logs, l)
	}

	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}
	for _, record := range records {
		off, err := leader.Append(record)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			for j := 0; j < nodeCount; j++ {
				got, err := logs[j].Read(off)
				if err != nil {
					return false
				}
				record.Offset = off
				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}
			return true
		}, 500*time.Millisecond, 50*time.Millisecond)
	}

	// Check GetServers
	servers, err := leader.GetServers()
	require.NoError(t, err)
	require.Equal(t, 3, len(servers))
	require.True(t, servers[0].IsLeader)
	require.False(t, servers[1].IsLeader)
	require.False(t, servers[2].IsLeader)
	// ----------------

	err = leader.Leave("test-1")
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	off, err := leader.Append(&api.Record{Value: []byte("third")})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	record, err := logs[1].Read(off)
	require.IsType(t, api.ErrOffsetOutOfRange{}, err)
	require.Nil(t, record)

	record, err = logs[2].Read(off)
	require.NoError(t, err)
	require.Equal(t, []byte("third"), record.Value)
	require.Equal(t, off, record.Offset)

	for _, log := range logs {
		err := log.Close()
		require.NoError(t, err)
	}
}

func testLogConfig(id string, ln net.Listener, isLeader bool) log.Config {
	config := log.Config{}
	config.Raft.StreamLayer = log.NewStreamLayer(ln, nil, nil)
	config.Raft.LocalID = raft.ServerID(id)
	config.Raft.HeartbeatTimeout = 50 * time.Millisecond
	config.Raft.ElectionTimeout = 50 * time.Millisecond
	config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
	config.Raft.CommitTimeout = 5 * time.Millisecond
	config.Raft.BindAddr = ln.Addr().String()
	if isLeader {
		config.Raft.Bootstrap = true
	}
	return config
}
