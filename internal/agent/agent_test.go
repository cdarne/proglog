package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/cdarne/proglog/internal/loadbalance"

	api "github.com/cdarne/proglog/api/v1"
	"github.com/cdarne/proglog/internal/agent"
	"github.com/cdarne/proglog/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestAgent(t *testing.T) {
	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	agents, teardown := setupAgents(t, peerTLSConfig)
	defer teardown()
	// Wait for cluster to synchronize
	time.Sleep(3 * time.Second)

	logValue := []byte("foo")

	leaderClient := client(t, agents[0], peerTLSConfig)
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{Record: &api.Record{Value: logValue}},
	)
	require.NoError(t, err)

	// Wait until replication has finished to Consume from followers
	time.Sleep(3 * time.Second)

	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, logValue)

	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, logValue)

	followerClient = client(t, agents[2], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, logValue)

	// Check that the leader has no more logs (loop replication of the leader-less naive implementation)
	consumeResponse, err = leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResponse.Offset + 1},
	)
	require.Nil(t, consumeResponse)
	require.Error(t, err)
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, want, got)
}

func setupAgents(t *testing.T, peerTLSConfig *tls.Config) (agents []*agent.Agent, teardown func()) {
	t.Helper()

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]
		dataDir, err := ioutil.TempDir("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		a, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("Agent-%d", i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
			Bootstrap:       i == 0,
		})
		require.NoError(t, err)

		agents = append(agents, a)
	}

	return agents, func() {
		for _, a := range agents {
			err := a.Shutdown()
			require.NoError(t, err)

			err = os.RemoveAll(a.Config.DataDir)
			require.NoError(t, err)
		}
	}
}

func client(t *testing.T, agent *agent.Agent, tlsConfig *tls.Config) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.Dial(fmt.Sprintf("%s:///%s", loadbalance.Name, rpcAddr), opts...)
	require.NoError(t, err)

	return api.NewLogClient(conn)
}
