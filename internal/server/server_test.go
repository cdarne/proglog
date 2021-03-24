package server

import (
	"context"
	"flag"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	api "github.com/cdarne/proglog/api/v1"
	"github.com/cdarne/proglog/internal/auth"
	"github.com/cdarne/proglog/internal/config"
	"github.com/cdarne/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume stream wait for next log":                   testConsumeStreamWaitForNext,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"authorized fails":                                   testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (rootClient api.LogClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}

	if fn != nil {
		fn(cfg)
	}

	telemetryTeardown := setupTelemetry(t)

	serverAddress, serverTeardown := setupServer(t, cfg)
	rootClient, rootClientTeardown := setupClient(t, serverAddress, config.RootClientKeyFile, config.RootClientCertFile)
	nobodyClient, nobodyClientTeardown := setupClient(t, serverAddress, config.NobodyClientKeyFile, config.NobodyClientCertFile)

	return rootClient, nobodyClient, cfg, func() {
		rootClientTeardown()
		nobodyClientTeardown()
		serverTeardown()
		clog.Remove()
		telemetryTeardown()
	}
}

func setupClient(t *testing.T, serverAddress, clientKeyFile, clientCertFile string) (client api.LogClient, teardown func()) {
	t.Helper()

	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:   config.CAFile,
		KeyFile:  clientKeyFile,
		CertFile: clientCertFile,
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(clientTLSConfig)
	cc, err := grpc.Dial(serverAddress, grpc.WithTransportCredentials(clientCreds))
	require.NoError(t, err)

	client = api.NewLogClient(cc)

	return client, func() {
		cc.Close()
	}
}

func setupServer(t *testing.T, serverConfig *Config) (serverAddress string, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serverAddress = l.Addr().String()

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: serverAddress,
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	server, err := NewGRPCServer(serverConfig, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return serverAddress, func() {
		server.Stop()
		l.Close()
	}
}

func setupTelemetry(t *testing.T) (teardown func()) {
	t.Helper()
	if !*debug {
		return func() {}
	}

	var telemetryExporter *exporter.LogExporter
	metricsLogFile, err := ioutil.TempFile("", "metrics-*.log")
	require.NoError(t, err)
	t.Logf("metrics log file: %s", metricsLogFile.Name())

	tracesLogFile, err := ioutil.TempFile("", "traces-*.log")
	require.NoError(t, err)
	t.Logf("traces log file: %s", tracesLogFile.Name())

	telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
		MetricsLogFile:    metricsLogFile.Name(),
		TracesLogFile:     tracesLogFile.Name(),
		ReportingInterval: time.Second,
	})
	require.NoError(t, err)
	err = telemetryExporter.Start()
	require.NoError(t, err)

	return func() {
		time.Sleep(1500 * time.Millisecond)
		telemetryExporter.Stop()
		telemetryExporter.Close()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, _ api.LogClient, cfg *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)
	require.Equal(t, uint64(0), produce.Offset)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	require.NoError(t, err)
	require.Equal(t, want.Offset, consume.Record.Offset)
	require.Equal(t, want.Value, consume.Record.Value)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, _ api.LogClient, cfg *Config) {
	ctx := context.Background()

	_, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("Hello World"),
		}})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 1})
	require.Nil(t, consume)
	require.Error(t, err)
	errCode := status.Code(err)
	want := "rpc error: code = Code(404) desc = offset out of range: 1"
	require.Equal(t, want, err.Error())
	require.Equal(t, codes.Code(404), errCode)
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, _ api.LogClient, cfg *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{Value: []byte("First message"), Offset: 0},
		{Value: []byte("Second message"), Offset: 1},
	}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, uint64(offset), res.Offset)
		}
	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for offset, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, record.Value, res.Record.Value)
			require.Equal(t, record.Offset, res.Record.Offset)
			require.Equal(t, uint64(offset), res.Record.Offset)
		}
	}
}

func testConsumeStreamWaitForNext(t *testing.T, client api.LogClient, _ api.LogClient, cfg *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{Value: []byte("First message"), Offset: 0},
		{Value: []byte("Second message"), Offset: 1},
	}

	produceStream, err := client.ProduceStream(ctx)
	require.NoError(t, err)
	err = produceStream.Send(&api.ProduceRequest{Record: records[0]})
	require.NoError(t, err)

	consumeStream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	require.NoError(t, err)

	res, err := consumeStream.Recv()
	require.NoError(t, err)
	require.Equal(t, records[0].Value, res.Record.Value)
	require.Equal(t, records[0].Offset, res.Record.Offset)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		res, err := consumeStream.Recv()
		require.NoError(t, err)
		require.Equal(t, records[1].Value, res.Record.Value)
		require.Equal(t, records[1].Offset, res.Record.Offset)
	}()

	err = produceStream.Send(&api.ProduceRequest{Record: records[1]})
	require.NoError(t, err)

	wg.Wait()
}

func testUnauthorized(t *testing.T, _ api.LogClient, client api.LogClient, cfg *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	if produce != nil {
		t.Fatalf("produce response must be nil")
	}
	require.Error(t, err)
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	require.Equal(t, wantCode, gotCode)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	if consume != nil {
		t.Fatalf("consume response must be nil")
	}
	require.Error(t, err)
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	require.Equal(t, wantCode, gotCode)
}
