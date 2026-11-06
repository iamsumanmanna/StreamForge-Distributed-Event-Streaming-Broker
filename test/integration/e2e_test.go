package integration

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/distributed-system/streamforge/internal/broker"
	"github.com/distributed-system/streamforge/internal/config"
	"github.com/distributed-system/streamforge/internal/storage/compress"
	"github.com/distributed-system/streamforge/internal/transport/protocol"
	"github.com/distributed-system/streamforge/internal/transport/server"
	"github.com/distributed-system/streamforge/pkg/client"
	"github.com/prometheus/client_golang/prometheus"
)

func startBroker(t *testing.T, cfg config.Config) (*broker.Broker, *server.Server) {
	t.Helper()
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	prometheus.DefaultGatherer = prometheus.DefaultRegisterer.(prometheus.Gatherer)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	b, err := broker.New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(b, "127.0.0.1:0", "127.0.0.1:0", logger)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
		_ = b.Close()
	})
	return b, srv
}

func TestProduceFetchAndGroup(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.NodeID = "test-1"
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.FsyncEveryN = 1
	cfg.FsyncInterval = time.Hour
	cfg.EnableCluster = false
	cfg.DefaultAcks = 1

	_, srv := startBroker(t, cfg)
	c, err := client.Dial(srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.CreateTopic("orders", 2); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if _, err := c.Produce("orders", 0, []byte("k"), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	resp, err := c.Fetch("orders", 0, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Records) != 50 {
		t.Fatalf("got %d records", len(resp.Records))
	}

	join, err := c.JoinGroup("g1", "", []string{"orders"})
	if err != nil {
		t.Fatal(err)
	}
	sync, err := c.SyncGroup(protocol.SyncGroupReq{
		GroupID: "g1", MemberID: join.MemberID, GenerationID: join.GenerationID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sync.Assignments) == 0 {
		t.Fatal("expected assignments")
	}
	if err := c.CommitOffset("g1", "orders", 0, 50); err != nil {
		t.Fatal(err)
	}
	offs, err := c.FetchOffsets("g1", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(offs.Offsets) != 1 || offs.Offsets[0].Offset != 50 {
		t.Fatalf("offsets %+v", offs)
	}
}

func TestCompressionAndDurableOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.NodeID = "test-2"
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.FsyncEveryN = 1
	cfg.FsyncInterval = time.Hour

	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	prometheus.DefaultGatherer = prometheus.DefaultRegisterer.(prometheus.Gatherer)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	b1, err := broker.New(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	srv1 := server.New(b1, "127.0.0.1:0", "127.0.0.1:0", logger)
	if err := srv1.Start(); err != nil {
		t.Fatal(err)
	}

	c, err := client.Dial(srv1.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateTopic("chat", 1); err != nil {
		t.Fatal(err)
	}
	payload := []byte("realtime hello with compression")
	if _, err := c.ProduceOpts("chat", 0, []byte("u1"), payload, protocol.AcksLeader, compress.Gzip); err != nil {
		t.Fatal(err)
	}
	resp, err := c.Fetch("chat", 0, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Records) != 1 || string(resp.Records[0].Value) != string(payload) {
		t.Fatalf("got %+v", resp.Records)
	}
	if err := c.CommitOffset("workers", "chat", 0, 1); err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = srv1.Stop(ctx)
	cancel()
	_ = b1.Close()

	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	prometheus.DefaultGatherer = prometheus.DefaultRegisterer.(prometheus.Gatherer)
	_, srv2 := startBroker(t, cfg)
	c2, err := client.Dial(srv2.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	offs, err := c2.FetchOffsets("workers", "chat")
	if err != nil {
		t.Fatal(err)
	}
	if len(offs.Offsets) != 1 || offs.Offsets[0].Offset != 1 {
		t.Fatalf("durable offsets missing: %+v", offs)
	}
}

func TestAuthToken(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.NodeID = "test-auth"
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.AuthToken = "secret"
	cfg.FsyncEveryN = 1

	_, srv := startBroker(t, cfg)

	bad, err := client.Dial(srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	if err := bad.CreateTopic("t", 1); err == nil {
		// unauthenticated create should fail with unauthorized
		t.Fatal("expected unauthorized without token")
	}

	good, err := client.DialWithOptions(srv.Addr(), client.DialOptions{AuthToken: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer good.Close()
	if err := good.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
}
