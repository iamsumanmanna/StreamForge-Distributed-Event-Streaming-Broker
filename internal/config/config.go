package config

import (
	"flag"
	"os"
	"strconv"
	"time"
)

// Config holds broker runtime configuration.
type Config struct {
	NodeID            string
	ListenAddr        string
	HTTPAddr          string
	DataDir           string
	SegmentMaxBytes   int64
	FsyncInterval     time.Duration
	FsyncEveryN       int
	RetentionBytes    int64
	RetentionAge      time.Duration
	ClusterPeers      string // comma-separated nodeID=host:port for raft/replication
	RaftBind          string
	EnableCluster     bool
	ReplicationFactor int

	// Durability / produce defaults
	DefaultAcks        int16  // 0, 1, or -1
	DefaultCompression string // none|gzip|snappy|lz4|zstd

	// Security
	TLSCertFile string
	TLSKeyFile  string
	AuthToken   string // empty = auth disabled
}

func Default() Config {
	return Config{
		NodeID:             "broker-1",
		ListenAddr:         ":9092",
		HTTPAddr:           ":8080",
		DataDir:            "./data",
		SegmentMaxBytes:    64 << 20, // 64 MiB
		FsyncInterval:      50 * time.Millisecond,
		FsyncEveryN:        1000,
		RetentionBytes:     1 << 30, // 1 GiB per partition
		RetentionAge:       7 * 24 * time.Hour,
		RaftBind:           ":7000",
		EnableCluster:      false,
		ReplicationFactor:  1,
		DefaultAcks:        -1,
		DefaultCompression: "none",
	}
}

// ParseFlags loads config from flags and env overrides (SF_* preferred).
func ParseFlags() Config {
	cfg := Default()
	flag.StringVar(&cfg.NodeID, "id", envSF("NODE_ID", cfg.NodeID), "broker node id")
	flag.StringVar(&cfg.ListenAddr, "listen", envSF("LISTEN", cfg.ListenAddr), "TCP broker listen address")
	flag.StringVar(&cfg.HTTPAddr, "http", envSF("HTTP", cfg.HTTPAddr), "HTTP metrics/health address")
	flag.StringVar(&cfg.DataDir, "data", envSF("DATA", cfg.DataDir), "data directory")
	flag.Int64Var(&cfg.SegmentMaxBytes, "segment-bytes", envSFInt64("SEGMENT_BYTES", cfg.SegmentMaxBytes), "max segment size")
	flag.DurationVar(&cfg.FsyncInterval, "fsync-interval", envSFDuration("FSYNC_INTERVAL", cfg.FsyncInterval), "periodic fsync interval")
	flag.IntVar(&cfg.FsyncEveryN, "fsync-every", envSFInt("FSYNC_EVERY", cfg.FsyncEveryN), "fsync after N appends")
	flag.Int64Var(&cfg.RetentionBytes, "retention-bytes", envSFInt64("RETENTION_BYTES", cfg.RetentionBytes), "retention by size")
	flag.DurationVar(&cfg.RetentionAge, "retention-age", envSFDuration("RETENTION_AGE", cfg.RetentionAge), "retention by age")
	flag.StringVar(&cfg.ClusterPeers, "peers", envSF("PEERS", ""), "cluster peers nodeID=host:raftPort,...")
	flag.StringVar(&cfg.RaftBind, "raft-bind", envSF("RAFT_BIND", cfg.RaftBind), "raft bind address")
	flag.BoolVar(&cfg.EnableCluster, "cluster", envSFBool("CLUSTER", false), "enable raft cluster mode")
	flag.IntVar(&cfg.ReplicationFactor, "rf", envSFInt("RF", cfg.ReplicationFactor), "default replication factor")
	acks := flag.Int("acks", envSFInt("ACKS", int(cfg.DefaultAcks)), "default produce acks: 0, 1, or -1 (all)")
	flag.StringVar(&cfg.DefaultCompression, "compression", envSF("COMPRESSION", cfg.DefaultCompression), "default compression: none|gzip|snappy|lz4|zstd")
	flag.StringVar(&cfg.TLSCertFile, "tls-cert", envSF("TLS_CERT", ""), "TLS certificate file (enables TLS)")
	flag.StringVar(&cfg.TLSKeyFile, "tls-key", envSF("TLS_KEY", ""), "TLS private key file")
	flag.StringVar(&cfg.AuthToken, "auth-token", envSF("AUTH_TOKEN", ""), "shared auth token (empty disables)")
	flag.Parse()
	cfg.DefaultAcks = int16(*acks)
	return cfg
}

// envSF reads SF_<key>, then legacy MK_<key>.
func envSF(suffix, def string) string {
	if v := os.Getenv("SF_" + suffix); v != "" {
		return v
	}
	if v := os.Getenv("MK_" + suffix); v != "" {
		return v
	}
	return def
}

func envSFInt(suffix string, def int) int {
	if v := envSF(suffix, ""); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envSFInt64(suffix string, def int64) int64 {
	if v := envSF(suffix, ""); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envSFBool(suffix string, def bool) bool {
	if v := envSF(suffix, ""); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envSFDuration(suffix string, def time.Duration) time.Duration {
	if v := envSF(suffix, ""); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
