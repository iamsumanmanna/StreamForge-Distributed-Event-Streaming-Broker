# Architecture & repository layout

Enterprise-style Go layout for **StreamForge Distributed Event Streaming Broker**.

```text
.
├── cmd/                      # Binaries (thin main packages)
│   ├── broker/               # Broker process entrypoint
│   ├── produce/              # Produce CLI
│   └── consume/              # Consume CLI
├── internal/                 # Private application code (not importable by others)
│   ├── broker/               # Core request handlers / domain orchestration
│   ├── config/               # Flags + env configuration (SF_*)
│   ├── storage/
│   │   ├── log/              # Segmented append-only partition log
│   │   └── compress/         # gzip / snappy / lz4 / zstd
│   ├── cluster/
│   │   ├── raftmeta/         # Raft-backed topic metadata
│   │   └── replica/          # Leader → follower replication
│   ├── consumer/
│   │   └── group/            # Consumer groups + durable offsets
│   └── transport/
│       ├── protocol/         # Custom TCP wire protocol
│       └── server/           # TCP listener, TLS, HTTP metrics
├── pkg/                      # Public libraries safe for external import
│   └── client/               # Go client SDK
├── test/
│   └── integration/          # End-to-end tests
├── configs/                  # Example env / config files
├── deployments/              # docker-compose, k8s (ops)
├── build/                    # Dockerfile and build assets
├── docs/                     # Design & ops docs
├── scripts/                  # Dev helpers
├── Makefile
├── go.mod
└── README.md
```

## Layering

```text
cmd/*  →  internal/broker  →  storage / cluster / consumer
                ↓
         transport/server  ←  transport/protocol
                ↓
            pkg/client (apps & CLIs)
```

## Module

```text
github.com/distributed-system/streamforge
```

## Branding

| | |
|--|--|
| **Product** | StreamForge |
| **Full name** | StreamForge Distributed Event Streaming Broker |
| **Env prefix** | `SF_` |
| **Metrics** | `streamforge_*` |
