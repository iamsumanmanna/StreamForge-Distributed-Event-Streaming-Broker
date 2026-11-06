# StreamForge

**StreamForge Distributed Event Streaming Broker** — open-source (Apache-2.0) software for durable, real-time event streaming: partitioned logs, produce/fetch, replication, and consumer groups. Built in Go. You own the code.

> Product name: **StreamForge**  
> Full name: **StreamForge Distributed Event Streaming Broker**

## Quick start

```bash
docker compose -f deployments/docker-compose.yml up --build

go run ./cmd/produce -addr 127.0.0.1:9092 -topic orders -partitions 3 -count 100 \
  -acks -1 -compression gzip
go run ./cmd/consume -addr 127.0.0.1:9092 -topic orders -group workers
```

Single node:

```bash
go run ./cmd/broker -id broker-1 -listen :9092 -http :8080 -data ./data/broker-1
```

```bash
make up && make produce-mk && make consume-mk
make test
```

## Repository layout

| Path | Role |
|------|------|
| `cmd/` | Binaries (`broker`, `produce`, `consume`) |
| `internal/broker` | Core handlers |
| `internal/storage` | Log + compression |
| `internal/cluster` | Raft metadata + replication |
| `internal/consumer` | Consumer groups |
| `internal/transport` | TCP protocol + server |
| `internal/config` | Flags / env (`SF_*`) |
| `pkg/client` | **Public** Go client SDK |
| `test/integration` | E2E tests |
| `configs/` | Example env files |
| `deployments/` | Compose / ops |
| `build/` | Dockerfile |
| `docs/` | Design & ops |

Full map: [docs/architecture.md](docs/architecture.md).

## Docs

- [setup.md](docs/setup.md) — **how to install and run**  
- [architecture.md](docs/architecture.md) — enterprise folder layout  
- [design.md](docs/design.md) — storage, protocol, replication  
- [production.md](docs/production.md) — acks, TLS, auth, compression  
- [scale.md](docs/scale.md) — capacity notes  

## License

[Apache License 2.0](LICENSE) — see [NOTICE](NOTICE).
