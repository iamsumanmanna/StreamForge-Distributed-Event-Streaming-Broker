# How to set up StreamForge

**StreamForge Distributed Event Streaming Broker** — install, run, and verify.

> After install, follow **[usage.md](usage.md)** for a full “how to use” guide with verify steps.

## Requirements

| Tool | Version | Notes |
|------|---------|--------|
| Go | 1.22+ | For local build / `go run` |
| Docker + Docker Compose | Recent | For the 3-node cluster |
| Git | Any | Clone the repository |
| Make (optional) | Any | Shortcuts in `Makefile` |

Supported OS: Linux, macOS, Windows (PowerShell or WSL).

---

## 1. Get the code

```bash
git clone https://github.com/iamsumanmanna/StreamForge-Distributed-Event-Streaming-Broke.git
cd StreamForge-Distributed-Event-Streaming-Broke
```

Download Go modules:

```bash
go mod download
```

---

## 2. Choose how to run

| Mode | Best for | Command |
|------|----------|---------|
| **A. Docker cluster (3 nodes)** | Local demo / closest to production | `docker compose … up --build` |
| **B. Single broker (Go)** | Quick develop / debug | `go run ./cmd/broker …` |
| **C. Built binaries** | Repeatable local installs | `make build` then run `bin/broker` |

---

## 3A. Setup with Docker (recommended)

### Start the cluster

From the repo root:

```bash
docker compose -f deployments/docker-compose.yml up --build -d
```

Or:

```bash
make up
```

This starts three brokers:

| Node | TCP (produce/consume) | Health / metrics |
|------|------------------------|------------------|
| broker-1 | `127.0.0.1:9092` | http://127.0.0.1:8080/healthz |
| broker-2 | `127.0.0.1:9093` | http://127.0.0.1:8081/healthz |
| broker-3 | `127.0.0.1:9094` | http://127.0.0.1:8082/healthz |

### Check health

```bash
curl http://127.0.0.1:8080/healthz
# expect: ok
```

### Produce and consume

```bash
go run ./cmd/produce -addr 127.0.0.1:9092 -topic orders -partitions 3 -count 50 \
  -acks -1 -compression gzip

go run ./cmd/consume -addr 127.0.0.1:9092 -topic orders -group workers
```

### Stop

```bash
docker compose -f deployments/docker-compose.yml down
# or: make down

# also remove volumes (wipes data):
docker compose -f deployments/docker-compose.yml down -v
```

---

## 3B. Setup single broker (no Docker)

```bash
mkdir -p data/broker-1

go run ./cmd/broker \
  -id broker-1 \
  -listen :9092 \
  -http :8080 \
  -data ./data/broker-1
```

Leave this terminal open. In another terminal:

```bash
go run ./cmd/produce -addr 127.0.0.1:9092 -topic demo -partitions 1 -count 10 -value hello
go run ./cmd/consume -addr 127.0.0.1:9092 -topic demo -partition 0
```

Stop the broker with `Ctrl+C`.

---

## 3C. Setup from binaries

```bash
make build
# creates: bin/broker  bin/produce  bin/consume
```

Windows (PowerShell):

```powershell
go build -o bin/broker.exe ./cmd/broker
go build -o bin/produce.exe ./cmd/produce
go build -o bin/consume.exe ./cmd/consume

.\bin\broker.exe -id broker-1 -listen :9092 -http :8080 -data ./data/broker-1
```

Linux / macOS:

```bash
./bin/broker -id broker-1 -listen :9092 -http :8080 -data ./data/broker-1
./bin/produce -addr 127.0.0.1:9092 -topic demo -count 5 -value hello
./bin/consume -addr 127.0.0.1:9092 -topic demo -group g1
```

---

## 4. Configuration

### Flags (broker)

| Flag | Default | Meaning |
|------|---------|---------|
| `-id` | `broker-1` | Node ID |
| `-listen` | `:9092` | Client TCP port |
| `-http` | `:8080` | Health + Prometheus `/metrics` |
| `-data` | `./data` | Data directory |
| `-cluster` | `false` | Enable Raft cluster |
| `-rf` | `1` | Replication factor |
| `-peers` | — | `id=host:raftPort,...` |
| `-raft-bind` | `:7000` | Raft bind address |
| `-acks` | `-1` | Default produce acks |
| `-compression` | `none` | Default codec |
| `-tls-cert` / `-tls-key` | — | Enable TLS |
| `-auth-token` | — | Shared token auth |

### Environment variables

Copy the example file:

```bash
cp configs/broker.example.env .env
# edit SF_* values, then export them or use with your process manager
```

Prefix is **`SF_`** (see [configs/broker.example.env](../configs/broker.example.env)).

---

## 5. Optional: TLS + auth

```bash
# Broker
go run ./cmd/broker -id broker-1 -listen :9092 -data ./data/broker-1 \
  -tls-cert cert.pem -tls-key key.pem -auth-token mysecret

# Clients
go run ./cmd/produce -addr 127.0.0.1:9092 -tls -auth-token mysecret \
  -topic orders -value hello -acks -1

go run ./cmd/consume -addr 127.0.0.1:9092 -tls -auth-token mysecret \
  -topic orders -group workers
```

---

## 6. Use from your Go app

```go
import "github.com/distributed-system/streamforge/pkg/client"

func main() {
    c, err := client.Dial("127.0.0.1:9092")
    if err != nil {
        panic(err)
    }
    defer c.Close()

    _ = c.CreateTopic("orders", 3)
    _, _ = c.Produce("orders", 0, []byte("key"), []byte("hello"))
}
```

---

## 7. Verify installation

```bash
# Unit + integration tests
make test
# or: go test ./...

# Health
curl http://127.0.0.1:8080/healthz

# Metrics
curl http://127.0.0.1:8080/metrics
```

---

## Troubleshooting

| Problem | What to try |
|---------|-------------|
| Port already in use | Change `-listen` / compose ports, or stop the other process |
| `connection refused` | Broker not up yet — wait a few seconds after `compose up` |
| Docker build fails | Ensure Docker is running; retry `compose up --build` |
| Windows path / Go errors | Use PowerShell from repo root; install Go 1.22+ |
| Consume shows nothing | Produce first; check topic name and group offsets |

---

## Next docs

- [production.md](production.md) — acks, compression, TLS details  
- [design.md](design.md) — how the broker works  
- [architecture.md](architecture.md) — folder layout  
- [scale.md](scale.md) — capacity notes  
