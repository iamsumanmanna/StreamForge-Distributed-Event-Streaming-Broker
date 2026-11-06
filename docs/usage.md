# How to use StreamForge (with verify)

**StreamForge Distributed Event Streaming Broker**

This guide explains **what each part does**, **how to use it step by step**, and **how to verify** that it works.

---

## What you are using

```text
  Your app / CLI                StreamForge broker                 Your workers
  ─────────────                 ─────────────────                 ────────────
  produce  ──────TCP──────►  topics + partitions  ──────TCP──────►  consume
                             (durable log on disk)
```

| Piece | Command / package | Job |
|-------|-------------------|-----|
| **Broker** | `cmd/broker` | Stores messages, replicates, serves clients |
| **Produce** | `cmd/produce` | Sends (publishes) messages into a topic |
| **Consume** | `cmd/consume` | Reads (subscribes) messages from a topic |
| **Go SDK** | `pkg/client` | Same protocol inside your own Go apps |

**Topic** = named stream (example: `orders`)  
**Partition** = ordered slice of a topic (for parallelism)  
**Consumer group** = set of workers that share the work (`-group workers`)

---

## Step 0 — One-time setup

```bash
git clone https://github.com/iamsumanmanna/StreamForge-Distributed-Event-Streaming-Broke.git
cd StreamForge-Distributed-Event-Streaming-Broke
# or if you already have the folder:
cd DISTRIBUTED_SYSTEM

go mod download
```

**Verify setup**

```bash
go version          # need 1.22+
go test ./...       # all packages should pass (ok)
```

---

## Step 1 — Start the broker

### Option A — Docker cluster (3 nodes)

```bash
docker compose -f deployments/docker-compose.yml up --build -d
```

**Verify broker is up**

```bash
curl http://127.0.0.1:8080/healthz
```

Expected:

```text
ok
```

Also check:

```bash
curl http://127.0.0.1:8080/metrics
# should show streamforge_produce_total / streamforge_fetch_total lines
```

| Port | Use |
|------|-----|
| `9092` | Main client TCP (produce / consume) |
| `9093`, `9094` | Other brokers in the cluster |
| `8080` | Health + metrics for broker-1 |

### Option B — Single broker (no Docker)

Terminal 1:

```bash
mkdir -p data/broker-1
go run ./cmd/broker -id broker-1 -listen :9092 -http :8080 -data ./data/broker-1
```

**Verify**

```bash
curl http://127.0.0.1:8080/healthz
# ok
```

---

## Step 2 — Produce messages (send data)

Open a **new** terminal (broker must keep running).

```bash
go run ./cmd/produce \
  -addr 127.0.0.1:9092 \
  -topic orders \
  -partitions 3 \
  -count 20 \
  -acks -1 \
  -compression gzip
```

What this means:

| Flag | Meaning |
|------|---------|
| `-addr` | Broker address |
| `-topic orders` | Stream name |
| `-partitions 3` | Create topic with 3 partitions if needed |
| `-count 20` | Send 20 messages |
| `-acks -1` | Wait for durable/majority ack |
| `-compression gzip` | Compress payload on the wire/log |

**Verify produce**

You should see lines like:

```text
produced topic=orders partition=0 offset=0 acks=-1 compression=gzip
produced topic=orders partition=1 offset=0 acks=-1 compression=gzip
...
```

If you see `connection refused`, the broker is not running (repeat Step 1).

Send one custom message:

```bash
go run ./cmd/produce -addr 127.0.0.1:9092 -topic orders \
  -key user-42 -value "order-created" -acks 1
```

---

## Step 3 — Consume messages (read data)

### A) Simple read (one partition)

```bash
go run ./cmd/consume \
  -addr 127.0.0.1:9092 \
  -topic orders \
  -partition 0
```

**Verify consume**

You should see:

```text
partition=0 offset=0 key=... value=...
partition=0 offset=1 key=... value=...
```

Press `Ctrl+C` to stop.

### B) Consumer group (realtime workers — recommended)

```bash
go run ./cmd/consume \
  -addr 127.0.0.1:9092 \
  -topic orders \
  -group workers
```

What happens:

1. Joins group `workers`  
2. Gets partition assignments  
3. Fetches new messages in a loop  
4. Commits offsets (saved on disk — survives broker restart)

**Verify group**

Look for:

```text
joined group member=... gen=... leader=...
assigned [{orders 0} {orders 1} ...]
partition=0 offset=... key=... value=...
```

Run a **second** consume with the same `-group workers` in another terminal — partitions are shared between the two workers.

---

## Step 4 — End-to-end verify checklist

Run these in order and tick each box:

| # | Action | Pass if |
|---|--------|---------|
| 1 | `curl http://127.0.0.1:8080/healthz` | prints `ok` |
| 2 | Produce 20 messages to `orders` | prints `produced ... offset=...` |
| 3 | Consume with `-group workers` | prints message lines |
| 4 | Produce again while consume is running | new lines appear without restart |
| 5 | `go test ./...` | all `ok` |

Quick one-liner flow (broker already up):

```bash
go run ./cmd/produce -addr 127.0.0.1:9092 -topic orders -partitions 3 -count 5 -value hello
go run ./cmd/consume -addr 127.0.0.1:9092 -topic orders -group verify -from-beginning
```

Stop after you see the 5 messages (`Ctrl+C`).

---

## Step 5 — Use inside your Go application

```go
package main

import (
    "fmt"
    "log"

    "github.com/distributed-system/streamforge/pkg/client"
)

func main() {
    c, err := client.Dial("127.0.0.1:9092")
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    if err := c.CreateTopic("orders", 3); err != nil {
        log.Fatal(err)
    }

    // SEND
    off, err := c.Produce("orders", 0, []byte("user-1"), []byte(`{"status":"paid"}`))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("produced offset", off)

    // RECEIVE
    resp, err := c.Fetch("orders", 0, off, 1<<20)
    if err != nil {
        log.Fatal(err)
    }
    for _, r := range resp.Records {
        fmt.Printf("got offset=%d value=%s\n", r.Offset, r.Value)
    }
}
```

**Verify in app:** print shows the same `value` you produced.

With auth:

```go
c, err := client.DialWithOptions("127.0.0.1:9092", client.DialOptions{
    AuthToken: "mysecret",
    TLS:       false,
})
```

---

## Common flags (cheat sheet)

### Produce

```text
-addr            broker host:port
-topic           topic name
-partitions      create with N partitions
-key / -value    message key and body
-count           how many messages
-acks            0 | 1 | -1 (all)
-compression     none | gzip | snappy | lz4 | zstd
-tls             enable TLS
-auth-token      shared secret
```

### Consume

```text
-addr            broker host:port
-topic           topic name
-partition       fixed partition (-1 if using group)
-group           consumer group id
-from-beginning  ignore stored offsets, start at 0
-tls / -auth-token
```

---

## Typical realtime pattern

```text
1. Start StreamForge broker(s)
2. API / service PRODUCES events  →  topic "orders"
3. Worker CONSUMES with -group     →  update DB / push WebSocket / send email
4. More workers with same -group   →  scale out automatically by partition
```

Example:

```bash
# Terminal 1 — broker (or Docker already running)

# Terminal 2 — API simulator
go run ./cmd/produce -addr 127.0.0.1:9092 -topic chat -partitions 2 -count 100 -value "hi"

# Terminal 3 — realtime worker
go run ./cmd/consume -addr 127.0.0.1:9092 -topic chat -group chat-push
```

**Verify realtime:** leave consume running, produce more messages — they appear within a fraction of a second.

---

## Stop and clean up

```bash
# Docker
docker compose -f deployments/docker-compose.yml down
docker compose -f deployments/docker-compose.yml down -v   # also delete data

# Local broker
Ctrl+C in the broker terminal
```

---

## If something fails

| Symptom | Fix |
|---------|-----|
| `connection refused` | Start broker first; wait ~5s after Docker up |
| Healthz not `ok` | Check Docker: `docker ps` — containers must be running |
| Consume empty | Produce first; or use `-from-beginning` |
| Unauthorized | Pass same `-auth-token` on broker and client |
| Port in use | Change `-listen` or stop the process using `9092` |

More install detail: [setup.md](setup.md)  
Hardening (TLS, acks): [production.md](production.md)
