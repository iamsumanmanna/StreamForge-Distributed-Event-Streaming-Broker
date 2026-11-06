# Run StreamForge

**StreamForge Distributed Event Streaming Broker**

```bash
docker compose -f deployments/docker-compose.yml up --build
go run ./cmd/produce -addr 127.0.0.1:9092 -topic orders -partitions 3 -count 100 \
  -acks -1 -compression gzip
go run ./cmd/consume -addr 127.0.0.1:9092 -topic orders -group workers
```

Example env: [configs/broker.example.env](../configs/broker.example.env) (`SF_*`).

## Hardened features

| Feature | Flags / behavior |
|---------|------------------|
| Durable group offsets | Saved under `data/groups/offsets.json` |
| Produce acks | `-acks 0\|1\|-1` (all) |
| Compression | `-compression none\|gzip\|snappy\|lz4\|zstd` |
| TLS | Broker: `-tls-cert` `-tls-key`; client: `-tls` |
| Shared token auth | Broker: `-auth-token`; client: `-auth-token` |

```bash
go run ./cmd/broker -id broker-1 -listen :9092 -data ./data/broker-1 \
  -tls-cert cert.pem -tls-key key.pem -auth-token mysecret

go run ./cmd/produce -addr 127.0.0.1:9092 -tls -auth-token mysecret \
  -topic orders -value hello -acks -1 -compression snappy
```

## Client SDK

```go
import "github.com/distributed-system/streamforge/pkg/client"

c, err := client.DialWithOptions("127.0.0.1:9092", client.DialOptions{
    AuthToken: "mysecret",
})
```

License: Apache-2.0 — [LICENSE](../LICENSE).
