# StreamForge design

**StreamForge Distributed Event Streaming Broker** — custom wire protocol (not Apache Kafka wire-compatible).

## Storage

Each partition is an ordered sequence of **segments**:

- `{baseOffset}.log` — length-prefixed records with Castagnoli CRC + attrs (compression)
- `{baseOffset}.index` — sparse map of relative offset → file position

On crash recovery the broker scans each `.log`, truncates a trailing partial record, and rebuilds the index.

## Durability

- Appends use `Write`; `fsync` every `FsyncEveryN` records and on a timer.
- Produce **acks**: `0` fire-and-forget, `1` leader, `-1` majority replicas.
- Consumers only see offsets below the **high-watermark (HWM)**.

## Compression

Record attrs store codec (`none`, `gzip`, `snappy`, `lz4`, `zstd`). Values are compressed on produce and decompressed on fetch.

## Replication

Raft stores topic metadata. Leaders fan out `ReplicaAppend` to followers and advance HWM on majority ack when `acks=-1`.

## Consumer groups

Coordinator (Join/Sync/Heartbeat). Committed offsets are **persisted** to `data/groups/offsets.json`.

## Security

Optional TLS on the broker TCP port and optional shared-token `APIAuth` handshake.

## Protocol

Length-prefixed TCP frames. Use `cmd/produce`, `cmd/consume`, or `pkg/client`.
