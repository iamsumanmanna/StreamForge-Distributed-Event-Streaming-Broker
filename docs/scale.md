# Scale notes

**StreamForge Distributed Event Streaming Broker** is a custom broker you can grow. Throughput depends on your hardware, fsync settings, partition count, and replication factor.

## Practical levers

- More partitions → more parallel produce/consume  
- More brokers (compose already has 3) → more replica capacity  
- Tune `FsyncEveryN` vs durability  
- Keep hot topics on fast disks  

## Honest limit

Matching industry brokers at extreme rates (100k–1M+/sec) needs years of engineering and ops. Treat StreamForge as your product baseline and harden via [production.md](production.md).
