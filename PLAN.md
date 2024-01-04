# Raft KV

## Project Overview
Distributed key-value store in Go using the Raft consensus algorithm for strong consistency across replicated nodes. Implements leader election, log replication, and fault tolerance under node failures. Exposes a gRPC API for reads and writes. Docker-based multi-node simulation for evaluating consistency and availability tradeoffs under network partitions.

## Tech Stack
- **Language:** Go 1.22+
- **Consensus:** Raft (custom implementation)
- **RPC:** gRPC + Protobuf
- **Storage:** In-memory + WAL (write-ahead log)
- **Deployment:** Docker, Docker Compose (multi-node)
- **Testing:** Go testing, Docker network partitions

## Architecture Overview
```
Client (gRPC)
     |
     v
[Leader Node]  <--- Raft consensus ---> [Follower 1] [Follower 2]
     |
  [KV Store]
  [WAL Log]
```
