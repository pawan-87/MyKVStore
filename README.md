# MyKVStore
A distributed, reliable key-value store built to learn the [Raft consensus algorithm](https://raft.github.io/) and [etcd](https://etcd.io/).

> **Note:** This is a personal learning project to understand etcd internals. It does not implement all etcd features and is not production-ready.

## Architecture

<img width="1367" height="813" alt="Screenshot 2026-02-08 at 11 33 40 PM" src="https://github.com/user-attachments/assets/e9aa1782-a324-4c41-87be-c1f8bcc11359" />


## Learnings

| Topic | Doc |
|-------|-----|
| Raft Leader Election | [learnings/LeaderElection.md](learnings/LeaderElection.md) |
| MyKVStore Architecture | [learnings/MyKVStoreArchitecture.md](learnings/MyKVStoreArchitecture.md) |


## Tech Stack

| Layer | Technology |
|-------|-----------|
| Consensus | [`go.etcd.io/raft/v3`](https://github.com/etcd-io/raft) |
| Storage | [`go.etcd.io/bbolt`](https://github.com/etcd-io/bbolt) |
| API | [gRPC](https://grpc.io/) + [Protocol Buffers](https://protobuf.dev/) |
| Logging | [`go.uber.org/zap`](https://github.com/uber-go/zap) |
| Language | Go 1.21+ |

See the [examples/](https://github.com/pawan-87/MyKVStore/tree/main/examples) directory for working code samples.
-

## References

- [etcd GitHub](https://github.com/etcd-io/etcd) · [etcd Docs](https://etcd.io/docs/)
- [Raft Paper](https://raft.github.io/raft.pdf) · [Raft Visualization](http://thesecretlivesofdata.com/raft/)
- [go.etcd.io/raft](https://github.com/etcd-io/raft) · [bbolt](https://github.com/etcd-io/bbolt) · [gRPC Go](https://grpc.io/docs/languages/go/)


## Contact

**Pawan Mehta** — arowpk@gmail.com
