# TinyKafka

[![CI](https://github.com/Tenaryo/TinyKafka/actions/workflows/ci.yml/badge.svg)](https://github.com/Tenaryo/TinyKafka/actions/workflows/ci.yml)
[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](https://en.cppreference.com/w/cpp/23)
[![Coverage](https://img.shields.io/badge/coverage-93.3%25-brightgreen.svg)](.)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Lines](https://img.shields.io/badge/core-~3%2C800%20LOC-lightgrey.svg)](.)

A Kafka broker written from scratch in C++23 — ~3,800 LOC core, 6,600 LOC tests, **zero external dependencies**. Supports 14 Kafka wire protocols (Produce, Fetch, Metadata, Consumer Groups, Join/Sync/Heartbeat, OffsetCommit/Fetch, ListOffsets, and more).

## Highlights

- **14 Kafka API keys** — full produce/fetch pipeline, KRaft metadata, consumer group coordination (JoinGroup→SyncGroup→Heartbeat state machine with session timeout)
- **93.3% test coverage** — 140 GoogleTest cases (parser × 54, serializer × 32, broker × 30, integration × 14, config × 5, metrics × 4, cluster metadata × 1)
- **128K msg/s** (loopback, 1KB, 8 connections, Release build)
- **Zero dependencies** — C++23 STL + POSIX sockets + epoll, no Boost, no ZooKeeper, no Java
- **Production-grade CI** — TSan + ASan + UBSan + clang-tidy + 80% coverage gate (see FIX-1, commit 1156c30)

## Quick Start

```bash
git clone https://github.com/Tenaryo/TinyKafka.git
cd TinyKafka

# Build
./build.sh             # Debug (default)
./build.sh Release     # Release (-O3)

# Test
./run_tests.sh         # 140 tests, < 2s
./run_tests.sh --coverage  # lcov + genhtml report

# Benchmark
cmake -B benchmark/build -S benchmark -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build benchmark/build --target producer_bench
./benchmark/run_all.sh # 8 scenarios, Producer + Consumer throughput
```

## Supported APIs

| Key | Name | Version | Description |
|-----|------|---------|-------------|
| 0 | Produce | 0–11 | Record batch v2, segment roll, sparse offset index |
| 1 | Fetch | 0–16 | Full + incremental (offset/max_bytes via index lookup) |
| 2 | ListOffsets | 0–8 | Earliest (0) / latest (PartitionContext offset) |
| 3 | Metadata | 0–12 | Cluster topology, broker list, topic UUID |
| 8 | OffsetCommit | 0–8 | In-memory offset store (group→topic→partition) |
| 9 | OffsetFetch | 0–8 | Read committed offsets from memory |
| 10 | FindCoordinator | 0–4 | Returns self (single-broker) |
| 11 | JoinGroup | 0–9 | Member registration, leader election, generation counter |
| 12 | Heartbeat | 0–4 | Generation validation + session timeout eviction |
| 13 | LeaveGroup | 0–5 | Member removal, state transition to AwaitingSync |
| 14 | SyncGroup | 0–5 | Leader submits assignments, follower retrieves |
| 18 | ApiVersions | 0–4 | Capability negotiation |

## Architecture

```
src/
├── broker/       Batch handler, PartitionContext, GroupCoordinator
├── cluster/      KRaft metadata (topic/partition records)
├── config/       Properties file + CLI override
├── logging/      Structured UTC logging (gettid, lock-free)
├── net/          Epoll reactor, non-blocking I/O, backpressure
├── protocol/     Wire format parser + serializer (14 APIs)
├── storage/      Log segment reader, partition log paths
└── util/         Byte reader/writer, varint, endian, record batch v2 parser
```

## Performance

Release build (-O3), loopback, single broker, 50K messages per scenario:

| Scenario | msg/s | MB/s | P50 |
|----------|-------|------|-----|
| 100B | 30,884 | 2.9 | 30μs |
| 1KB | 30,438 | 29.7 | 30μs |
| 10KB | 26,260 | 256.4 | 34μs |
| 100KB | 9,962 | 972.9 | 79μs |
| 1KB × 4 conn | 112,212 | 109.6 | 32μs |
| 1KB × 8 conn | 128,947 | 125.9 | 59μs |

Run `./benchmark/run_all.sh` to reproduce on your machine.

## License

MIT — see [LICENSE](LICENSE).
