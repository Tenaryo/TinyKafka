# TinyKafka

[![CI](https://github.com/Tenaryo/TinyKafka/actions/workflows/ci.yml/badge.svg)](https://github.com/Tenaryo/TinyKafka/actions/workflows/ci.yml)
[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](https://en.cppreference.com/w/cpp/23)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A Kafka broker written from scratch in C++23 (~1,800 LOC core, 3,200 LOC tests, zero external dependencies). Implements the KRaft cluster metadata protocol and core Kafka wire protocol (API keys 0, 1, 18, 75), supporting produce, fetch, topic discovery, and version negotiation with disk-backed persistence over raw TCP.

## Table of Contents

- [Highlights](#highlights)
- [Features](#features)
  - [Supported API Keys](#supported-api-keys)
  - [Protocol Coverage](#protocol-coverage)
- [Architecture](#architecture)
  - [Project Structure](#project-structure)
  - [Module Breakdown](#module-breakdown)
  - [Request Lifecycle](#request-lifecycle)
- [Key Design Decisions](#key-design-decisions)
- [Usage](#usage)
  - [Prerequisites](#prerequisites)
  - [Build](#build)
  - [Run](#run)
- [Testing](#testing)
- [License](#license)

## Highlights

| Category | Detail |
|----------|--------|
| **Language** | C++23 throughout — `std::expected`, `std::variant`, `std::visit`, designated initializers, `std::ranges`, `constexpr` utilities |
| **Dependencies** | Zero. C++23 stdlib + POSIX sockets only |
| **Protocol** | Full manual implementation of the Kafka wire format — big-endian binary encoding, unsigned/signed varints, compact string arrays, UUID fields, TAG_BUFFER sections |
| **Metadata** | KRaft-mode cluster metadata log parser (record batch v2 → topic records → partition records), with O(1) UUID/name lookups |
| **Concurrency** | One detached `std::thread` per client connection, parallel request processing |
| **Error handling** | `std::expected<T, std::error_code>` throughout — no exceptions in control flow |
| **Storage** | Disk-backed partitioned topic logs following real Kafka's naming convention (`{topic}-{partition}/00000000000000000000.log`) |
| **Memory safety** | `std::span`-based zero-copy reads, RAII socket management (move-only `Server`), bounds-checked `ByteReader`/`ByteWriter` |
| **Build** | CMake + Ninja, `-Wall -Wextra -Wpedantic -Werror -Wshadow -Wconversion`, optional AddressSanitizer + UBSan |
| **Testing** | 30+ test cases across 5 suites — unit tests for every module + full integration tests spawning the real binary and verifying wire-level response bytes |

## Features

### Supported API Keys

| API Key | Name | Versions | Max Version | Description |
|---------|------|----------|-------------|-------------|
| 18 | `ApiVersions` | 0–4 | 4 | Negotiate supported API key ranges; returns `UNSUPPORTED_VERSION (35)` for out-of-range |
| 75 | `DescribeTopicPartitions` | 0 | 0 | Topic/partition metadata discovery; returns `UNKNOWN_TOPIC_OR_PARTITION (3)` for unknown topics; results sorted alphabetically |
| 1 | `Fetch` | 0–16 | 16 | Consumer fetch — reads partition log segments from disk by topic UUID; returns `UNKNOWN_TOPIC_ID (100)` for unknown topics |
| 0 | `Produce` | 0–11 | 11 | Producer write — appends record batches to `{root}/{topic}-{partition}/00000000000000000000.log` |

### Protocol Coverage

- **Request parsing**: 4-byte length-prefixed binary frames → `switch` on API key → per-API request body parsing with `ByteReader`
- **Response serialization**: Pre-computes exact body size → allocates single buffer → fills via `ByteWriter` → sends in one `send_all()` call
- **Compact array encoding**: `length = N + 1` unsigned varint prefix (real Kafka v2 message format)
- **TAG_BUFFER support**: All request/response structs include proper tagged field sections (0x00 byte)
- **UUID handling**: 16-byte topic IDs with custom `UuidHash` for `unordered_map` lookup — produces on topic name, fetches on topic UUID

## Architecture

### Project Structure

```
src/
├── main.cpp                     # Entry point: parse __cluster_metadata-0, bind :9092,
│                                # accept loop spawning detached worker threads
├── broker/
│   ├── broker.hpp/cpp           # Broker: std::visit-based request→response dispatch,
│                                # topic lookup (by name & UUID), metadata building
├── cluster/
│   ├── metadata.hpp/cpp         # KRaft metadata parser:
│                                #   record batch v2 → records (topic/partition type) →
│                                #   ClusterMetadata with bidirectional maps
├── net/
│   ├── server.hpp/cpp           # Server: POSIX socket()/bind()/listen()/accept() with
│                                #   SO_REUSEADDR, RAII close, move-only semantics
│   ├── socket.hpp/cpp           # send_all() / recv_all() — loop until all bytes transferred
├── protocol/
│   ├── request.hpp              # Request variant: ApiVersions, DescribeTopicPartitions,
│                                #   Fetch, Produce (each with typed sub-structs)
│   ├── response.hpp             # Response variant: corresponding response types with all
│                                #   spec-mandated fields (epochs, replicas, ISRs, offsets)
│   ├── parser.hpp/cpp           # Binary → Request: reads API key/version/correlation_id,
│                                #   dispatches per-API body parsing (~290 LOC)
│   ├── serializer.hpp/cpp       # Response → Binary: size computation + buffer fill via
│                                #   std::visit + overloaded, ~225 LOC
│   ├── api_registry.hpp         # Compile-time table of 4 supported API key ranges
├── storage/
│   ├── log_reader.hpp/cpp       # read_topic_log(): reads full partition log into vector
│   ├── log_writer.hpp/cpp       # write_topic_log(): creates directories, appends to log
└── util/
    ├── byte_reader.hpp          # Sequential big-endian binary reader — bounds-checked,
    │                            #   constexpr-friendly, ~107 LOC
    ├── byte_writer.hpp          # Sequential big-endian binary writer — ~68 LOC
    ├── endian.hpp               # decode_int16_be / decode_int32_be / write_int{16,32,64}_be
    ├── varint.hpp               # Unsigned varint, zigzag-encoded signed varint,
    │                            #   compact string (length+1 variant) — all constexpr
    └── overloaded.hpp           # template helper: std::visit pattern matching
```

### Module Breakdown

#### `main.cpp` — Event Loop

```
1. parse_cluster_metadata_file() → ClusterMetadata (topic names, UUIDs, partitions)
2. Server::create(9092) → bind TCP socket
3. while(true):
     accept() → client_fd
     std::thread([client_fd]:
       while(true):
         recv_all() → 4-byte message length
         recv_all() → message body
         parse_request() → Request variant
         Broker::handle() → Response variant
         serialize() → byte vector
         send_all() → client
     ).detach()
```

Each client connection runs in its own detached thread, allowing multiple concurrent producers and consumers.

#### `broker/` — Request Dispatch

The `Broker` class holds a `ClusterMetadata` reference and a log root path. Its single public method `handle(const Request&) -> Response` uses `std::visit` with the `overloaded` pattern:

```cpp
return std::visit(overloaded{
    [](const ApiVersionsRequest& r) -> Response { ... },
    [this](const DescribeTopicPartitionsRequest& r) -> Response { ... },
    [this](const ProduceRequest& r) -> Response { ... },
    [this](const FetchRequest& r) -> Response { ... },
}, req);
```

- **ApiVersions**: Validates `api_version ∈ [0,4]`, returns error 35 if unsupported, otherwise returns all 4 API key entries
- **DescribeTopicPartitions**: Looks up each requested topic by name, builds `TopicMetadata` with partition lists; unknown topics get error 3; results sorted alphabetically
- **Produce**: Looks up topic by name, validates partition exists, writes record batches to disk via `storage::write_topic_log()`
- **Fetch**: Looks up topic by UUID, reads partition log from disk via `storage::read_topic_log()`, returns raw record bytes

#### `cluster/` — KRaft Metadata Parser

Parses the Kafka KRaft metadata log (`__cluster_metadata-0/00000000000000000000.log`) which is a sequence of record batch v2 structures (magic byte = 2). Each record's value is itself a compact-varint-encoded frame:

```
record value →
  frame_version (varint)
  api_key (varint): 2 = topic, 3 = partition
  version (varint)
  payload:
    topic record:    name (compact string) + uuid (16 bytes)
    partition record: partition_id (int32) + uuid (16 bytes) + replicas/ISR data
```

The parser walks record batches, discards non-topic/partition record types, then links partitions to topics by UUID, producing a `ClusterMetadata` struct with:
- `topics: vector<TopicInfo>` — ordered list
- `name_to_topic: unordered_map<string, size_t>` — O(1) name lookup
- `uuid_to_topic: unordered_map<array<uint8_t,16>, size_t, UuidHash>` — O(1) UUID lookup

#### `protocol/` — Wire Protocol

**Parser** (`parser.cpp`, 290 LOC): Reads a binary buffer sequentially. The 4-byte message-length header is consumed by the caller; the parser reads:
1. `api_key` (int16 BE)
2. `api_version` (int16 BE)
3. `correlation_id` (int32 BE)
4. `switch(api_key)` dispatches to per-API body parsing

Each API handler skips header fields (client_id, TAG_BUFFERs), then reads the body's compact arrays. For Produce, record batch bytes are copied into an owning `vector<uint8_t>`. For Fetch, partition-level fields (`current_leader_epoch`, `fetch_offset`, etc.) are skipped — only `partition_index` is retained.

**Serializer** (`serializer.cpp`, 225 LOC): Uses `std::visit` on the `Response` variant. For each response type:
1. Computes exact body size by walking all nested arrays
2. Allocates `vector<uint8_t>(4 + body_size)`
3. Writes 4-byte BE message length + response body via `ByteWriter`
4. Returns the buffer for `send_all()`

**API Registry** (`api_registry.hpp`): Compile-time `constexpr std::array<ApiVersionEntry, 4>` defining the supported API key ranges.

#### `net/` — TCP Server

- **`Server`**: Factory method `create(port)` calls `socket()` → `setsockopt(SO_REUSEADDR)` → `bind()` → `listen()`. Move-only semantics (RAII close on destructor). No exceptions — returns `std::expected<Server, std::error_code>`.
- **`send_all` / `recv_all`**: Loop `::send()` / `::read()` until all requested bytes are transferred or socket error occurs. Return `std::expected<size_t, std::error_code>`.

#### `storage/` — Disk Logs

Follows real Kafka's on-disk layout:
```
/tmp/kraft-combined-logs/
  __cluster_metadata-0/
    00000000000000000000.log     ← KRaft metadata (read at startup)
  <topic>-<partition>/
    00000000000000000000.log     ← user topic logs (append on produce, read on fetch)
```

- **`write_topic_log()`**: Creates directories with `std::filesystem::create_directories()`, opens file in binary append mode, writes record bytes. Returns `std::error_code`.
- **`read_topic_log()`**: Opens file, reads entire content into `vector<uint8_t>`. Uses `std::ios::ate` to get size before allocation.

#### `util/` — Binary Primitives

- **`ByteReader`**: Non-owning, bounds-checked sequential reader over `std::span<const uint8_t>`. Supports `read_int8/16/32`, `read_bytes` (zero-copy span view), `read_varint`, `read_signed_varint` (zigzag), `read_compact_string`, `skip`, `skip_varint`. All methods return `std::expected`.
- **`ByteWriter`**: Non-owning sequential writer over `std::span<uint8_t>`. Supports `write_int8/16/32/64`, `write_bytes`, `write_varint`, `write_signed_varint`, `write_compact_string`. No bounds checking (caller pre-allocates).
- **`varint.hpp`**: Full `constexpr` varint implementation:
  - `read_unsigned_varint` / `write_unsigned_varint` — standard variable-length unsigned integer
  - `zigzag_encode` / `zigzag_decode` — maps signed int32 to unsigned uint32 for compact signed varints
  - `read_signed_varint` / `write_signed_varint` — zigzag + unsigned varint
  - `read_compact_string` — varint-prefixed (length+1) string
  - `varint_encoded_size` / `signed_varint_encoded_size` — pre-compute output size without allocating
- **`endian.hpp`**: `decode_int16_be` / `decode_int32_be` / `write_int16_be` / `write_int32_be` / `write_int64_be` — manual big-endian conversion over `std::span` slices
- **`overloaded.hpp`**: `template<typename... Ts> struct overloaded : Ts... { using Ts::operator()...; }` — enables `std::visit` without defining a separate visitor class

### Request Lifecycle

```
Client TCP bytes
    │
    ▼
┌──────────────────┐
│  recv_all(4)      │  Read 4-byte big-endian message size prefix
│  recv_all(N)      │  Read N-byte message body
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│  parse_request()  │  ByteReader walks binary:
│                   │    api_key → switch dispatch
│                   │    per-API: skip headers, read
│                   │    compact arrays, sub-structs
│                   │  Returns Request variant or error
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│  Broker::handle() │  std::visit dispatch:
│                   │    ApiVersions       → validate version
│                   │    DescribeTopic     → lookup + build metadata
│                   │    Produce           → lookup + write to disk
│                   │    Fetch             → lookup + read from disk
│                   │  Returns Response variant
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│  serialize()      │  std::visit dispatch:
│                   │    compute exact body size
│                   │    allocate buffer
│                   │    ByteWriter fills: fields, arrays, TAG_BUFFERs
│                   │  Returns vector<uint8_t> (4-byte len + body)
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│  send_all()       │  Loop ::send() until all bytes flushed
└──────────────────┘
```

## Key Design Decisions

1. **`std::variant` + `std::visit` over inheritance** — All 4 request types and 4 response types are modeled as `std::variant` alternatives. Dispatch uses `std::visit` with `overloaded` lambdas. No virtual functions, no dynamic allocation, no vtables. Compile-time exhaustive matching guarantees every case is handled.

2. **`std::expected<T, std::error_code>` throughout** — Every fallible function (parsing, serialization, I/O, network) returns `std::expected`. Errors propagate via `std::unexpected`. Zero exceptions are used for control flow, avoiding hidden code paths and making error handling explicit at every call site.

3. **KRaft-only metadata, no ZooKeeper** — Parses the KRaft metadata log directly from disk at startup. The parser handles Kafka record batch v2 format: iterates record batches by magic byte detection, decodes compact-varint record value frames, distinguishes topic records (type=2) from partition records (type=3), and links them by UUID into bidirectional lookup maps.

4. **Manual big-endian binary protocol** — No serialization library. `ByteReader`/`ByteWriter` provide safe sequential access with bounds checking. All multi-byte fields are explicitly converted to/from network byte order via `endian.hpp`. Varint encoding (unsigned + zigzag signed) is fully `constexpr`, allowing compile-time size precomputation.

5. **Precomputed buffer allocation** — Response serializers compute exact body size before allocation, walking all nested structures once. A single `vector<uint8_t>` is allocated at the correct size and filled in one pass. No resizing, no chained buffers.

6. **Disk-persistent storage** — Follows real Kafka's per-partition log segment naming: `{topic}-{partition}/00000000000000000000.log`. Produce appends record batch bytes directly (no transformation). Fetch returns raw log bytes (consumer-side decoding left to clients). Directories are created on first write to a partition.

7. **One thread per connection** — `std::thread::detach()` per client, each running a blocking recv→parse→handle→serialize→send loop. Simple, avoids thread pool complexity for this scale. Server remains single-threaded for `accept()`.

8. **Zero dependencies, ~1,800 LOC** — The entire broker including protocol, networking, storage, and metadata parsing fits in under 1,800 lines of C++23. No external libraries beyond the standard library and POSIX sockets. GoogleTest is fetched via CMake `FetchContent` at build time only.

9. **Compile-time safety** — `-Wall -Wextra -Wpedantic -Werror -Wshadow -Wconversion` enforced. Optional AddressSanitizer and UndefinedBehaviorSanitizer via CMake option. All integer conversions are explicit and bounds-checked.

10. **Comprehensive testing** — 3,200+ lines of tests covering every component: unit tests for broker logic, protocol parsing/serialization round-trips, metadata parser edge cases, and 9 integration tests that spawn the real broker binary, send raw TCP frames byte-by-byte, and verify wire-level Kafka responses.

## Usage

### Prerequisites

- GCC 13+ (or Clang 17+)
- CMake 3.21+
- Ninja

```bash
sudo apt install g++-13 ninja-build cmake
```

### Build

```bash
./build.sh              # Debug build (default)
./build.sh Release      # Release build with -O3

# With sanitizers:
cmake -B build -G Ninja -DENABLE_SANITIZERS=ON
cmake --build build

# With coverage:
./run_tests.sh --coverage
```

### Run

```bash
./build/kafka
```

The broker binds to port **9092** and reads cluster metadata from `/tmp/kraft-combined-logs/`.

## Testing

```bash
./run_tests.sh
```

Uses **GoogleTest v1.14.0** (auto-fetched via CMake `FetchContent`). Test structure:

| Test File | Lines | Coverage |
|-----------|-------|----------|
| `tests/broker/broker_test.cpp` | ~320 | 18 tests: version validation, known/unknown topics, fetch with/without records, produce to disk, multi-topic sorted output |
| `tests/protocol/parser_test.cpp` | ~340 | 9 tests: all 4 request types, truncated buffer, unknown API key, produce with record batches |
| `tests/protocol/serializer_test.cpp` | ~520 | 10 tests: all 4 response types, multi-topic, error codes, record payloads, empty arrays |
| `tests/cluster/metadata_test.cpp` | ~120 | 4 tests: topic+partition records, unknown types, empty input, UUID hash round-trip |
| `tests/integration_test.cpp` | ~1,160 | 9 tests: spawns real `kafka` binary via `fork+exec`, sends raw TCP frames, validates wire-level response bytes for ApiVersions, DescribeTopicPartitions (unknown, multi-topic, sorted), Fetch (empty topics, record batch from disk), Produce (persistence to disk verified post-request), concurrent clients |

### Code Coverage

Generate an HTML coverage report with a single command:

```bash
./run_tests.sh --coverage
```

This builds the project with `--coverage`, runs all 54 tests via ctest, and generates an HTML report at `build/coverage/index.html`.

**Prerequisites:** `lcov` (install with `sudo apt install lcov`).

**Manual usage:**

```bash
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON
cmake --build build
cmake --build build --target test    # run tests without lcov
lcov --capture --directory build --gcov-tool gcov-14 --output-file build/coverage.info
lcov --remove build/coverage.info '/usr/*' '*/_deps/*' --output-file build/coverage.info
genhtml build/coverage.info --output-directory build/coverage
```

> **Note:** `main.cpp`, `server.cpp`, and `socket.cpp` are exercised only through integration tests that spawn a separate `kafka` process via `fork+exec`, so their coverage data is not captured by the in-process gcov instrumentation. The reported coverage (92.8% lines, 99.7% functions) reflects all code exercised by unit tests + integration test helper logic.

## License

[MIT](LICENSE)
