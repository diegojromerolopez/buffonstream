# `buffonstream` Technical Specification & Architecture

`buffonstream` is an end-to-end Protobuf-native binary storage engine and real-time bi-directional gRPC streaming server written in Go 1.22+. It stores structured data on disk directly in length-prefixed Protobuf binary wire format (`.pbdb`) with sparse index lookups and CRC32 checksums, delivering zero-copy read performance and real-time Change Data Capture (CDC) streaming to concurrent subscriber clients over gRPC / HTTP/2.

---

## 1. Core Technical Invariants & Architectural Boundaries

> [!IMPORTANT]
> **GOAL 1: PROTOBUF ON-DISK STORAGE ENGINE (.pbdb)**
> - **Varint Length-Prefixed Binary Records**: On-disk files (`data/events.pbdb`) store contiguous Protobuf payloads framed as `[Varint32(Length)][ProtobufBytes][CRC32(4B)]`.
> - **Zero-Copy Serialization**: Query reads fetch raw Protobuf binary bytes directly from the memory-mapped `.pbdb` file and transmit them across the gRPC network wire without deserializing into intermediate heap structs.
> - **Binary Sparse Index**: Maintains an in-memory B-Tree or Hash Index mapping `record_id` $\rightarrow$ `file_byte_offset`.

> [!IMPORTANT]
> **GOAL 2: BI-DIRECTIONAL REAL-TIME gRPC STREAMING**
> - **Live Change Data Capture (CDC)**: Clients subscribing to `SubscribeLive` MUST receive real-time Protobuf event payloads instantly when another client executes a `PutRecord` write.
> - **Bi-Directional Sync**: `SyncStream` permits clients to stream a continuous batch of writes while simultaneously receiving stream acknowledgments (`SyncAck`) over a single persistent HTTP/2 TCP connection.

> [!CAUTION]
> **GOAL 3: ZERO DEPENDENCY INJECTION VIOLATIONS & 500-LINE LIMIT**
> - No single `.go` source file may exceed **500 lines of code**.
> - Storage engine, indexer, broadcaster, and gRPC handlers MUST be supplied via constructors. Global singletons are strictly forbidden.

---

## 2. Directory Layout & Protobuf Architecture

```
buffonstream/
├── Makefile                      # generate, build, test, lint, e2e targets
├── Dockerfile                    # Multi-stage Docker build with protoc & Go toolchain
├── docker-compose.e2e.yml        # E2E black-box test harness (buffonstream + streaming client runner)
├── proto/
│   └── v1/
│       └── buffonstream.proto    # gRPC service & message definitions
├── pkg/
│   ├── domain/
│   │   └── record.go             # Storage record & Index interfaces
│   ├── infrastructure/
│   │   ├── pb_storage.go         # Varint length-prefixed binary disk engine (.pbdb)
│   │   ├── indexer.go            # Sparse in-memory indexer (record_id -> offset)
│   │   └── broadcaster.go        # Thread-safe pub-sub event fan-out broadcaster
│   └── service/
│       └── grpc_service.go       # gRPC server implementation for Unary & Streaming RPCs
├── main.go                       # Entry point, flag parsing, gRPC listener initialization
└── tests/
    ├── unit/                     # Varint framing, CRC32 verification, index tests
    ├── integration/              # Real gRPC client/server unary & streaming integration tests
    └── e2e/                      # Docker E2E black-box CDC stream assertion runner
```

---

## 3. Protocol Buffer Schema (`proto/v1/buffonstream.proto`)

```protobuf
syntax = "proto3";

package buffonstream.v1;

option go_package = "pkg/proto/v1;buffonstreamv1";

message EventRecord {
  string id = 1;
  uint64 timestamp = 2;
  string topic = 3;
  bytes payload = 4;
  map<string, string> metadata = 5;
}

message PutRequest {
  EventRecord record = 1;
}

message PutResponse {
  bool success = 1;
  uint64 offset = 2;
}

message GetRequest {
  string id = 1;
}

message SubscribeRequest {
  string topic = 1;
  uint64 start_timestamp = 2;
}

message SyncAck {
  string last_processed_id = 1;
  uint64 total_processed = 2;
}

service BuffonStreamService {
  // Unary KV Operations
  rpc PutRecord (PutRequest) returns (PutResponse);
  rpc GetRecord (GetRequest) returns (EventRecord);

  // Server-Side Streaming (Real-Time CDC Subscriber)
  rpc SubscribeLive (SubscribeRequest) returns (stream EventRecord);

  // Bi-Directional Streaming (High-Throughput Ingestion & Ack)
  rpc SyncStream (stream EventRecord) returns (stream SyncAck);
}
```

---

## 4. Binary Storage Disk Format (`.pbdb`)

```
+--------------------------------------------------------------------------+
| Header: "BUFF0001" (8 Bytes) | Version (2 Bytes)                         |
+--------------------------------------------------------------------------+
| Record 1: [Varint32(Len=42)] [Protobuf EventRecord Bytes] [CRC32(4B)]    |
+--------------------------------------------------------------------------+
| Record 2: [Varint32(Len=128)] [Protobuf EventRecord Bytes] [CRC32(4B)]   |
+--------------------------------------------------------------------------+
| ... Append-Only Binary Stream ...                                        |
+--------------------------------------------------------------------------+
```

---

## 5. End-to-End Black-Box Verification (`docker-compose.e2e.yml`)

The project MUST include a `docker-compose.e2e.yml` configuration executing a **5-Minute Multi-Producer / Multi-Consumer Real-Time Streaming Stress Test**:

1. **Architecture**:
   - **Server**: `buffonstream` gRPC server listening on port `50051`.
   - **Multiple Writing Clients**: At least 3 concurrent writer containers (`writer-1`, `writer-2`, `writer-3`) publishing a continuous stream of Protobuf `EventRecord` messages via `PutRecord` and `SyncStream`.
   - **Multiple Consumer Clients**: At least 3 concurrent consumer containers (`consumer-1`, `consumer-2`, `consumer-3`) maintaining long-lived `SubscribeLive` gRPC streams receiving real-time CDC updates.
   - **E2E Test Orchestrator**: An `e2e-runner` container that controls test duration (300 seconds / 5 minutes), monitors throughput, and asserts zero packet drops, zero consumer deadlocks, and clean server state.
2. **5-Minute Test Execution Invariants**:
   - The harness runs for **300 seconds** under non-stop concurrent read/write traffic.
   - Consumers MUST verify that all published records arrive in sequence over `SubscribeLive` without data loss or checksum errors.
   - Server CPU/memory usage MUST remain stable without memory leaks or descriptor exhaustion.

---

## 6. Local Testing & Verification Engine

### 6.1 Makefile Targets (REQUIRED)
- `make generate` → Runs `protoc` to generate Go code from `.proto`.
- `make build` → Compiles binary into `bin/buffonstream`.
- `make run` → Runs `bin/buffonstream --port 50051`.
- `make test` → Runs unit & integration tests (`go test -v ./...`).
- `make lint` → Enforces `go vet ./...` and `golangci-lint`.
- `make e2e` → Executes 5-minute multi-client test harness (`docker compose -f docker-compose.e2e.yml up --build --exit-code-from e2e-runner`).

### 6.2 Definition of Done (DoD) Criteria
1. **100% Pass Rate** on all unit, integration, and streaming tests.
2. **5-Minute E2E Multi-Client Stability**: `docker compose -f docker-compose.e2e.yml` executes continuously for 5 minutes with zero crashes, zero deadlock, and 100% subscriber receipt.
3. **Zero-Copy Protobuf Streaming**: Live subscription streams push raw Protobuf payloads directly off disk.
4. **CRC32 Validation**: Corrupted disk bytes trigger explicit read error without crashing server.
5. **Zero Linter Findings** on `go vet ./...`.
