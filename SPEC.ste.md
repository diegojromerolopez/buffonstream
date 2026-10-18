# buffonstream Specification

## 1. Purpose

Build a Go 1.22 or later service that stores Protobuf event records in append-only `.pbdb` files. Expose unary and streaming gRPC APIs. Deliver real-time change data capture for concurrent clients.

## 2. Required Repository Layout

Use these modular files and directories:

```text
Makefile
Dockerfile
docker-compose.e2e.yml
go.mod
go.sum
main.go
proto/v1/buffonstream.proto
pkg/proto/v1/buffonstream.pb.go
pkg/proto/v1/buffonstream_grpc.pb.go
pkg/domain/record.go
pkg/infrastructure/pb_storage.go
pkg/infrastructure/pb_storage_test.go
pkg/infrastructure/indexer.go
pkg/infrastructure/indexer_test.go
pkg/infrastructure/broadcaster.go
pkg/infrastructure/broadcaster_test.go
pkg/service/grpc_service.go
pkg/service/grpc_service_test.go
tests/integration/grpc_unary_test.go
tests/integration/grpc_streaming_test.go
tests/e2e/e2e_runner.go
```

Split additional files by responsibility. Do not create a source file with more than 500 lines. Keep `main.go` below 15 lines. Make `main.go` delegate immediately to a testable server runner.

Use constructor injection for storage, index, broadcaster, clocks, listeners, and services. Do not use global singletons.

## 3. Protobuf Contract

Use this schema in `proto/v1/buffonstream.proto`:

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
message PutRequest { EventRecord record = 1; }
message PutResponse { bool success = 1; uint64 offset = 2; }
message GetRequest { string id = 1; }
message SubscribeRequest { string topic = 1; uint64 start_timestamp = 2; }
message SyncAck { string last_processed_id = 1; uint64 total_processed = 2; }
service BuffonStreamService {
  rpc PutRecord(PutRequest) returns (PutResponse);
  rpc GetRecord(GetRequest) returns (EventRecord);
  rpc SubscribeLive(SubscribeRequest) returns (stream EventRecord);
  rpc SyncStream(stream EventRecord) returns (stream SyncAck);
}
```

Generate Go code with `protoc` through `make generate`. Generated files must match the schema. Do not edit generated files manually.

## 4. Storage Format

Store files at `data/events.pbdb` by default. Allow the server to override the path with `--data-path PATH`. Create the parent directory when it does not exist.

Write this 10-byte header once:

```text
ASCII BUFF0001, followed by uint16 version 1 in big-endian order
```

Write each record as:

```text
[varint32 payload length][exact Protobuf EventRecord bytes][CRC32C 4 bytes]
```

Store CRC32C using the Castagnoli polynomial. Encode the CRC as four big-endian bytes. Calculate the CRC over the Protobuf payload only. Reject payload lengths greater than 4 MiB. Reject truncated varints, truncated payloads, and truncated CRC values.

Define the record offset as the byte position of the first varint byte. Return that offset in `PutResponse.offset`.

Use an in-memory index from non-empty `record.id` to the record offset. Reject duplicate IDs. Recover complete valid records during startup. Stop recovery at a truncated final record and retain earlier records. Reject a bad header, unsupported version, or CRC failure in a complete record during startup.

Read record payload bytes from the file mapping or equivalent direct file region. Avoid a second storage payload copy. The gRPC boundary may perform the Protobuf operation required by the generated `EventRecord` API. Document this boundary operation. Do not claim that generated gRPC messages provide absolute end-to-end zero-copy transfer.

## 5. Server Flags and Transport

Start the server with:

```text
auto/bin/buffonstream --port 50051 --data-path data/events.pbdb
```

Use `--port` with a decimal value from 1 through 65535. Use port 50051 by default. Listen on TCP. Use gRPC over HTTP/2. Bind to `0.0.0.0` in containers and `127.0.0.1` for local tests when the test runner supplies the address.

Provide a health or readiness check for tests. A readiness check must return success only after storage recovery and gRPC registration complete.

Return process exit code 0 on clean termination. Return a non-zero exit code for invalid flags, listener failure, storage initialization failure, or unsupported file format. Write operational errors to stderr with the prefix `ERROR:`.

## 6. RPC Behavior

### 6.1 PutRecord

Accept a non-null record. Require a non-empty `id` and `topic`. Require `timestamp` greater than zero. Permit an empty payload and empty metadata values. Reject an ID longer than 256 UTF-8 bytes, a topic longer than 256 UTF-8 bytes, and a payload larger than 4 MiB.

Append the record atomically before broadcasting it. Return `success=true` and the record offset after the append succeeds. Broadcast the exact accepted record after the index update succeeds.

Return these gRPC statuses:

- `INVALID_ARGUMENT` for a missing record or invalid field.
- `ALREADY_EXISTS` for a duplicate ID.
- `RESOURCE_EXHAUSTED` for a payload over 4 MiB or a full subscriber queue.
- `INTERNAL` for write or index failures.

Never broadcast a failed write.

### 6.2 GetRecord

Require a non-empty ID. Return the indexed record when it exists. Verify the frame length and CRC before returning it. Return `NOT_FOUND` for an unknown ID. Return `DATA_LOSS` for a CRC failure or invalid frame. Return `INVALID_ARGUMENT` for an empty ID. Do not crash on corrupt bytes.

### 6.3 SubscribeLive

Accept an empty topic as a wildcard. When `topic` is non-empty, send only matching records. Send records with `timestamp >= start_timestamp` when `start_timestamp` is non-zero. Send records in successful append order. Register the subscriber before it waits for events.

Use a bounded queue of 1024 records for each subscriber. Disconnect a subscriber with `RESOURCE_EXHAUSTED` when its queue cannot accept an event. End normally when the client cancels the context. Do not block all publishers on one slow subscriber.

A new subscriber receives live records only. The service does not replay historical records unless a future version adds an explicit replay field.

### 6.4 SyncStream

Accept a continuous client stream of `EventRecord` messages. Apply the same validation and duplicate rules as `PutRecord`. Process each valid message in receive order for that stream. Send an acknowledgment after each successful write:

```text
SyncAck.last_processed_id = accepted record ID
SyncAck.total_processed = count of successful records on this stream
```

Return `INVALID_ARGUMENT`, `ALREADY_EXISTS`, `RESOURCE_EXHAUSTED`, or `INTERNAL` with the same meanings as `PutRecord`. Preserve acknowledgments already sent before a later stream error. End with status `OK` after the client half-closes and all accepted writes finish. Support simultaneous send and receive operations on one HTTP/2 stream.

## 7. Concurrency and Ordering

Support concurrent `PutRecord`, `SyncStream`, and `SubscribeLive` clients. Protect the file, index, and subscriber registry with appropriate synchronization. Define global append order as the order in which the storage append lock commits records. Publish records in that same order.

Do not promise an order between separate SyncStream clients beyond global append order. Preserve per-stream receive order.

## 8. Required Commands

Implement these Makefile recipes:

```text
make generate
make build
make run
make test
make lint
make e2e
```

`make build` must create `bin/buffonstream`. `make run` must run it on port 50051. `make test` must run `go test -v ./...` and fail when any test fails or when no tests execute. `make lint` must run `go vet ./...` and the configured `golangci-lint` command. `make e2e` must run:

```text
docker compose -f docker-compose.e2e.yml up --build --exit-code-from e2e-runner
```

Never use the hyphenated `docker-compose` command.

## 9. Docker E2E Harness

Define services named `buffonstream`, `writer-1`, `writer-2`, `writer-3`, `consumer-1`, `consumer-2`, `consumer-3`, and `e2e-runner` in `docker-compose.e2e.yml`.

Run the test for exactly 300 seconds after readiness. Writers must use both `PutRecord` and `SyncStream`. Consumers must keep `SubscribeLive` streams open. Generate globally unique IDs. Record every published ID and every received ID.

The runner must verify 100 percent receipt for each consumer, global sequence validity, topic filtering, CRC validity, zero duplicate delivery per consumer, no deadlock, clean stream completion, and stable resource usage. Poll readiness instead of using fixed sleep delays. Set a maximum two-second timeout on every network connection, request, send, and receive operation. Fail on a crash, timeout, packet loss, checksum error, descriptor exhaustion, or unbounded memory growth.

## 10. Testing Rules

Use Chicago-school tests. Assert observable state transitions on real domain objects. Inject in-memory doubles for external boundaries during unit and intermediate integration tests. Do not require live Docker, external databases, or live background brokers outside the final E2E story.

Cover empty values, invalid IDs, invalid topics, zero timestamps, maximum valid payloads, oversized payloads, duplicate IDs, missing IDs, corrupt CRCs, truncated frames, bad headers, unsupported versions, concurrent writers, slow subscribers, cancelled streams, half-closed SyncStream clients, and server shutdown.

Use deterministic clocks for timestamp-related tests. Do not use static sleep delays. Do not use tautological tests, stubs, placeholders, `NotImplementedError`, empty function bodies, or shell error masking.

## 11. Quality Constraints

Keep each Go file below 500 lines. Keep dependency direction acyclic. Leaf modules must not import parent facades or coordinator entrypoints. Use Go 1.22 or later. Use only approved Go and gRPC tooling declared in `go.mod` and the Docker build. Do not add an unapproved framework or global service registry.

Use an automated script for structured files, generated code, Compose configuration, and test fixtures when scripting improves reliability over manual editing.

## 12. Acceptance Standard

All unit, integration, streaming, and black-box tests must pass. The five-minute E2E run must pass with zero crashes, deadlocks, data loss, checksum errors, or resource exhaustion. `go vet ./...` and the configured linter must report no findings during final hardening. Documentation must describe commands, flags, file format, RPC errors, and E2E execution.
