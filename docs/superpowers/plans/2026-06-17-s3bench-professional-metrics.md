# s3bench Professional Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the single-binary S3 benchmark with run isolation, richer read workloads, warmup, cleanup, error classification, and machine-readable results.

**Architecture:** Use AWS SDK v2 for S3 protocol operations, while keeping the benchmark executable small and focused. Add config fields, per-operation statistics, and focused helpers that can be unit-tested without a live S3 cluster.

**Tech Stack:** Go 1.24+, AWS SDK v2, standard-library metrics/reporting code.

---

### Task 1: Config And Workload Parsing

**Files:**
- Modify: `/Users/quzhihao/Downloads/s3bench/main.go`
- Create: `/Users/quzhihao/Downloads/s3bench/main_test.go`

- [ ] Add tests for default run ID behavior, read operation parsing, object size mix parsing, and duration defaults.
- [ ] Implement `RUN_ID`, `READ_OPS`, `RANGE_SIZE`, `WARMUP_DURATION`, `CLEAN_AFTER`, `RESULT_FILE`, and `RETRY` parsing.
- [ ] Run `go test ./...`.

### Task 2: S3 Operations

**Files:**
- Modify: `/Users/quzhihao/Downloads/s3bench/main.go`

- [ ] Add `HEAD`, `RANGE GET`, `LIST`, and `DELETE` request methods through AWS SDK v2.
- [ ] Add status-aware errors so failures are grouped by status and operation.
- [ ] Run `go test ./...`.

### Task 3: Metrics And Output

**Files:**
- Modify: `/Users/quzhihao/Downloads/s3bench/main.go`
- Modify: `/Users/quzhihao/Downloads/s3bench/README.md`
- Modify: `/Users/quzhihao/Downloads/s3bench/.env.example`

- [ ] Track stats per operation while keeping current summary output readable.
- [ ] Add warmup execution that does not enter final stats.
- [ ] Add JSON result output and optional cleanup.
- [ ] Run `gofmt -w main.go main_test.go` and `go test ./...`.
