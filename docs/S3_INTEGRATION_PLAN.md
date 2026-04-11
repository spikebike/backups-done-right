# Integration Plan: S3-Compatible Backend (minio-go)

## Objective
Enable the BDR server to use an S3-compatible backend (Amazon S3, Backblaze B2, Minio) for storing 256MB/512MB shards, instead of the local filesystem. This reduces the local storage requirement for VPS deployments while maintaining high durability and performance.

## Key Constraints & Considerations
1. **S3 Immutability:** S3 objects cannot be appended to. Currently, the server incrementally appends to `open` shards (up to 512MB) over time as clients upload 4MB chunks. 
   - **Solution:** `open` shards must be staged locally (e.g., in a `staging_dir` or `spool_dir`). Once they reach the threshold and are `sealed`, the completed 512MB file is uploaded to S3 and then removed from local staging.
2. **Streaming Peer Shards:** When receiving a shard from a peer (e.g., `peer_HASH`), the exact size is known upfront via the Cap'n Proto header. 
   - **Solution:** We can stream these directly into S3 using `minio.PutObject` without needing to touch the local disk.
3. **Erasure Coding & Outbound:** The `QueueDir` (for 256MB erasure-coded pieces ready to be sent to peers) remains entirely local, as these pieces are ephemeral and meant to be distributed quickly. `encodeShard` can stream the full 512MB shard directly *from* S3 into the local queue pieces.

## Proposed Implementation Steps

### Phase 1: Configuration & Initialization
1. Add a new `s3` block under `storage` in `server.yaml` and `internal/config/config.go`:
   ```yaml
   storage:
     s3:
       enabled: true
       endpoint: "s3.us-west-004.backblazeb2.com"
       access_key: "my-access-key"
       secret_key: "my-secret-key"
       bucket: "bdr-shards"
       use_ssl: true
   ```
2. Initialize `minio.Client` in `server/main.go` if `s3.enabled == true`.

### Phase 2: The `BlobStore` Abstraction
Create a new interface `internal/server/blobstore.go` to cleanly decouple storage logic from the engine:
```go
type BlobStore interface {
    Get(ctx context.Context, key string) (io.ReadCloser, error)
    Put(ctx context.Context, key string, reader io.Reader, size int64) error
    Stat(ctx context.Context, key string) (int64, error)
    Delete(ctx context.Context, key string) error
}
```
Implement two structs: `LocalBlobStore` (wraps `os` operations on `BlobStoreDir`) and `S3BlobStore` (wraps `minio-go`).

### Phase 3: Engine Refactoring
1. **Engine Struct:** Replace the hardcoded `BlobStoreDir` references in `Engine` with the new `BlobStore` interface.
2. **Ingestion (Client Uploads):** Update `IngestItemsStreamed` to write active `open` shards to `e.SpoolDir` (local). When sealing, trigger an asynchronous upload to `e.BlobStore.Put()` before calling `encodeShard`.
3. **Erasure Coding:** Modify `encodeShard` to use `e.BlobStore.Get("shard_X.dat")` to stream the source data into the Reed-Solomon encoder.
4. **Peer Transfers:** Update `PushPieceBatched` to read from the interface, and `PullPieceRaw` / `DownloadItems` to use `e.BlobStore.Put` directly if size is known.
5. **Garbage Collection & Repair:** Update `gc_worker.go` and `repair_worker.go` to interact with the interface for `Stat` and `Delete`.

## Alternatives Considered
- **FUSE / S3FS:** Mounting an S3 bucket to the local OS and treating it as a local filesystem. 
  *Reason Rejected:* Often unreliable under heavy concurrent I/O, difficult to configure across different OS environments, and introduces extreme latency during local appends to `open` shards. Native `minio-go` integration is far more robust and memory-efficient.

## Verification & Testing
- Deploy a local `minio` docker container for integration testing.
- Verify that `open` shards correctly stage locally and transition to S3 seamlessly upon sealing.
- Ensure peer transfers (upload and download) stream data cleanly to/from S3 without leaking memory.