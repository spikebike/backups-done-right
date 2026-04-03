# Client Upload Pipeline

The client uses a multi-stage, channel-based pipeline to back up files. Each stage runs as one or more concurrent goroutines connected by Go channels, providing natural backpressure and bounded memory usage.

## Pipeline Stages

```text
┌────────────┐    jobChan       ┌────────────┐   uploadChan   ┌───────────────┐  UploadPipelineChan ┌─────────────┐
│ Scanners   ├─────────────────►│ CryptoPool ├───────────────►│ Offer Pipeline├────────────────────►│ Upload Pump │
│ (4 threads)│  FileJob(1000)   │ (4 threads)│ UploadJob(100) │ (4 threads)   │   pendingUpload     │ (4 threads) │
└────────────┘                  └─────┬──────┘                └───────┬───────┘                     └──────┬──────┘
                                      │                               │   (releases unneeded)              │
                                ┌─────▼──────┐                  ┌─────▼──────┐                       ┌─────▼──────┐
                                │ cipherBufs │◄─────────────────│ Release()  │◄──────────────────────│ Release()  │
                                │ (free-list)│                  └────────────┘    (releases uploaded)└────────────┘
                                └────────────┘                                                                      
```

### 1. Scanner Pool (`scan_threads`, default 4)

Scanner workers walk the configured backup directories and compare each file's modification time (`mtime`) against the local SQLite database. Files that are new or changed are sent as `FileJob` structs into `jobChan` (buffered channel, capacity 1000). Deleted files are marked in the database.

### 2. Crypto Pool (`crypto_threads`, default 4)

Crypto workers consume `FileJob` entries from `jobChan`. For each file:

1. **Read** the file in 4MB chunks using a pooled read buffer.
2. **Compress** each chunk with zstd.
3. **Check session deduplication** — an ultra-fast in-memory `sync.Map` checks if this exact plain hash has already been processed or is currently being processed by another worker in this session, avoiding duplicate crypto work.
4. **Grab a cipher buffer** from the pre-allocated free-list (`<-cipherBufs`). This blocks if all buffers are in use, providing natural backpressure.
5. **Encrypt** the compressed chunk into the buffer using XChaCha20-Poly1305 with Convergent Encryption (the BLAKE3 hash of the plaintext determines the nonce).
6. **Send an `UploadJob`** into `uploadChan`, carrying the encrypted data and a `Release()` callback that returns the buffer to the free-list.
7. **Record the chunk** in the local SQLite `file_blobs` table to track the file's reconstruction recipe.

### 3. Offer Pipeline (`upload_threads`, default 2)

Offer Pipeline workers consume `UploadJob` entries from `uploadChan` and accumulate them into batches of `batch_upload_size`. For each full batch:

1. **Offer** — Send the batch's hashes and sizes to the server via the `OfferBlobs` Cap'n Proto RPC. The server executes a highly optimized bulk SQLite check and responds with indices of chunks it doesn't already have.
2. **Filter & Release** — Instantly release buffers for any unneeded chunks back to the free-list, preventing them from consuming memory pipeline slots.
3. **Dispatch** — Push the exactly needed chunks into the `UploadPipelineChan` for the next stage.

### 4. Upload Data Pump (`upload_threads`, default 2)

Upload Pump workers consume the filtered batches from `UploadPipelineChan`.

1. **Stream** — Dynamically establish a raw libp2p QUIC stream (`/bdr/upload/1.0.0`) to the server, bypassing Cap'n Proto memory allocation overhead entirely. It precisely blasts only the required ciphertext bytes array sequentially over the network.
2. **Release** — Call `Release()` on the uploaded jobs, returning the memory to the pre-allocated free-list.

Because the Offer Phase and Upload Phase are handled by asynchronous goroutines, the client flawlessly hides network TTFB (time-to-first-byte) latency. It can saturate high-bandwidth links by continuously pipelining chunk bundles without blocking on a single rigid sequence.

## Memory Management

### Pre-Allocated Cipher Buffer Pool

The pipeline pre-allocates a fixed set of ~4MB cipher buffers at startup and reuses them for the entire backup. This eliminates garbage collection pressure from repeated large allocations.

```
Pool size = upload channel capacity + number of crypto workers
         = 100 + 4 = 104 buffers (default)
         ≈ 416 MB reserved at startup
```

The buffers are stored in a **buffered channel** (`chan []byte`), not a `sync.Pool`. This is a deliberate choice: `sync.Pool` entries are cleared on every GC cycle (~2 minutes), forcing re-allocation. A buffered channel acts as a fixed free-list that the GC never touches — buffers are allocated once and recycled indefinitely.

**Buffer lifecycle:**
```
free-list ──► crypto worker ──► uploadChan ──► uploader batch ──► RPC ──► free-list
   ▲                                                                         │
   └─────────────────────────────────────────────────────────────────────────┘
```

Buffers are returned to the free-list regardless of outcome — whether the server accepted the chunk, already had it, or the RPC failed.

### Backpressure

The pipeline self-regulates through channel blocking:

- If the **uploader is slow** (network bottleneck), `uploadChan` fills up → crypto workers block trying to send → scanner results queue up in `jobChan`.
- If **all cipher buffers are in use**, crypto workers block on `<-cipherBufs`, preventing memory from growing beyond the pre-allocated pool.
- If the **scanner is slow** (disk I/O), crypto workers block on `jobChan` → uploaders drain remaining work.

### Memory Budget

Total pipeline memory is bounded by `max_pipeline_mem_mb` (default 400 MB):

```
upload channel capacity = max_pipeline_mem_mb / block_size_bytes
                        = 400 MB / 4 MB = 100 slots
```

Each slot holds one encrypted chunk. The actual memory footprint is slightly higher due to the extra `numWorkers` buffers and compression intermediates.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pipeline.scan_threads` | 4 | Concurrent directory scanner workers |
| `pipeline.crypto_threads` | 4 | Concurrent encryption workers |
| `pipeline.upload_threads` | 2 | Concurrent uploader workers |
| `pipeline.batch_upload_size` | 10 | Chunks per OfferBlobs/UploadBlobs RPC call |
| `pipeline.max_pipeline_mem_mb` | 400 | Memory budget for the upload queue |
| `crypto.block_size_bytes` | 4194304 | Chunk size (4 MB) |
