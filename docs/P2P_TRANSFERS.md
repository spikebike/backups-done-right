# P2P Transfer Pipeline

The server uses a highly optimized, dual-plane architecture for distributing and retrieving erasure-coded pieces (shards) across the decentralized P2P swarm. This design cleanly separates control messages from bulk data transfer, ensuring maximum throughput with zero garbage collection spikes.

## Architecture Overview

```text
┌────────────────┐      (Cap'n Proto RPC)       ┌───────────────┐
│ OutboundWorker ├─────────────────────────────►│ Peer Receiver │
│  (Queue Scan)  │  OfferItems / PrepareUpload  │ (Quota Check) │
└───────┬────────┘                              └───────┬───────┘
        │                                               │
        │               (Raw libp2p QUIC)               │
        └───────────────────────────────────────────────┘
                     /bdr/stream/1.0.0 (Data Plane)
```

### 1. The Control Plane (Cap'n Proto)

To keep multiplexed connections highly responsive, all negotiation happens "out of band" using lightweight Cap'n Proto RPCs. 

1. **Discovery & Batching**: The background `OutboundWorker` periodically scans the `server_queue` directory for newly generated erasure-coded pieces. It groups them by target peer, evaluating quotas, untrusted limits, and ensuring redundancy rules are met.
2. **Offer Phase (`offerItems`)**: The sender queries the target peer, asking which of the offered piece checksums it actually needs. The receiver queries its local SQLite database and returns the needed indices.
3. **Prepare Phase (`prepareUpload`)**: The sender tells the receiver to expect the exact metadata (checksums, sequence numbers, parent shard hashes) for the pieces it is about to send, allowing the receiver to validate and allocate temporary expectations.

### 2. The Data Plane (Raw Stream)

Once the Control Plane negotiation is complete, the actual bulk transfer shifts to a dedicated raw byte stream over libp2p (`/bdr/stream/1.0.0`). This completely bypasses the Cap'n Proto deserializer.

1. **Protocol Framing**: The sender initiates a stream and writes a simple binary header structure:
   - **Batch Header** (8 bytes): Contains the `Count` of items in the batch (4 bytes) and `Reserved` padding (4 bytes).
   - **Item Header** (42 bytes per item): Contains `OpCode` (1 byte), BLAKE3 `Hash` (32 bytes), `Size` (8 bytes), and `Flags` (1 byte).
2. **Zero-Copy Streaming**: The sender uses `io.CopyBuffer` to pipe the piece natively from disk to the network socket in buffered chunks using a pre-allocated pool (`StreamBufferPool`). 
3. **On-the-fly Hashing**: The receiver uses an `io.TeeReader` to generate the BLAKE3 hash dynamically as the stream arrives. Data is piped directly into a temporary file (`tmp_<hash>`).
4. **Validation**: If the final hash matches the expected checksum from the header, the temporary file is atomically renamed into the permanent `server_blobs` store. The receiver never allocates the full 256MB file in RAM.
5. **Acknowledgment**: The stream completes by returning a single status byte (e.g., `0` for success, `2` for checksum mismatch, `3` for quota exceeded).

## Unified Push/Pull Mechanism

The `/bdr/stream/1.0.0` protocol handles both uploading (Push) and downloading (Pull):

- **Push (`OpCodePush`, 0x01)**: Sender provides the data payload immediately following the header. Used for standard outbound piece distribution.
- **Pull (`OpCodePull`, 0x02)**: Sender provides an empty payload in the request. The receiver locates the requested piece locally, and replies by acting as the sender of a Push batch on the exact same stream, sending the data back. Used by the `RepairWorker` and `GCWorker` to recover missing pieces.

## Resource Management

### Bandwidth Throttling

To ensure the server doesn't monopolize host bandwidth, the raw data streams are explicitly throttled.
- Every established data stream is wrapped in a `ThrottledReadWriteCloser`.
- Governed by the global `max_upload_kbps` and `max_download_kbps` server configuration limits.
- Uses a smooth token-bucket rate limiter (`golang.org/x/time/rate`).

### Memory Safety

Similar to the client pipeline, the Data Plane uses a synchronized `StreamBufferPool` (via `sync.Pool`).
- Buffers are fixed byte arrays.
- Network operations read/write strictly through these intermediate buffers.
- Large operations (like a 256MB piece transfer) remain perfectly flat in memory usage regardless of transfer speed or shard size, completely eliminating GC thrashing.

### Proof of Storage Generation

After a successful Push stream completes, the sender securely samples the uploaded piece. It extracts multiple 32-byte randomized snippets (default 8) from the local file and records them in the local `piece_challenges` table alongside their exact byte offset. The local queue file is then deleted. The background `ChallengeWorker` will later use these stored fingerprints to randomly verify that the peer is still honestly storing the piece.
