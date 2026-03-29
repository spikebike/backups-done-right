# P2P Backup System Design Document

## Vision
A decentralized, peer-to-peer backup system designed for high-performance storage exchange between trusted and untrusted nodes.

## Architecture Overview
The system is split into two distinct programs to enforce a strict zero-knowledge security boundary.

### 1. The Client (Trusted)
Runs on the user's machine, crawls directories, and handles all plaintext data.

#### Concurrency Pipeline (Go Channels)
The client uses a highly parallel, configurable pipeline to maximize hardware utilization:
1. **Initialization**: Reads config (threads, directories, keys). Queues target directories into the `dbJobChan` and `jobChan`.
2. **Scanner Pool (`scan_threads`)**: Workers scan for new/modified files (via timestamps) and enqueue file paths to the `jobChan`.
3. **Crypto Pool (`crypto_threads`)**: Workers dequeue files. For each file:
   - Reads file in exactly 4MB chunks.
   - Calculates plaintext BLAKE3 hash.
   - Encrypts chunk (XChaCha20-Poly1305) using the first 24 bytes of the plaintext hash as the nonce (Convergent Encryption).
   - Calculates encrypted BLAKE3 hash.
   - Writes to a `spool_dir`.
   - On completion, atomically moves the file to `upload_dir` named by its `hash_encrypted`.
   - Records the chunk sequence in the local SQLite database.
   - Enqueues `UploadJob` to `uploadChan`.
4. **Upload Thread**: Consumes `uploadChan`, batches chunks, and offers them to the Server. Uploads missing chunks using Cap'n Proto over mTLS.

#### Local State (SQLite)
Normalized schema acts as the File System Manifest. Client-side deduplication has been deliberately removed to reduce RAM usage and allow pure streaming.
- **`directories` table**: `id` (PK), `full_path`, `first_seen`, `last_seen`.
- **`files` table**: `id` (PK), `dir_id` (FK), `filename`, `first_seen`, `last_seen`, `mtime`, `size`, `hash_plain`, `hash_encrypted`.
- **`file_blobs` table**: Maps `file_id` directly to `hash_encrypted`, `size`, and `sequence`. This is the recipe to rebuild a file during restore.

#### Client CLI Commands
- `backup <dir1> <dir2> ...` — Run a backup of the specified directories.
- `restore [-b backup_id] <file/dir> [destination_dir]` — Restore files from the newest (or specified) backup, using relative path mapping for clean destination placement.
- `status` — Display server status, replication metrics, and uptime.
- `addpeer <ip:port>` — Instruct the server to dial and register a new peer.
- `updatepeer <id> <trusted|untrusted|blocked>` — Change a peer's trust status.
- `listpeers` — Display all known peers with their status, storage usage, and last-seen timestamp.

#### Restoration
The client features a flexible dynamic restorer that manages file retrieval:
- **Batched Downloading**: Downloads required encrypted chunks from the server in batches (max 10 chunks at a time, or 40MB) to seamlessly respect remote Cap'n Proto memory allocation limits. 
- **Zero-Copy Memory Safety**: Explicitly manages underlying Cap'n Proto arenas by copying internal data bytes instantly, preventing deferred garbage collector corruptions during the slower decryption routines.
- **Relative Path Mapping**: Intelligently strips the origin target directory root from the queried absolute database values using relative maps. This ensures files are placed inside explicitly configured `destination_dir` pathways cleanly, rather than endlessly concatenating deep absolute strings inside the folder.

### 2. The Server (Zero-Knowledge P2P Node)
Runs locally or remotely. Never sees plaintext data or client encryption keys.
- **Data Integrity Validation**: Evaluates the BLAKE3 cryptographic hash of every incoming encrypted chunk payload actively against the client's claimed checksum reference during the initial RPC ingestion flow, immediately warning of mismatches before disk commit.
- **Deduplication**: Tracks which encrypted 4MB chunk hashes already exist. Only stores one copy of identical encrypted chunks, even across multiple clients.
- **Infinite Stream Packing**: Packs incoming 4MB chunks sequentially into massive shard files. Tracks exact offsets in `blob_locations`.
- **Garbage Collection (GC)**: A background `GCWorker` periodically scans for "wasted" shards.
  - **Waste Threshold**: Shards are eligible for GC if the ratio of deleted bytes (from blobs with `ref_count=0` and expired `deleted_at`) exceeds `waste_threshold`.
  - **Repacking**: Live blobs are extracted from wasted shards and re-ingested into the current open shard using the primary ingestion pipeline.
  - **Zero-Copy Swapping**: Old shard pieces are only released from peers after the new shard is fully distributed.
  - **Pure Swarm Recovery**: If `keep_local_copy` is false, the GC worker automatically downloads enough pieces from peers to reconstruct the shard for repacking.
- **Erasure Coding**: Uses `klauspost/reedsolomon` (Streaming API). When a shard seals, it mathematically slices it into N data + K parity pieces (e.g., 10+4 or 2+1) in a background thread.
- **Peer Encryption**: Derives a unique encryption key for each peer dynamically using HKDF (HMAC-based Key Derivation Function) combining the Server's Master Key and the Peer's Public Key.
- **Swarm Distribution**: An Outbound Worker actively looks for peers under their storage quota who don't already have a piece for the given shard, and pushes the pieces to them over mTLS Cap'n Proto streams.
- **Storage Strategy**: Can act as a "Hybrid NAS" (`keep_local_copy: true`) retaining the shard for fast local restores, or a "Pure Swarm Node" (`keep_local_copy: false`) which deletes the local shard once fully replicated to the P2P network.

#### Server SQLite Schema
- **`blobs`**: Tracks every encrypted 4MB chunk by hash, size, ref_count, and special flag. Includes `deleted_at` timestamp.
- **`shards`**: Tracks packed shard files (id, filename, current_size, status, hash).
- **`blob_locations`**: Maps blob hashes to their offset/size within a shard.
- **`peers`**: Dynamic peer registry keyed by public key, with `status` (untrusted/trusted/blocked), `last_seen`, `max_storage_size`, `current_storage_size`, and `outbound_storage_size`.
- **`outbound_pieces`**: Tracks which erasure piece was sent to which peer.
- **`hosted_shards`**: Tracks shard pieces received from other peers.
- **`piece_challenges`**: Stores random 32-byte fingerprints sampled from piece data at upload time for later Proof of Storage verification.
- **`challenge_results`**: Logs the outcome of each challenge (pass/fail/unavailable) with timestamps for reputation and uptime tracking.

## Peer Management

### Mnemonic-Based Identity (BIP-39)
The system uses 24-word recovery mnemonics to deterministically derive all cryptographic identities.
- **Deterministic TLS**: The mnemonic generates a consistent Ed25519 private key. This ensures a node always has the same Public Key (Identity), even after a total data loss and reinstall.
- **Deterministic Encryption**: The same mnemonic derives the Master Encryption Key used for local database snapshots and client data.
- **Self-Healing Identity**: A user only needs their 24 words to "re-incarnate" their server or client.

### Dynamic Discovery
Peers and Clients are managed dynamically via the server's SQLite database. 
- **Client Authorization**: When a client connects, the server extracts its public key. If the key is not in the `clients` table, it is registered as `pending` with zero quota. An Admin must explicitly `updateclient` to grant access.
- **Peer Discovery**: When a new peer connects, it is registered as `untrusted`. Reputation and storage swap limits are tracked per public key.

### Connection Model
- **Strict mTLS**: Every connection (Client-Server and Peer-Peer) is protected by mutual TLS using Ed25519 certificates derived from mnemonics.
- **Server Pinning**: Clients strictly enforce server identity by pinning the server's Public Key Hex (`expected_server_key`) in their configuration. If the server provides a certificate that doesn't match the pin, the client immediately disconnects.
- **Zero-Knowledge Quotas**: Identity is verified by the TLS handshake before any RPC calls are processed.

### Bidirectional NAT Traversal (Capability Passing)
Servers maintain persistent connection pools (`ActivePeers`). When Peer A dials Peer B, A calls the `Announce` RPC and passes a live Cap'n Proto capability (`callback :PeerNode`) pointing to its own local engine. Peer B saves this capability. Later, when B needs to send data to A, it uses this capability to push data *backwards* through the exact same TCP connection A originally opened, completely bypassing NATs and firewalls without port forwarding.

### Uniform Trade Units (256MB Padding)
To ensure a fair and consistent "space-for-space" trade economy, ALL P2P data exchanges are normalized to exactly 256MB pieces.
- **Data Shards**: Standard files are ingested until the active shard hits the configured threshold, then erasure-coded into N pieces of exactly 256MB.
- **Recovery Shards**: Special metadata backups (like the server's own SQLite database) are padded with cryptographic zeros to exactly 256MB before being mirrored to the swarm, obscuring the size of the metadata.

### Trust & Quota Enforcement
- **Client Quotas**: Trusted clients are assigned a `max_storage_size` in the `clients` table.
- **Peer Quotas**: Untrusted peers are limited by `untrusted_peer_upload_limit_mb` (default 1024 MB).
- **Admin Access**: A specific `admin_public_key` in the server configuration grants a client the right to manage other clients and peers.

## Disaster Recovery

### Passive Server Rescue
If a server loses its local state, it can be recovered using only its 24-word mnemonic:
1. **Re-incarnation**: User enters the mnemonic on a fresh install. The server derives its identity and starts its TLS listener.
2. **Passive Discovery**: The server waits for its peers to initiate their hourly heartbeat calls.
3. **Reconstruction**: When a peer calls, the server recognizes the connection and calls `listSpecialPieces()`. It downloads the "Special" pieces, extracts the peer map, dials the rest of the swarm in parallel, and reconstructs its compressed database.

## Proof of Storage (Reputation & Health)

A multi-signal reputation system ensures peers are reliable and honest.

### Real-World Health Signals
While hourly background challenges provide a baseline, the system prioritizes real-world interactions as health signals:
1. **Successful Uploads**: Counts as an "Uptime" check (`status='ok'`). If a peer answers an `uploadShards` request, it is reachable.
2. **Successful Downloads (GC/Restore)**: Counts as a "Passed Challenge" (`status='pass'`). Downloading a piece and verifying its BLAKE3 hash provides 100% certainty of data integrity.
3. **Failed Interactions**: Network timeouts count as `unavailable`, and checksum mismatches count as `fail`.

### Background Fingerprint Challenges
1. **Fingerprint Generation**: When the Outbound Worker uploads a piece to a peer, it reads `challenges_per_piece` (default 8) random 32-byte samples from the piece data at random offsets and stores them in the `piece_challenges` table.
2. **Hourly Challenge Cycle**: A background `ChallengeWorker` wakes every hour. For each peer that is storing pieces and has fingerprints, it sends a `challengePiece` RPC.
3. **Verification**: challenger compares the received bytes against the stored expected value.

### Reputation Metrics
Challenge outcomes are logged in `challenge_results` over a rolling 7-day window. These are displayed in the `listpeers` command:
- **Uptime**: (Successful Connections / Total Challenges Made)
- **Integrity (Pass%)**: (Checksum Passes / Total Challenges Made)

## Cap'n Proto Schema (`api/schema.capnp`)

```capnp
@0xdf5e7a9b8c1d2e3f;

using Go = import "/go.capnp";
$Go.package("rpc");
$Go.import("p2p-backup/internal/rpc");

# ... (rest of structs) ...

interface PeerNode {
  offerShards @0 (shards :List(PeerShardMetadata)) -> (neededIndices :List(UInt32));
  uploadShards @1 (shards :List(PeerShardData)) -> (success :Bool, error :Text);
  challengePiece @2 (shardChecksum :Data, offset :UInt64) -> (data :Data);
  releasePiece @3 (shardChecksum :Data) -> (success :Bool, error :Text);
  downloadPiece @4 (shardChecksum :Data) -> (data :Data);
}
```

## Configuration Files (YAML)

### Client Configuration (`client.yaml`)
```yaml
mnemonic: "word1 word2 ... word24"

server:
  address: "127.0.0.1:8080"
  expected_server_key: "4a2b...f3e1" # Hex key from 'server identity'

storage:
  sqlite_path: "backup.db"
  spool_dir: "spool"
  upload_dir: "upload"
```

### Server Configuration (`server.yaml`)
```yaml
mnemonic: "word1 word2 ... word24"
admin_public_key: "6b3c...a2d4" # Authorized client for management

network:
  listen_address: "0.0.0.0:8080"

storage:
  sqlite_path: "server_state.db"
  blob_store_dir: "server_blobs"
  queue_dir: "server_queue"
  keep_local_copy: true
  keep_deleted_minutes: 30
  waste_threshold: 0.5
  gc_interval_minutes: 720
```

## Dependencies
- **`capnproto.org/go/capnp/v3`**: RPC and serialization.
- **`lukechampine.com/blake3`**: Cryptographic hashing.
- **`modernc.org/sqlite`**: Pure-Go SQLite driver (no CGo).
- **`github.com/klauspost/reedsolomon`**: Erasure coding.
- **`golang.org/x/crypto`**: XChaCha20-Poly1305 AEAD encryption and HKDF key derivation.

## Future Work
- **DHT Discovery**: Kademlia-based automatic peer discovery.
- **Storage Contracts**: Time-bound agreements for data retention.
- **Reputation System**: Automated trust escalation based on challenge history and uptime metrics.
- **Reed-Solomon Challenge Regeneration**: Client-side generation of fresh fingerprint challenges for shards that have exhausted their initial pool.
