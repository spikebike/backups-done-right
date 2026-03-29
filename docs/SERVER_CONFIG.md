# Server Configuration (`server.yaml`)

## top-level
| Parameter | Type | Description |
|-----------|------|-------------|
| `mnemonic` | string | Mandatory. The 24-word recovery phrase for the server's identity. |
| `admin_public_key` | string | Optional. The Hex public key of the client authorized to manage the server. |

## network
| Parameter | Type | Description |
|-----------|------|-------------|
| `listen_address` | string | Address and port to listen on (default `0.0.0.0:8080`) |

## erasure_coding
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data_shards_n` | int | 10 | Number of data shards for Reed-Solomon encoding |
| `parity_shards_k` | int | 4 | Number of parity shards for Reed-Solomon encoding |
| `target_piece_size_mb` | int | 256 | Size of each distributed piece in MB. Total shard = `data_shards_n × target_piece_size_mb` |
| `challenges_per_piece` | int | 8 | Number of Proof-of-Storage fingerprints generated per piece at upload |

## storage
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sqlite_path` | string | `server_state.db` | Path to the server's SQLite database |
| `spool_dir` | string | — | Temporary spool directory |
| `queue_dir` | string | `server_queue` | Directory for erasure-coded pieces awaiting P2P distribution |
| `blob_store_dir` | string | `server_blobs` | Directory for stored encrypted blobs and hosted peer shards |
| `master_password` | string | — | Password used to encrypt the server's own database backups |
| `keep_local_copy` | bool | false | If true, retain local shards after full P2P distribution (Hybrid NAS mode) |
| `keep_deleted_minutes` | int | 43200 | Minutes to retain blobs after client deletes them before garbage collection (default 30 days) |
| `waste_threshold` | float64 | 0.5 | Percentage (0.0 to 1.0) of deleted bytes in a shard required to trigger GC repacking |
| `gc_interval_minutes` | int | 720 | How often to run the background GC worker (default 12 hours) |
| `self_backup_interval_minutes` | int | 1440 | How often to backup the server's own database to the swarm (default 24 hours) |
| `untrusted_peer_upload_limit_mb` | int | 1024 | Max MB an untrusted peer may store. Set to 0 to reject all untrusted peers |

## Example
```yaml
mnemonic: "word1 word2 ... word24"
admin_public_key: "6b3c...a2d4"

network:
  listen_address: "0.0.0.0:8081"

erasure_coding:
  data_shards_n: 2
  parity_shards_k: 1
  target_piece_size_mb: 256
  challenges_per_piece: 8

storage:
  sqlite_path: "server_state.db"
  blob_store_dir: "server_blobs"
  queue_dir: "server_queue"
  keep_local_copy: true
  keep_deleted_minutes: 60
  waste_threshold: 0.3
  gc_interval_minutes: 360
  self_backup_interval_minutes: 1440
  untrusted_peer_upload_limit_mb: 1024
```
