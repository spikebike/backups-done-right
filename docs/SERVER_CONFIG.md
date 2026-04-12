# Server Configuration (`server.yaml`)

## top-level
| Parameter | Type | Description |
|-----------|------|-------------|
| `mnemonic` | string | Mandatory. The 24-word recovery phrase for the server's identity. |
| `admin_public_key` | string | Optional. The Hex public key of the client authorized to manage the server. |
| `contact_info` | string | Optional. Contact details for the server operator (email, social, etc.) |

## network
| Parameter | Type | Description |
|-----------|------|-------------|
| `listen_address` | string | Address and port to listen on (default `0.0.0.0:8080`) |
| `standalone_mode` | bool | If true, disables all peer communication, erasure coding, challenges, and repair. Server acts as a simple upload target (default false) |
| `max_concurrent_streams` | int | Max number of concurrent out-of-band piece transfers (default 4) |
| `max_upload_kbps` | int | Maximum outbound bandwidth in KB/s (default 0, unlimited) |
| `max_download_kbps` | int | Maximum inbound bandwidth in KB/s (default 0, unlimited) |

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

### s3 (nested under storage)
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | false | If true, offloads sealed shards to the specified S3-compatible backend. |
| `endpoint` | string | — | S3 Endpoint URL (e.g., `s3.amazonaws.com` or `s3.us-west-004.backblazeb2.com`) |
| `bucket` | string | — | The target S3 bucket name. |
| `access_key` | string | — | S3 Access Key ID. |
| `secret_key` | string | — | S3 Secret Access Key. |
| `use_ssl` | bool | true | Set to false if connecting to a local insecure Minio instance. |

## discovery
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | false | Enable DHT-based peer discovery |
| `listen_address` | string | `0.0.0.0:8080` (UDP) | Address for DHT Kademlia queries |
| `bootstrap_peers` | []string | — | List of multiaddrs to seed the DHT from |
| `automatic_adoption` | object | — | Configuration for testing new DHT-discovered peers |

### automatic_adoption
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | false | If true, automatically test and promote new peers to 'trusted' |
| `test_period_minutes` | int | 60 | Duration the peer must hold challenge data before verification |
| `challenge_pieces` | int | 1 | Number of 256MB random pieces sent to the peer for testing |

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
  spool_dir: "spool"
  queue_dir: "server_queue"
  keep_local_copy: true
  keep_deleted_minutes: 60
  waste_threshold: 0.3
  gc_interval_minutes: 360
  self_backup_interval_minutes: 1440
  untrusted_peer_upload_limit_mb: 1024
  
  s3:
    enabled: true
    endpoint: "s3.amazonaws.com"
    bucket: "my-bdr-bucket"
    access_key: "AKIAIOSFODNN7EXAMPLE"
    secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    use_ssl: true

discovery:
  enabled: true

adoption:
  enabled: true
  test_period_minutes: 60
  challenge_pieces: 1
```
