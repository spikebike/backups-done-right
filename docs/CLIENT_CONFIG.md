# Client Configuration (`client.yaml`)

## top-level
| Parameter | Type | Description |
|-----------|------|-------------|
| `mnemonic` | string | Mandatory. The 24-word recovery phrase for the client's identity. |

## server
| Parameter | Type | Description |
|-----------|------|-------------|
| `address` | string | Server address and port (e.g. `127.0.0.1:8081`) |
| `expected_server_key` | string | Mandatory. The Hex public key of the server (get it via `server identity`). |

## crypto
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `password` | string | — | Password used to derive the AEAD encryption key (XChaCha20-Poly1305) |
| `block_size_bytes` | int | 4194304 | Chunk size in bytes for file splitting (default 4MB) |

## pipeline
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `scan_threads` | int | 4 | Number of concurrent directory scanner workers |
| `crypto_threads` | int | 4 | Number of concurrent encryption workers |
| `upload_threads` | int | 2 | Number of concurrent uploader workers (Pseudo-Streaming) |
| `max_pipeline_mem_mb` | int | 400 | Max RAM to use for the upload queue (in MB). |
| `batch_upload_size` | int | 10 | Number of 4MB chunks per upload batch (10 = ~40MB per RPC call) |

## storage
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sqlite_path` | string | `backup.db` | Path to the client's local SQLite database |
| `spool_dir` | string | `spool` | **Legacy.** Previously used for temp encryption. No longer used. |
| `upload_dir` | string | `upload` | **Legacy.** Previously used for upload staging. No longer used. |

## backup_directories
A list of directories to back up by default if none are specified on the command line. Each entry supports an optional list of exclusion paths.

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | string | Absolute path to the directory to back up |
| `excludes` | []string | List of subdirectories or files to skip (exact match or prefix) |

## Example
```yaml
mnemonic: "word1 word2 ... word24"

server:
  address: "127.0.0.1:8081"
  expected_server_key: "4a2b...f3e1"

backup_directories:
  - path: "/home/bill"
    excludes:
      - "/home/bill/.cache"
      - "/home/bill/tmp"
  - path: "/etc"

crypto:
  password: "my_encryption_password"

pipeline:
  scan_threads: 4
  crypto_threads: 4
  batch_upload_size: 10

storage:
  sqlite_path: "backup.db"
  spool_dir: "spool"
  upload_dir: "upload"
```
