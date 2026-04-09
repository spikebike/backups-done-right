# Client Commands

```
Usage: client [options] <command> [args...]

Options:
  -config string    Path to the client configuration file (default "client.yaml")
  -db string        Path to the local SQLite database (default "backup.db")
  -v                Enable verbose logging (use -v -v to display Public Keys)
  -fake-upload      Simulate upload without connecting to server
```

## Commands

### backup
```
client backup <dir1> [dir2] ...
client backup --list
```
Scan the specified directories, encrypt new/changed files, and upload them to the server. Use `--list` to show all previous backups with their IDs, timestamps, and file counts.

### restore
```
client restore [-b backup_id] <file_or_dir> [destination_dir]
```
Restore files from the server. Defaults to the newest backup. Downloads encrypted chunks in batches, decrypts, and writes to `destination_dir` (default `restored_files`).

### status
```
client status
```
Display server runtime metrics: uptime, total local full shards, replication progress (fully/partially replicated), hosted peer shard count, and queued data (with partial shard indication).

### history
```
client history <file>
```
List all backed-up versions of a specific file, showing backup ID, modification time, size, and hash.

### prune
```
client prune
```
Find blobs stored on the server that are no longer referenced by any backup and request their deletion. Identifies orphans by comparing server-side blob list against the client's local database.

### reset
```
client reset
```
Clear local modification timestamps to force a full re-scan and re-offer of all files on the next backup. Does not delete any data.

### addpeer
```
client addpeer <address:port>
```
Instruct the server to dial a remote peer via TLS. The peer's public key is extracted from the handshake and registered in the server's database with `untrusted` status.

### updatepeer
```
client updatepeer <id> <status>
```
Change a peer's trust status. Valid statuses: `trusted`, `untrusted`, `blocked`. The peer ID can be found via `listpeers`.

### listpeers
```
client listpeers
```
Display all peers tracked in the server's database, showing their ID, address, public key, trust status, last seen time, and storage usage.
