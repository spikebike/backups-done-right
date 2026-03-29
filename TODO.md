# Roadmap to Open Source Release

This document tracks the critical features and architectural improvements required to move from a technical prototype to a production-ready, user-friendly P2P backup tool.

## 1. Disaster Recovery & Bootstrapping
- [x] **Mnemonic Recovery String**: Implement a BIP-39 style mnemonic (12-24 words) that encodes the primary server's identity and the master encryption key.
- [x] **Database Discovery**: Mechanism to find the newest "Special" shards (latest sequence) across the swarm and reassemble them. (Implemented via `runRescue` and `SequenceNumber`).
- [x] **Challenge Regeneration**: Mechanism to generate fresh fingerprints for shards that have exhausted their initial pool. (Implemented via `replenishPool`).

## 2. Automatic Peer Discovery (DHT)
- [ ] **Kademlia Integration**: Replace manual `addpeer` workflows with a DHT (e.g., `libp2p`) for automatic node discovery.
- [x] **NAT Hole Punching**: Implement UPnP/PMP or STUN/TURN support to allow home users to trade data without manual port forwarding. (Implemented via Cap'n Proto Capability Passing during Announce).

## 3. Automated Swarm Healing
- [ ] **Repair Worker**: A background process that monitors Reed-Solomon health.
- [x] **Shard Reconstruction**: If a shard drops below a safety threshold, the server must automatically recall pieces and reconstruct the shard. (Implemented via `EnsureShardLocal`).
- [ ] **Swarm Re-distribution**: Automatically distribute newly reconstructed pieces to healthy peers to restore redundancy.

## 4. Resource Management
- [ ] **Bandwidth Throttling**: Add `max_upload_kbps` and `max_download_kbps` settings using a Token Bucket algorithm.
- [ ] **CPU/IO Niceness**: Ensure background encryption and RS-encoding don't impact system responsiveness for desktop users.
- [ ] **Filesystem Watching**: Optional background service using `inotify`/`fsevents` for real-time backups instead of batch scans.

## 5. Automated Quota & Reputation
- [ ] **Space-for-Space Scaling**: Implement an automated "fair exchange" algorithm where a peer's storage quota scales based on how much they are successfully storing for you.
- [ ] **Automated Trust Escalation**: Move from manual `updatepeer` to automated status transitions based on rolling 30-day uptime and integrity metrics.

## 6. Portability & Distribution
- [x] **Internal Certificate CA**: Move certificate generation from `makecert.sh` into the Go binary for one-click setup on Windows/macOS/Linux.
- [ ] **Path Normalization**: Ensure SQLite file paths are stored in a platform-agnostic format to allow cross-OS restoration (e.g., backup on Linux, restore on Windows).
- [ ] **Single-Binary Client**: Statically link all dependencies for easy installation.

## 7. UX & Polish
- [ ] **Progress Bars**: Add rich CLI progress indicators for large file scans and shard uploads.
- [ ] **Dry Run Mode**: A command to see exactly what *would* be backed up and how much space it *would* take before actually doing it.
