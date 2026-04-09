# DHT Discovery and Automatic Adoption Pipeline

The Backups Done Right (BDR) system utilizes a fully automated, trust-escalating peer discovery pipeline. This document outlines how nodes find each other on the Kademlia DHT, how they undergo rigorous testing, and how they are eventually promoted to a fully trusted status capable of participating in erasure-coded shard distribution.

## 1. DHT Discovery (`DiscoveryWorker`)
- **Initialization:** Nodes with DHT discovery enabled join the libp2p Kademlia DHT. Nodes with public IP addresses (like VPS servers) can be configured to act as routing servers (`ModeAuto`), making the network resilient and discoverable.
- **Advertisement & Search:** Every ~15 minutes, the worker advertises its presence on the DHT using the service tag `"bdr-v1.0"`. Concurrently, it actively searches for other peers advertising the same tag.
- **Connection & Lazy Registration:** When a new peer is found, the node attempts to connect and triggers a "lazy registration" via the Cap'n Proto `Announce` RPC.

## 2. Immediate Bidirectional Testing
To prevent nodes from idling in a discovered state until their own periodic DHT scan fires, the system utilizes **Immediate Bidirectional Testing**. 
- When Peer A dials Peer B and sends an `Announce` RPC, Peer B immediately registers Peer A.
- During the exact same RPC handler execution, Peer B checks if Automatic Adoption is enabled. If it is, Peer B instantly initiates `StartAdoptionTest` against Peer A in the background.
- This ensures that the moment two nodes make contact, their testing timers start concurrently, establishing a fair and immediate testing cycle.

## 3. The Testing Phase: Dual Verification
During the "Lazy Registration" period, a peer is inserted into the local SQLite database with a primary `status` of `'discovered'` and an `adoption_status` of `'testing'`.

To ensure maximum reliability before granting a node the privilege to hold critical backup data, the system subjects the new node to **two distinct layers of verification**:

### A. The Recovery Shard (Statistical Sampling)
Because the node is now in the database, it becomes eligible to receive the server's mirrored **Recovery Shard** (a 256MB encrypted bundle containing the server's own database and peer list). 
- **Action:** The `OutboundWorker` pushes this shard to the node.
- **Testing Mechanism:** During the upload, the server generates 8 random 32-byte cryptographic fingerprints (Proof of Storage). Throughout the node's lifecycle, a background `ChallengeWorker` will randomly wake up and demand these specific 32 bytes back.
- **Purpose:** This tests latency, baseline honesty, and active disk persistence (random I/O reads). Note: Mirrored shards only require 2 trusted/testing peers to be considered fully replicated.

### B. The Dedicated Test Shard (Full Retrieval)
The `StartAdoptionTest` routine generates a dedicated, mathematically random 256MB test piece specifically for the new node.
- **Action:** This shard is uploaded and recorded in the local database with a special flag (`is_special = 2`) so it isn't confused with real client data.
- **Testing Mechanism:** Once the configured testing period elapses (default 60 minutes), the `AdoptionWorker` wakes up and demands a **100% full retrieval** of this piece via the `PullPieceRaw` mechanism.
- **Purpose:** By downloading the entire 256MB and streaming it through a BLAKE3 hasher, the server tests sustained network bandwidth and absolute data integrity.

## 4. Promotion to Trusted
When the `AdoptionWorker` initiates the full retrieval of the Test Shard:
- **Pass:** If the node successfully returns the 256MB piece and the BLAKE3 hash matches perfectly, the test pieces are deleted from the local database. The peer's `adoption_status` is marked as `'completed'`, and their main `status` is promoted to `'trusted'`. They are now eligible to receive and host real client data shards.
- **Fail:** If the node times out, refuses the connection, or returns a corrupted hash, their `adoption_status` becomes `'failed'`, and their main `status` reverts to `'discovered'`. They will not be entrusted with client data.