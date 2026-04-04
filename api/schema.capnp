@0xdf5e7a9b8c1d2e3f;

using Go = import "/go.capnp";
$Go.package("rpc");
$Go.import("p2p-backup/internal/rpc");

# Metadata for a single encrypted blob
struct BlobMetadata {
  checksum @0 :Data;     # BLAKE3 hash of the encrypted blob (32 bytes)
  size @1 :UInt64;       # Size of the encrypted blob in bytes
  special @2 :Bool;      # True for critical metadata (e.g., the encrypted SQLite DB)
}

# A chunk of encrypted data
struct BlobData {
  checksum @0 :Data;
  data @1 :Data;
}

# Server status and replication metrics
struct LocalServerStatus {
  uptimeSeconds @0 :UInt64;
  totalShards @1 :UInt32;
  fullyReplicatedShards @2 :UInt32;
  partiallyReplicatedShards @3 :UInt32;
  hostedPeerShards @4 :UInt32;
  queuedBytes @5 :UInt64;
}

# Metadata for a remote Peer currently active in the database
struct PeerInfo {
  id @0 :UInt64;
  address @1 :Text;
  publicKey @2 :Text;
  status @3 :Text;
  firstSeen @4 :Text;
  lastSeen @5 :Text;
  maxStorageSize @6 :UInt64;
  currentStorageSize @7 :UInt64;
  outboundStorageSize @8 :UInt64;
  challengesMade @9 :UInt32;
  challengesPassed @10 :UInt32;
  connectionsOk @11 :UInt32;
  integrityAttempts @12 :UInt32;
  contactInfo @13 :Text;
  totalShards @14 :UInt64;
  currentShards @15 :UInt64;
}

struct ClientInfo {
  id @0 :UInt64;
  publicKey @1 :Text;
  status @2 :Text;
  lastSeen @3 :Text;
  maxStorageSize @4 :UInt64;
  currentStorageSize @5 :UInt64;
}

# Interface exposed by the Server to the local Client
interface BackupServer @0xd606e7f1b66afc98 {
  # 1. Offer a list of blobs. Server replies with indices of blobs it needs.
  offerBlobs @0 (blobs :List(BlobMetadata)) -> (neededIndices :List(UInt32));
  
  # 2. Control Plane: Notify server of upcoming out-of-band blob streams.
  prepareUploadClient @1 (blobs :List(BlobMetadata)) -> (success :Bool, error :Text);
  
  # 3. Disaster Recovery: Get a list of all "special" blobs (the encrypted SQLite DBs).
  listSpecialBlobs @2 () -> (specialBlobs :List(BlobMetadata));
  
  # @3 (downloadBlobs) removed: handled out-of-band via raw stream

  # 5. Monitoring: Get current server status and swarm replication metrics.
  getStatus @3 () -> (status :LocalServerStatus);

  # 6. Garbage Collection: Mark specific blobs as deleted (to be purged after retention period).
  deleteBlobs @4 (checksums :List(Data)) -> (success :Bool, error :Text);

  # 7. Garbage Collection: Get a list of all blob hashes currently stored for this client.
  listAllBlobs @5 () -> (checksums :List(Data));

  # 8. Manage Peers: Add a new peer by IP address/port.
  addPeer @6 (address :Text) -> (success :Bool, error :Text);

  # 9. Manage Peers: Set peer status (e.g. trusted/blocked) and storage limit.
  updatePeer @7 (id :UInt64, status :Text, maxStorageSize :UInt64) -> (success :Bool, error :Text);

  # 10. Manage Peers: List all active peers.
  listPeers @8 () -> (peers :List(PeerInfo));

  # 11. Manage Clients: List all known clients.
  listClients @9 () -> (clients :List(ClientInfo));

  # 12. Manage Clients: Update client status and quota.
  updateClient @10 (id :UInt64, status :Text, maxStorageSize :UInt64) -> (success :Bool, error :Text);

  # 13. Manage Clients: Add a new client by Public Key.
  addClient @11 (publicKey :Text, status :Text, maxStorageSize :UInt64) -> (success :Bool, error :Text);
}

# Metadata for an erasure-coded shard sent to a peer
struct PeerShardMetadata {
  checksum @0 :Data;     # Hash of the encrypted shard piece
  size @1 :UInt64;       # Size of the shard piece
  isSpecial @2 :Bool;    # True if this is critical metadata (e.g. Server DB)
  pieceIndex @3 :UInt32; # RS index (0 to N+K-1)
  parentShardHash @4 :Data; # Hash of the full original shard
  sequenceNumber @5 :UInt64; # Version/Timestamp for discovery
  totalPieces @6 :UInt32;    # Total parts in this shard version
  }

  # Data for an erasure-coded shard
  struct PeerShardData {
  checksum @0 :Data;
  data @1 :Data;
  isSpecial @2 :Bool;
  pieceIndex @3 :UInt32;
  parentShardHash @4 :Data;
  sequenceNumber @5 :UInt64;
  totalPieces @6 :UInt32;
  }

# Interface exposed by remote Peers to the Server
interface PeerNode @0xfe39e0e15b88669f {
  offerShards @0 (shards :List(PeerShardMetadata)) -> (neededIndices :List(UInt32));
  
  # 2. Control Plane: Notify peer of upcoming out-of-band data streams
  prepareUpload @1 (shards :List(PeerShardMetadata)) -> (success :Bool, error :Text);
  
  # Evaluate PoS hash fingerprints over explicit intervals natively
  challengePiece @2 (shardChecksum :Data, offset :UInt64) -> (data :Data);

  # Garbage Collection: Tell a peer we no longer need them to store this piece.
  releasePiece @3 (shardChecksum :Data) -> (success :Bool, error :Text);

  # @4 (downloadPiece) removed: handled out-of-band via raw stream

  # Disaster Recovery: List shard pieces tagged as "Special" for the calling node.
  listSpecialPieces @4 () -> (shards :List(PeerShardMetadata));

  # Peer Discovery: Tell a peer our own listen address so they can dial us back (or use the callback directly).
  announce @5 (listenAddress :Text, contactInfo :Text, callback :PeerNode) -> (success :Bool);
}
