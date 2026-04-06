@0xdf5e7a9b8c1d2e3f;

using Go = import "go.capnp";
$Go.package("rpc");
$Go.import("p2p-backup/internal/rpc");

# Metadata for any item (client blob or peer shard) sent over the network
struct TransferMetadata {
  checksum @0 :Data;     # Hash of the item (32 bytes)
  size @1 :UInt64;       # Total byte size
  type @2 :TransferType; # Enum: clientBlob or peerShard
  isSpecial @3 :Bool;    # True for critical metadata (e.g., DB pieces)
  
  # Shard-specific fields (used only if type == peerShard)
  pieceIndex @4 :UInt32; 
  parentShardHash @5 :Data;
  sequenceNumber @6 :UInt64;
  totalPieces @7 :UInt32;
}

enum TransferType {
  clientBlob @0;
  peerShard @1;
}

# A generic chunk of data for any item type
struct TransferData {
  meta @0 :TransferMetadata;
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
  source @16 :Text;
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
  # 1. Offer a list of items. Server replies with indices of items it needs.
  offerItems @0 (items :List(TransferMetadata)) -> (neededIndices :List(UInt32));
  
  # 2. Upload a batch of files (legacy/metadata).
  uploadItems @1 (items :List(TransferData)) -> (success :Bool, error :Text);
  
  # 3. Disaster Recovery: Get a list of all "special" blobs.
  listSpecialItems @2 () -> (items :List(TransferMetadata));
  
  # 4. Restore: Download specific items by their checksums.
  downloadItems @3 (checksums :List(Data)) -> (items :List(TransferData), missing :List(Data));

  # 5. Monitoring metrics.
  getStatus @4 () -> (status :LocalServerStatus);

  # 6. Garbage Collection.
  deleteItems @5 (checksums :List(Data)) -> (success :Bool, error :Text);

  # 7. List all hashes currently stored for this client.
  listAllItems @6 () -> (checksums :List(Data));

  # 8-10. Peer Management
  addPeer @7 (address :Text) -> (success :Bool, error :Text);
  updatePeer @8 (id :UInt64, status :Text, maxStorageSize :UInt64) -> (success :Bool, error :Text);
  listPeers @9 () -> (peers :List(PeerInfo));

  # 11-13. Client Management
  listClients @10 () -> (clients :List(ClientInfo));
  updateClient @11 (id :UInt64, status :Text, maxStorageSize :UInt64) -> (success :Bool, error :Text);
  addClient @12 (publicKey :Text, status :Text, maxStorageSize :UInt64) -> (success :Bool, error :Text);
}

# Interface exposed by remote Peers to the Server
interface PeerNode @0xfe39e0e15b88669f {
  # 1. Symmetric offer pattern for shard distribution.
  offerItems @0 (items :List(TransferMetadata)) -> (neededIndices :List(UInt32));
  
  # 2. Control Plane: Notify peer of upcoming out-of-band data streams
  prepareUpload @1 (items :List(TransferMetadata)) -> (success :Bool, error :Text);
  
  # 3. Integrity: Evaluate PoS hash fingerprints.
  challengePiece @2 (checksum :Data, offset :UInt64) -> (data :Data);

  # 4. GC: Release a specific piece.
  releasePiece @3 (checksum :Data) -> (success :Bool, error :Text);

  # 5. Disaster Recovery: List shard pieces tagged as "Special" for the calling node.
  listSpecialItems @4 () -> (items :List(TransferMetadata));

  # 6. Peer Discovery.
  announce @5 (listenAddress :Text, contactInfo :Text, callback :PeerNode) -> (success :Bool, listenAddress :Text, contactInfo :Text);

  # 7. Retrieval: Retrieve specific pieces by their checksums.
  downloadItems @6 (checksums :List(Data)) -> (items :List(TransferData), missing :List(Data));
}
