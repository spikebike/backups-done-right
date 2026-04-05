package rpc

// BlobMeta represents metadata for a blob to be offered.
type BlobMeta struct {
	Hash           string
	Size           int64
	Special        bool
	SequenceNumber uint64
}

// LocalBlobData represents the actual encrypted bytes of a blob.
type LocalBlobData struct {
	Hash            string
	Data            []byte
	IsSpecial       bool   // Used for shard pieces during server DB recovery
	PieceIndex      int    // RS index
	ParentShardHash string // Hash of the full original shard
	SequenceNumber  uint64
	TotalPieces     int
}

// PeerShardMeta represents metadata for an erasure-coded shard from a peer.
type PeerShardMeta struct {
	Hash            string
	Size            int64
	IsSpecial       bool
	PieceIndex      int
	ParentShardHash string
	SequenceNumber  uint64
	TotalPieces     int
}

// StatusInfo represents the monitoring metrics of the server and its P2P state.
type StatusInfo struct {
	UptimeSeconds             uint64
	TotalShards               uint32
	FullyReplicatedShards     uint32
	PartiallyReplicatedShards uint32
	HostedPeerShards          uint32
	QueuedBytes               uint64
}

// UploadJob represents a unit of encrypted data ready for transmission.
type UploadJob struct {
	Hash      string // Blob hash
	Size      int64  // Blob size
	Data      []byte // Encrypted data in memory
	IsSpecial bool
	Release   func() // Returns the Data buffer to the pool (nil if not pooled)
}

// --- Data Plane Protocol Constants ---

const (
	OpCodePush      byte = 0x01
	OpCodePull      byte = 0x02
	OpCodePushBatch byte = 0x03
	OpCodePullBatch byte = 0x04
)

var MagicSync = []byte{0xBD, 0x52}

// PendingClientBlob tracks a blob being uploaded by a client out-of-band.
type PendingClientBlob struct {
	BlobMeta
	ClientPubKey string
}

// PendingStreamMeta tracks a shard piece being transferred between peers out-of-band.
type PendingStreamMeta struct {
	PeerID          int64
	IsSpecial       bool
	PieceIndex      int
	ParentShardHash string
	SequenceNumber  uint64
	TotalPieces     int
	Size            uint64
}
