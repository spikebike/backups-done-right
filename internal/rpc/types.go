package rpc

// BlobMeta represents metadata for a blob to be offered.
type BlobMeta struct {
	Hash    string
	Size    int64
	Special bool
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
