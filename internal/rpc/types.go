package rpc

import (
	"encoding/hex"

	capnp "capnproto.org/go/capnp/v3"
)

// Metadata represents any item (client blob or peer shard) sent over the network.
type Metadata struct {
	Hash            string
	Size            int64
	Type            int  // 0: ClientBlob, 1: PeerShard
	IsSpecial       bool // True for critical bits (e.g. DB metadata)
	
	// Shard-specific fields (used only if Type == 1)
	PieceIndex      int
	ParentShardHash string
	SequenceNumber  uint64
	TotalPieces     int
}

// ItemData represents the actual payload for a Metadated item.
type ItemData struct {
	Meta Metadata
	Data []byte
}

// UploadPlan tells the client which items actually need to be uploaded.
type UploadPlan struct {
	NeededIndices []int32
}

func (m *Metadata) IsClientBlob() bool {
	return m.Type == 0
}

func (m *Metadata) IsPeerShard() bool {
	return m.Type == 1
}

// MetadataFromCapnp converts a Cap'n Proto TransferMetadata to the internal Go Metadata struct.
func MetadataFromCapnp(cm TransferMetadata) Metadata {
	hash, _ := cm.Checksum()
	parentHash, _ := cm.ParentShardHash()
	return Metadata{
		Hash:            hex.EncodeToString(hash),
		Size:            int64(cm.Size()),
		Type:            int(cm.Type()),
		IsSpecial:       cm.IsSpecial(),
		PieceIndex:      int(cm.PieceIndex()),
		ParentShardHash: hex.EncodeToString(parentHash),
		SequenceNumber:  cm.SequenceNumber(),
		TotalPieces:     int(cm.TotalPieces()),
	}
}

// MetadataToCapnp converts an internal Go Metadata struct to a Cap'n Proto TransferMetadata.
func MetadataToCapnp(seg *capnp.Segment, m Metadata) (TransferMetadata, error) {
	cm, err := NewTransferMetadata(seg)
	if err != nil {
		return TransferMetadata{}, err
	}
	hashBytes, _ := hex.DecodeString(m.Hash)
	cm.SetChecksum(hashBytes)
	cm.SetSize(uint64(m.Size))
	cm.SetType(TransferType(m.Type))
	cm.SetIsSpecial(m.IsSpecial)
	cm.SetPieceIndex(uint32(m.PieceIndex))
	parentHashBytes, _ := hex.DecodeString(m.ParentShardHash)
	cm.SetParentShardHash(parentHashBytes)
	cm.SetSequenceNumber(m.SequenceNumber)
	cm.SetTotalPieces(uint32(m.TotalPieces))
	return cm, nil
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
