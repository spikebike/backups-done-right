package server

import (
	"context"
	"fmt"
)

// --- Shard Queries ---

// IsShardMirrored returns whether a shard uses mirrored (full-copy) replication.
func (e *Engine) IsShardMirrored(ctx context.Context, shardID int64) bool {
	var isMirrored bool
	_ = e.DB.QueryRowContext(ctx, "SELECT mirrored FROM shards WHERE id = ?", shardID).Scan(&isMirrored)
	return isMirrored
}

// --- Storage Queries ---

// GetTotalStorageUsed returns the total bytes stored in local shards and peer-hosted shards.
func (e *Engine) GetTotalStorageUsed(ctx context.Context) (localBytes, peerBytes int64) {
	_ = e.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(size), 0) FROM shards").Scan(&localBytes)
	_ = e.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(size), 0) FROM hosted_shards").Scan(&peerBytes)
	return
}

// CheckGlobalStorageLimit returns an error if adding additionalBytes would exceed MaxStorageBytes.
// Returns nil if no limit is configured or the limit is not exceeded.
func (e *Engine) CheckGlobalStorageLimit(ctx context.Context, additionalBytes int64) error {
	if e.MaxStorageBytes <= 0 {
		return nil
	}
	localTotal, peerTotal := e.GetTotalStorageUsed(ctx)
	if (localTotal + peerTotal + additionalBytes) > e.MaxStorageBytes {
		return fmt.Errorf("global server storage limit exceeded (%d GB)", e.MaxStorageBytes/(1024*1024*1024))
	}
	return nil
}

// --- Peer Queries ---

// CountUploadedPiecesForPeer returns number of uploaded outbound_pieces for a peer.
func (e *Engine) CountUploadedPiecesForPeer(ctx context.Context, peerID int64) int {
	var count int
	_ = e.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbound_pieces WHERE peer_id = ? AND status = 'uploaded'", peerID).Scan(&count)
	return count
}

// --- Challenge Queries ---

// RecordChallengeResult inserts a challenge result for a peer's piece.
func (e *Engine) RecordChallengeResult(ctx context.Context, peerID, shardID int64, pieceIndex int, status string) {
	e.DB.ExecContext(ctx, "INSERT INTO challenge_results (peer_id, shard_id, piece_index, status) VALUES (?, ?, ?, ?)", peerID, shardID, pieceIndex, status)
}

// InsertPieceChallenge stores a new proof-of-storage challenge for a peer's piece.
func (e *Engine) InsertPieceChallenge(ctx context.Context, shardID int64, pieceIndex int, peerID int64, pieceHash string, offset int, expectedData []byte) {
	e.DB.ExecContext(ctx, "INSERT INTO piece_challenges (shard_id, piece_index, peer_id, piece_hash, offset, expected_data) VALUES (?, ?, ?, ?, ?, ?)",
		shardID, pieceIndex, peerID, pieceHash, offset, expectedData)
}

// GetPieceHash returns the stored piece hash for a specific piece challenge, or empty string if not found.
func (e *Engine) GetPieceHash(ctx context.Context, shardID int64, pieceIndex int, peerID int64) string {
	var hash string
	_ = e.DB.QueryRowContext(ctx, "SELECT piece_hash FROM piece_challenges WHERE shard_id = ? AND piece_index = ? AND peer_id = ? LIMIT 1",
		shardID, pieceIndex, peerID).Scan(&hash)
	return hash
}
