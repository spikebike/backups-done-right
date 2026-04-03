package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"p2p-backup/internal/rpc"
)

// StartGCWorker periodically scans for wasted shards and performs garbage collection.
func (e *Engine) StartGCWorker(ctx context.Context) {
	if e.GCIntervalMinutes <= 0 {
		return
	}

	ticker := time.NewTicker(time.Duration(e.GCIntervalMinutes) * time.Minute)
	defer ticker.Stop()

	// Run once at startup
	go e.runGCLoop(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.runGCLoop(ctx)
		}
	}
}

// TriggerGC manually triggers a garbage collection cycle.
func (e *Engine) TriggerGC(ctx context.Context) {
	e.runGCLoop(ctx)
}

// TriggerEncodeShard manually triggers erasure coding for a specific shard.
func (e *Engine) TriggerEncodeShard(shardID int64) {
	e.wg.Add(1)
	e.encodeShard(shardID)
}

func (e *Engine) runGCLoop(ctx context.Context) {
	if e.Verbose {
		log.Println("GCWorker: Starting sweep...")
	}

	// Query for shards that exceed the waste threshold based on bytes
	// A blob is "wasted" if ref_count is 0 AND it was deleted longer than its specific grace period ago.
	// We use KeepMetadataMinutes for special blobs and KeepDeletedMinutes for regular blobs.
	query := `
		SELECT 
			l.shard_id, 
			SUM(CASE 
				WHEN b.ref_count = 0 AND (
					(b.special = 1 AND b.deleted_at < datetime('now', '-' || ? || ' minutes')) OR
					(b.special = 0 AND b.deleted_at < datetime('now', '-' || ? || ' minutes'))
				) THEN l.length ELSE 0 END) as wasted_bytes,
			s.size as total_bytes
		FROM blob_locations l
		JOIN blobs b ON l.blob_hash = b.hash
		JOIN shards s ON l.shard_id = s.id
		WHERE s.status = 'sealed'
		GROUP BY l.shard_id
		HAVING (CAST(wasted_bytes AS FLOAT) / total_bytes) >= ?
		ORDER BY (CAST(wasted_bytes AS FLOAT) / total_bytes) DESC
	`

	rows, err := e.DB.QueryContext(ctx, query, e.KeepMetadataMinutes, e.KeepDeletedMinutes, e.WasteThreshold)
	if err != nil {
		log.Printf("GCWorker: failed to query wasted shards: %v", err)
		return
	}
	defer rows.Close()

	var shardsToProcess []int64
	for rows.Next() {
		var shardID int64
		var wasted, total int64
		if err := rows.Scan(&shardID, &wasted, &total); err == nil {
			shardsToProcess = append(shardsToProcess, shardID)
		}
	}
	rows.Close()

	for _, shardID := range shardsToProcess {
		if err := e.processShardGC(ctx, shardID); err != nil {
			log.Printf("GCWorker: failed to process shard %d: %v", shardID, err)
		}
	}

	if e.Verbose {
		log.Printf("GCWorker: Sweep complete. Processed %d shards.", len(shardsToProcess))
	}
}

func (e *Engine) processShardGC(ctx context.Context, shardID int64) error {
	// 1. Check if 100% wasted
	var totalLive int
	err := e.DB.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM blob_locations l
		JOIN blobs b ON l.blob_hash = b.hash
		WHERE l.shard_id = ? AND (
			b.ref_count > 0 OR 
			(b.special = 1 AND b.deleted_at >= datetime('now', '-' || ? || ' minutes')) OR
			(b.special = 0 AND b.deleted_at >= datetime('now', '-' || ? || ' minutes'))
		)
	`, shardID, e.KeepMetadataMinutes, e.KeepDeletedMinutes).Scan(&totalLive)
	
	if err != nil {
		return err
	}

	if totalLive == 0 {
		if e.Verbose {
			log.Printf("GCWorker: Shard %d is 100%% garbage. Deleting.", shardID)
		}
		return e.deleteWastedShard(ctx, shardID)
	}

	// 2. Repack live blobs
	if e.Verbose {
		log.Printf("GCWorker: Repacking shard %d (%d live blobs).", shardID, totalLive)
	}

	// Ensure shard is local (reconstruct if missing)
	if err := e.EnsureShardLocal(ctx, shardID); err != nil {
		return fmt.Errorf("failed to ensure shard %d is local: %w", shardID, err)
	}

	shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d.dat", shardID))
	f, err := os.Open(shardPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Find live blobs and their offsets
	rows, err := e.DB.QueryContext(ctx, `
		SELECT DISTINCT l.blob_hash, b.special
		FROM blob_locations l
		JOIN blobs b ON l.blob_hash = b.hash
		WHERE l.shard_id = ? AND (
			b.ref_count > 0 OR 
			(b.special = 1 AND b.deleted_at >= datetime('now', '-' || ? || ' minutes')) OR
			(b.special = 0 AND b.deleted_at >= datetime('now', '-' || ? || ' minutes'))
		)
	`, shardID, e.KeepMetadataMinutes, e.KeepDeletedMinutes)
	if err != nil {
		return err
	}
	defer rows.Close()

	var liveBlobs []rpc.LocalBlobData
	for rows.Next() {
		var hash string
		var special bool
		if err := rows.Scan(&hash, &special); err != nil {
			continue
		}

		// Read blob data from the shard
		// We need to read all sequences for this blob in this shard
		blobData, err := e.readBlobFromShardFile(ctx, f, shardID, hash)
		if err != nil {
			log.Printf("GCWorker: failed to read blob %s from shard %d: %v", hash, shardID, err)
			continue
		}

		liveBlobs = append(liveBlobs, rpc.LocalBlobData{
			Hash: hash,
			Data: blobData,
		})
	}
	rows.Close()

	if len(liveBlobs) > 0 {
		// Ingest into current open shard
		if err := e.IngestBlobs(ctx, "system-gc", liveBlobs, true); err != nil {
			return fmt.Errorf("failed to ingest live blobs: %w", err)
		}

		// Now that they are in a new shard, we can remove the old locations
		tx, err := e.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		for _, b := range liveBlobs {
			_, err = tx.ExecContext(ctx, "DELETE FROM blob_locations WHERE shard_id = ? AND blob_hash = ?", shardID, b.Hash)
			if err != nil {
				return err
			}
		}
		
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	// Re-check if shard is now 100% empty of references (should be)
	var refCount int
	err = e.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM blob_locations WHERE shard_id = ?", shardID).Scan(&refCount)
	if err == nil && refCount == 0 {
		return e.deleteWastedShard(ctx, shardID)
	}

	return nil
}

func (e *Engine) readBlobFromShardFile(ctx context.Context, f *os.File, shardID int64, hash string) ([]byte, error) {
	rows, err := e.DB.QueryContext(ctx, "SELECT offset, length FROM blob_locations WHERE shard_id = ? AND blob_hash = ? ORDER BY sequence ASC", shardID, hash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data []byte
	for rows.Next() {
		var offset, length int64
		if err := rows.Scan(&offset, &length); err != nil {
			return nil, err
		}
		buf := make([]byte, length)
		if _, err := f.ReadAt(buf, offset); err != nil {
			return nil, err
		}
		data = append(data, buf...)
	}
	return data, nil
}

func (e *Engine) deleteWastedShard(ctx context.Context, shardID int64) error {
	// 1. Notify peers
	rows, err := e.DB.QueryContext(ctx, "SELECT DISTINCT peer_id, piece_hash FROM piece_challenges WHERE shard_id = ?", shardID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var peerID int64
			var pieceHash string
			if err := rows.Scan(&peerID, &pieceHash); err == nil {
				hashBytes, _ := hex.DecodeString(pieceHash)
				peerStub := e.GetActivePeer(peerID)
				if peerStub.IsValid() {
					wrapper := &CapnpPeerClient{clientStub: peerStub}
					_ = wrapper.ReleasePiece(ctx, hashBytes)
				}
			}
		}
		rows.Close()
	}

	// 2. Delete from DB
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, _ = tx.ExecContext(ctx, "DELETE FROM outbound_pieces WHERE shard_id = ?", shardID)
	_, _ = tx.ExecContext(ctx, "DELETE FROM piece_challenges WHERE shard_id = ?", shardID)
	_, _ = tx.ExecContext(ctx, "DELETE FROM blob_locations WHERE shard_id = ?", shardID)
	_, _ = tx.ExecContext(ctx, "DELETE FROM shards WHERE id = ?", shardID)

	if err := tx.Commit(); err != nil {
		return err
	}

	// 3. Delete file
	shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d.dat", shardID))
	_ = os.Remove(shardPath)

	return nil
}
