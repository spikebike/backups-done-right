package server

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"lukechampine.com/blake3"
	"p2p-backup/internal/rpc"
)

// OutboundWorker periodically scans the queue directory for erasure-coded pieces
// and uploads them to available peers.
func (e *Engine) StartOutboundWorker(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	// Mirror sync runs more frequently during bootstrap/testing
	mirrorTicker := time.NewTicker(60 * time.Second)
	defer mirrorTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.processQueue(ctx)
		case <-mirrorTicker.C:
			e.syncMirroredShards(ctx)
		}
	}
}

func (e *Engine) syncMirroredShards(ctx context.Context) {
	// Find all mirrored shards
	rows, err := e.DB.QueryContext(ctx, "SELECT id, hash, sequence, total_pieces FROM shards WHERE mirrored = 1")
	if err != nil {
		return
	}
	defer rows.Close()

	type mShard struct {
		id     int64
		hash   string
		seq    uint64
		total  int
	}
	var mirrored []mShard
	for rows.Next() {
		var s mShard
		if err := rows.Scan(&s.id, &s.hash, &s.seq, &s.total); err == nil {
			mirrored = append(mirrored, s)
		}
	}
	rows.Close()

	for _, s := range mirrored {
		// For each mirrored shard, find peers who don't have it yet.
		// We exclude peers who already have an 'uploaded' or 'pending' record for Piece 0.
		query := `
			SELECT p.id, p.ip_address, p.public_key 
			FROM peers p
			LEFT JOIN outbound_pieces op ON p.id = op.peer_id AND op.shard_id = ? AND op.piece_index = 0
			WHERE p.status != 'blocked'
			  AND (op.shard_id IS NULL OR (op.status != 'uploaded' AND op.status != 'pending'))
		`
		pRows, err := e.DB.QueryContext(ctx, query, s.id)
		if err != nil {
			continue
		}

		shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d.dat", s.id))
		data, err := os.ReadFile(shardPath)
		if err != nil {
			pRows.Close()
			continue
		}

		// Ensure the data is padded to targetPieceSize to match the trade unit
		targetPieceSize := e.ShardSize / int64(e.DataShards)
		if int64(len(data)) < targetPieceSize {
			padded := make([]byte, targetPieceSize)
			copy(padded, data)
			data = padded
		}

		for pRows.Next() {
			var peerID int64
			var peerAddr, peerPubKey string
			if err := pRows.Scan(&peerID, &peerAddr, &peerPubKey); err == nil {
				// Use the existing performUpload logic
				_ = e.performUpload(ctx, peerID, peerAddr, peerPubKey, s.id, 0, data, s.hash, true, s.hash, s.seq, s.total)
			}
		}
		pRows.Close()
	}
}

func (e *Engine) processQueue(ctx context.Context) {
	entries, err := os.ReadDir(e.QueueDir)
	if err != nil {
		log.Printf("OutboundWorker: Failed to read queue dir: %v", err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "shard_") {
			continue
		}

		// Filename format: shard_1_piece_0
		parts := strings.Split(entry.Name(), "_")
		if len(parts) != 4 {
			continue
		}

		shardID, err1 := strconv.ParseInt(parts[1], 10, 64)
		pieceIndex, err2 := strconv.Atoi(parts[3])
		if err1 != nil || err2 != nil {
			continue
		}

		filePath := filepath.Join(e.QueueDir, entry.Name())
		err = e.uploadPiece(ctx, shardID, pieceIndex, filePath)
		if err != nil {
			log.Printf("OutboundWorker: Failed to upload piece %s: %v", entry.Name(), err)
		} else {
			// Successfully uploaded and recorded, delete the local queued piece
			os.Remove(filePath)
			e.checkShardCompletion(ctx, shardID)
		}
	}
}

func (e *Engine) uploadPiece(ctx context.Context, shardID int64, pieceIndex int, filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	hashBytes := blake3.Sum256(data)
	hashHex := hex.EncodeToString(hashBytes[:])

	// Check if this shard is mirrored and get its metadata
	var isMirrored bool
	var parentShardHash string
	var sequence uint64
	var totalPieces int
	err = e.DB.QueryRowContext(ctx, "SELECT mirrored, hash, sequence, total_pieces FROM shards WHERE id = ?", shardID).Scan(&isMirrored, &parentShardHash, &sequence, &totalPieces)
	if err == sql.ErrNoRows {
		log.Printf("OutboundWorker: shard %d no longer exists in DB. Deleting orphaned piece: %s", shardID, filePath)
		os.Remove(filePath)
		return nil
	}
	if err != nil {
		return fmt.Errorf("query shard metadata: %w", err)
	}

	// For Mirrored Shards, we keep looping until we've tried all peers.
	// For Standard Shards, we just do one upload per call.
	for {
		// Find a suitable peer
		query := `
			SELECT p.id, p.ip_address, p.public_key 
			FROM peers p
			LEFT JOIN outbound_pieces op ON p.id = op.peer_id AND op.shard_id = ?
		`
	if isMirrored {
		query += " AND op.piece_index = ? "
	}

	query += `
		WHERE p.outbound_storage_size + ? <= 
		  CASE WHEN p.status = 'untrusted' THEN ? 
		  ELSE p.max_storage_size * 1024 * 1024 * 1024 END
		  AND (op.shard_id IS NULL OR (op.status != 'uploaded' AND op.status != 'pending'))
		  AND p.status != 'blocked'
		ORDER BY p.last_seen DESC
		LIMIT 1
	`
		var peerID int64
		var peerAddr string
		var peerPubKeyHex string

		limitBytes := e.UntrustedPeerUploadLimitMB * 1024 * 1024
		
		var err error
		if isMirrored {
			err = e.DB.QueryRowContext(ctx, query, shardID, pieceIndex, int64(len(data)), limitBytes).Scan(&peerID, &peerAddr, &peerPubKeyHex)
		} else {
			err = e.DB.QueryRowContext(ctx, query, shardID, int64(len(data)), limitBytes).Scan(&peerID, &peerAddr, &peerPubKeyHex)
		}

		if err == sql.ErrNoRows {
			if isMirrored {
				return nil // Successfully tried all available peers
			}
			return fmt.Errorf("no suitable peer found for piece")
		} else if err != nil {
			return fmt.Errorf("find suitable peer: %w", err)
		}

		if err := e.performUpload(ctx, peerID, peerAddr, peerPubKeyHex, shardID, pieceIndex, data, hashHex, isMirrored, parentShardHash, sequence, totalPieces); err != nil {
			log.Printf("OutboundWorker: upload to peer %d failed: %v", peerID, err)
			if !isMirrored {
				return err
			}
			// For mirrored shards, just continue to next peer
			continue
		}

		if !isMirrored {
			return nil // Finished one standard piece
		}
		// For mirrored, loop again to find the next peer
	}
}

func (e *Engine) performUpload(ctx context.Context, peerID int64, peerAddr string, peerPubKeyHex string, shardID int64, pieceIndex int, data []byte, hashHex string, isMirrored bool, parentShardHash string, sequence uint64, totalPieces int) error {
	client, err := e.GetOrDialPeer(ctx, peerID)
	if err != nil {
		return err
	}
	defer client.Close()

	meta := []rpc.PeerShardMeta{
		{
			Hash:            hashHex,
			Size:            int64(len(data)),
			IsSpecial:       isMirrored,
			PieceIndex:      pieceIndex,
			ParentShardHash: parentShardHash,
			SequenceNumber:  sequence,
			TotalPieces:     totalPieces,
		},
	}

	needed, err := client.OfferShards(ctx, meta)
	if err != nil {
		e.RemoveActivePeer(peerID)
		return fmt.Errorf("offer shards (connection may have dropped): %w", err)
	}

	if len(needed) > 0 {
		uploadData := []rpc.LocalBlobData{
			{
				Hash:            hashHex,
				Data:            data,
				IsSpecial:       isMirrored,
				PieceIndex:      pieceIndex,
				ParentShardHash: parentShardHash,
				SequenceNumber:  sequence,
				TotalPieces:     totalPieces,
			},
		}
		err = client.UploadShards(ctx, uploadData)
		if err != nil {
			e.RemoveActivePeer(peerID)
			// Record failure
			_, _ = e.DB.ExecContext(ctx, "INSERT INTO challenge_results (peer_id, shard_id, piece_index, status) VALUES (?, ?, ?, 'unavailable')", peerID, shardID, pieceIndex)
			return fmt.Errorf("upload shards: %w", err)
		}
		// Record success (counts as uptime)
		_, _ = e.DB.ExecContext(ctx, "INSERT INTO challenge_results (peer_id, shard_id, piece_index, status) VALUES (?, ?, ?, 'ok')", peerID, shardID, pieceIndex)
	}

	// Update our database
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Check if we already recorded this piece as uploaded to this peer
	var alreadyUploaded bool
	_ = tx.QueryRowContext(ctx, "SELECT 1 FROM outbound_pieces WHERE shard_id = ? AND piece_index = ? AND peer_id = ? AND status = 'uploaded'", shardID, pieceIndex, peerID).Scan(&alreadyUploaded)

	_, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO outbound_pieces (shard_id, piece_index, peer_id, status) VALUES (?, ?, ?, 'uploaded')", shardID, pieceIndex, peerID)
	if err != nil {
		return err
	}

	if !alreadyUploaded {
		_, err = tx.ExecContext(ctx, "UPDATE peers SET outbound_storage_size = outbound_storage_size + ? WHERE id = ?", int64(len(data)), peerID)
		if err != nil {
			return err
		}
	}

	if e.ChallengesPerPiece > 0 {
		maxOffset := len(data) - 32
		if maxOffset > 0 {
			for i := 0; i < e.ChallengesPerPiece; i++ {
				offset := rand.Intn(maxOffset)
				expectedData := data[offset : offset+32]
				_, err = tx.ExecContext(ctx, "INSERT INTO piece_challenges (shard_id, piece_index, peer_id, piece_hash, offset, expected_data) VALUES (?, ?, ?, ?, ?, ?)", shardID, pieceIndex, peerID, hashHex, offset, expectedData)
				if err != nil {
					log.Printf("OutboundWorker: failed to insert piece challenge natively: %v", err)
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	if e.Verbose {
		log.Printf("OutboundWorker: Successfully uploaded piece %d of shard %d to peer %d (%s)", pieceIndex, shardID, peerID, peerAddr)
	}

	return nil
}

func (e *Engine) checkShardCompletion(ctx context.Context, shardID int64) {
	// Check if this is a mirrored shard
	var isMirrored bool
	_ = e.DB.QueryRowContext(ctx, "SELECT mirrored FROM shards WHERE id = ?", shardID).Scan(&isMirrored)
	
	if isMirrored {
		return
	}

	// Check if all pieces (DataShards + ParityShards) are successfully uploaded
	var count int
	err := e.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbound_pieces WHERE shard_id = ? AND status = 'uploaded'", shardID).Scan(&count)
	if err != nil {
		log.Printf("OutboundWorker: check completion failed: %v", err)
		return
	}

	if count >= (e.DataShards + e.ParityShards) {
		if !e.KeepLocalCopy {
			shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d.dat", shardID))
			if err := os.Remove(shardPath); err == nil {
				if e.Verbose {
					log.Printf("OutboundWorker: Shard %d fully distributed. Deleted local copy (KeepLocalCopy=false).", shardID)
				}
			}
		} else {
			if e.Verbose {
				log.Printf("OutboundWorker: Shard %d fully distributed. Kept local copy (KeepLocalCopy=true).", shardID)
			}
		}
	}
}
