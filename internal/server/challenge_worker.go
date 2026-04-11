package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// StartChallengeWorker periodically validates that peers are genuinely storing the data they accepted.
func (e *Engine) StartChallengeWorker(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.runChallengeCycle(ctx)
		}
	}
}

// TriggerChallenge manually triggers a Proof-of-Storage challenge cycle.
func (e *Engine) TriggerChallenge(ctx context.Context) {
	e.runChallengeCycle(ctx)
}

// pendingChallenge holds one challenge to issue, collected before the cursor closes.
type pendingChallenge struct {
	challengeID   int64
	shardID       int64
	pieceIndex    int
	peerID        int64
	offset        int64
	expectedData  []byte
	hashHex       string
	peerAddr      string
	peerPubKeyHex string
}

func (e *Engine) runChallengeCycle(ctx context.Context) {
	if e.Verbose {
		log.Println("ChallengeWorker: Starting Proof of Storage challenge cycle...")
	}

	// 1. Replenish the pool if we have local copies of shards
	e.replenishPool(ctx)

	// 2. Pick exactly ONE random challenge from the entire pool of uploaded pieces.
	query := `
		SELECT pc.id, pc.shard_id, pc.piece_index, pc.peer_id, pc.offset, pc.expected_data, pc.piece_hash,
		       p.ip_address, p.public_key
		FROM piece_challenges pc
		JOIN outbound_pieces op ON pc.shard_id = op.shard_id AND pc.piece_index = op.piece_index AND pc.peer_id = op.peer_id
		JOIN peers p ON pc.peer_id = p.id
		WHERE op.status = 'uploaded'
		  AND p.status != 'blocked'
		ORDER BY RANDOM()
		LIMIT 1
	`

	var ch pendingChallenge
	err := e.DB.QueryRowContext(ctx, query).Scan(&ch.challengeID, &ch.shardID, &ch.pieceIndex, &ch.peerID,
		&ch.offset, &ch.expectedData, &ch.hashHex, &ch.peerAddr, &ch.peerPubKeyHex)

	if err == sql.ErrNoRows {
		if e.Verbose {
			log.Println("ChallengeWorker: No pending challenges in the pool.")
		}
		return
	} else if err != nil {
		log.Printf("ChallengeWorker: failed to query challenge: %v", err)
		return
	}

	hashBytes, _ := hex.DecodeString(ch.hashHex)

	client, err := e.GetOrDialPeer(ctx, ch.peerID)
	if err != nil {
		log.Printf("ChallengeWorker: failed to connect to peer %d at %s: %v", ch.peerID, ch.peerAddr, err)
		e.RecordChallengeResult(ctx, ch.peerID, ch.shardID, ch.pieceIndex, "unavailable")
		return
	}
	defer client.Close()

	receivedData, err := client.ChallengePiece(ctx, hashBytes, uint64(ch.offset))

	status := "fail"
	if err != nil {
		log.Printf("ChallengeWorker: ChallengePiece RPC to peer %d failed: %v", ch.peerID, err)
		status = "unavailable"
	} else if bytes.Equal(receivedData, ch.expectedData) {
		status = "pass"
	}

	e.RecordChallengeResult(ctx, ch.peerID, ch.shardID, ch.pieceIndex, status)
	// Consume this challenge so it can't be replayed.
	e.DB.ExecContext(ctx, "DELETE FROM piece_challenges WHERE id = ?", ch.challengeID)

	if e.Verbose {
		log.Printf("ChallengeWorker: Peer %d Piece %d result: %s", ch.peerID, ch.pieceIndex, status)
	}
}

func (e *Engine) replenishPool(ctx context.Context) {
	// Find pieces that have fewer than 3 challenges remaining
	query := `
		SELECT op.shard_id, op.piece_index, op.peer_id, s.hash as parent_hash
		FROM outbound_pieces op
		JOIN shards s ON op.shard_id = s.id
		LEFT JOIN piece_challenges pc ON op.shard_id = pc.shard_id AND op.piece_index = pc.piece_index AND op.peer_id = pc.peer_id
		WHERE op.status = 'uploaded'
		GROUP BY op.shard_id, op.piece_index, op.peer_id
		HAVING COUNT(pc.id) < 3
	`
	rows, err := e.DB.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer rows.Close()

	type pieceToRefill struct {
		shardID    int64
		pieceIndex int
		peerID     int64
		parentHash string
	}
	var targets []pieceToRefill
	for rows.Next() {
		var t pieceToRefill
		if err := rows.Scan(&t.shardID, &t.pieceIndex, &t.peerID, &t.parentHash); err == nil {
			targets = append(targets, t)
		}
	}
	rows.Close()

	for _, t := range targets {
		isMirrored := e.IsShardMirrored(ctx, t.shardID)

		// Can we regenerate? Only if we have the piece locally.
		var shardPath string
		if isMirrored {
			// For mirrored shards, the full shard is the piece
			shardPath = filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d_piece_0", t.shardID))
			if _, err := os.Stat(shardPath); err != nil {
				// Not local, maybe it's in the queue?
				shardPath = filepath.Join(e.QueueDir, fmt.Sprintf("shard_%d_piece_%d", t.shardID, t.pieceIndex))
				if _, err := os.Stat(shardPath); err != nil {
					continue // Cannot replenish without data
				}
			}
		} else {
			// For standard shards, we can only replenish if the specific piece is still on disk
			shardPath = filepath.Join(e.QueueDir, fmt.Sprintf("shard_%d_piece_%d", t.shardID, t.pieceIndex))
			if _, err := os.Stat(shardPath); err != nil {
				continue // Cannot replenish without the encoded piece data
			}
		}

		// 1. Get the existing hash from the database to ensure consistency with what the peer tracks.
		hashHex := e.GetPieceHash(ctx, t.shardID, t.pieceIndex, t.peerID)
		if hashHex == "" {
			// If we lost all challenges, fall back to calculation, but try to use hashPiece if possible
			// For now, calculating it is better than nothing, but hashing mirrored shards needs care
			data, err := os.ReadFile(shardPath)
			if err != nil {
				continue
			}

			pieceData := data
			if isMirrored {
				pieceData = padToTargetPieceSize(data, e.ShardSize/int64(e.DataShards))
			}
			hashHex = hex.EncodeToString(e.Hash(pieceData))
		}

		// 2. Read piece bytes for generating new challenges (random offsets)
		data, err := os.ReadFile(shardPath)
		if err != nil {
			continue
		}

		pieceData := data
		if isMirrored {
			pieceData = padToTargetPieceSize(data, e.ShardSize/int64(e.DataShards))
		}

		if len(pieceData) < 32 {
			continue
		}

		maxOffset := len(pieceData) - 32
		for i := 0; i < e.ChallengesPerPiece; i++ {
			offset := rand.Intn(maxOffset)
			expectedData := pieceData[offset : offset+32]
			e.InsertPieceChallenge(ctx, t.shardID, t.pieceIndex, t.peerID, hashHex, offset, expectedData)
		}

		if e.Verbose {
			log.Printf("ChallengeWorker: Replenished %d challenges for Peer %d Piece %d", e.ChallengesPerPiece, t.peerID, t.pieceIndex)
		}
	}
}

// padToTargetPieceSize pads data with zeros to reach targetSize if it's smaller.
func padToTargetPieceSize(data []byte, targetSize int64) []byte {
	if int64(len(data)) < targetSize {
		padded := make([]byte, targetSize)
		copy(padded, data)
		return padded
	}
	return data
}
