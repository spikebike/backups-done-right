package server

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"p2p-backup/internal/rpc"

	"lukechampine.com/blake3"
)

type QueueJob struct {
	ShardID         int64
	PieceIndex      int
	FilePath        string
	Size            int64
	HashHex         string
	IsMirrored      bool
	ParentShardHash string
	Sequence        uint64
	TotalPieces     int
}

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

// TriggerOutbound manually triggers a scan of the outbound queue.
func (e *Engine) TriggerOutbound(ctx context.Context) {
	e.processQueue(ctx)
}

// TriggerSyncMirrored manually triggers a sync of mirrored shards.
func (e *Engine) TriggerSyncMirrored(ctx context.Context) {
	e.syncMirroredShards(ctx)
}

func (e *Engine) syncMirroredShards(ctx context.Context) {
	// Find all mirrored shards
	rows, err := e.DB.QueryContext(ctx, "SELECT id, hash, sequence, total_pieces FROM shards WHERE mirrored = 1")
	if err != nil {
		return
	}
	defer rows.Close()

	type mShard struct {
		id    int64
		hash  string
		seq   uint64
		total int
	}
	var mirrored []mShard
	for rows.Next() {
		var s mShard
		if err := rows.Scan(&s.id, &s.hash, &s.seq, &s.total); err == nil {
			mirrored = append(mirrored, s)
		}
	}
	rows.Close()

	peerBatches := make(map[int64][]QueueJob)
	
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("syncMirroredShards: failed to start tx: %v", err)
		return
	}
	defer tx.Rollback()

	for _, s := range mirrored {
		// For each mirrored shard, find peers who don't have it yet.
		query := `
			SELECT p.id
			FROM peers p
			LEFT JOIN outbound_pieces op ON p.id = op.peer_id AND op.shard_id = ? AND op.piece_index = 0
			WHERE p.status != 'blocked'
			  AND (op.shard_id IS NULL OR (op.status != 'uploaded' AND op.status != 'pending'))
		`
		pRows, err := tx.QueryContext(ctx, query, s.id)
		if err != nil {
			continue
		}

		shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d.dat", s.id))
		stat, err := os.Stat(shardPath)
		if err != nil {
			pRows.Close()
			continue
		}

		// Ensure the padding is accounted for in the size sent over the wire
		targetPieceSize := e.ShardSize / int64(e.DataShards)
		size := stat.Size()
		if size < targetPieceSize {
			size = targetPieceSize
		}

		var peers []int64
		for pRows.Next() {
			var peerID int64
			if err := pRows.Scan(&peerID); err == nil {
				peers = append(peers, peerID)
			}
		}
		pRows.Close()

		for _, peerID := range peers {
			// Record pending status
			_, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO outbound_pieces (shard_id, piece_index, peer_id, status) VALUES (?, 0, ?, 'pending')", s.id, peerID)
			if err != nil {
				continue
			}
			
			// Update outbound storage tracking
			_, err = tx.ExecContext(ctx, "UPDATE peers SET outbound_storage_size = outbound_storage_size + ? WHERE id = ?", size, peerID)
			if err != nil {
				continue
			}

			job := QueueJob{
				ShardID:         s.id,
				PieceIndex:      0,
				FilePath:        shardPath,
				Size:            size,
				HashHex:         s.hash,
				IsMirrored:      true,
				ParentShardHash: s.hash,
				Sequence:        s.seq,
				TotalPieces:     s.total,
			}
			peerBatches[peerID] = append(peerBatches[peerID], job)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("syncMirroredShards: failed to commit tx: %v", err)
		return
	}

	// Launch parallel uploads
	var wg sync.WaitGroup
	for peerID, batch := range peerBatches {
		wg.Add(1)
		go func(pID int64, b []QueueJob) {
			defer wg.Done()
			e.performUploadBatch(ctx, pID, b)
		}(peerID, batch)
	}
	wg.Wait()
}

func (e *Engine) processQueue(ctx context.Context) {
	entries, err := os.ReadDir(e.QueueDir)
	if err != nil {
		log.Printf("OutboundWorker: Failed to read queue dir: %v", err)
		return
	}

	var jobs []QueueJob

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "shard_") {
			continue
		}

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
		stat, err := os.Stat(filePath)
		if err != nil {
			continue
		}

		var job QueueJob
		job.ShardID = shardID
		job.PieceIndex = pieceIndex
		job.FilePath = filePath
		job.Size = stat.Size()

		err = e.DB.QueryRowContext(ctx, "SELECT mirrored, hash, sequence, total_pieces FROM shards WHERE id = ?", shardID).Scan(&job.IsMirrored, &job.ParentShardHash, &job.Sequence, &job.TotalPieces)
		if err == sql.ErrNoRows {
			log.Printf("OutboundWorker: shard %d no longer exists in DB. Deleting orphaned piece: %s", shardID, filePath)
			os.Remove(filePath)
			continue
		} else if err != nil {
			continue
		}

		// Hash file efficiently using StreamBufferPool
		f, err := os.Open(filePath)
		if err != nil {
			continue
		}
		hasher := blake3.New(32, nil)
		buf := e.StreamBufferPool.Get().([]byte)
		_, err = io.CopyBuffer(hasher, f, buf)
		e.StreamBufferPool.Put(buf)
		f.Close()

		if err != nil {
			log.Printf("OutboundWorker: failed to hash piece %s: %v", filePath, err)
			continue
		}
		
		job.HashHex = hex.EncodeToString(hasher.Sum(nil))
		jobs = append(jobs, job)
	}

	if len(jobs) == 0 {
		return
	}

	// Group jobs by target PeerID
	peerBatches := make(map[int64][]QueueJob)
	
	// Transaction to map pieces to peers and track quotas
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("OutboundWorker: failed to start tx for assignment: %v", err)
		return
	}

	for _, job := range jobs {
		query := `
			SELECT p.id
			FROM peers p
			LEFT JOIN outbound_pieces op ON p.id = op.peer_id AND op.shard_id = ?
		`
		if job.IsMirrored {
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
		limitBytes := e.UntrustedPeerUploadLimitMB * 1024 * 1024
		var peerID int64
		var tErr error

		if job.IsMirrored {
			tErr = tx.QueryRowContext(ctx, query, job.ShardID, job.PieceIndex, job.Size, limitBytes).Scan(&peerID)
		} else {
			tErr = tx.QueryRowContext(ctx, query, job.ShardID, job.Size, limitBytes).Scan(&peerID)
		}

		if tErr == sql.ErrNoRows {
			continue
		} else if tErr != nil {
			log.Printf("OutboundWorker: peer assignment query failed: %v", tErr)
			continue
		}

		_, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO outbound_pieces (shard_id, piece_index, peer_id, status) VALUES (?, ?, ?, 'pending')", job.ShardID, job.PieceIndex, peerID)
		if err != nil {
			continue
		}
		
		// Temporarily inflate constraint tracking map
		_, err = tx.ExecContext(ctx, "UPDATE peers SET outbound_storage_size = outbound_storage_size + ? WHERE id = ?", job.Size, peerID)
		if err != nil {
			continue
		}

		peerBatches[peerID] = append(peerBatches[peerID], job)
	}

	tx.Commit()

	// Launch multiplexed batches!
	var wg sync.WaitGroup
	for peerID, batch := range peerBatches {
		wg.Add(1)
		go func(pID int64, b []QueueJob) {
			defer wg.Done()
			e.performUploadBatch(ctx, pID, b)
		}(peerID, batch)
	}
	wg.Wait()
}

func (e *Engine) performUploadBatch(ctx context.Context, peerID int64, jobs []QueueJob) {
	client, err := e.GetOrDialPeer(ctx, peerID)
	if err != nil {
		e.failJobs(ctx, peerID, jobs)
		log.Printf("OutboundWorker: batch dial failed for peer %d", peerID)
		return
	}
	defer client.Close()

	var meta []rpc.PeerShardMeta
	jobMap := make(map[string]QueueJob)
	
	for _, job := range jobs {
		meta = append(meta, rpc.PeerShardMeta{
			Hash:            job.HashHex,
			Size:            job.Size,
			IsSpecial:       job.IsMirrored,
			PieceIndex:      job.PieceIndex,
			ParentShardHash: job.ParentShardHash,
			SequenceNumber:  job.Sequence,
			TotalPieces:     job.TotalPieces,
		})
		jobMap[job.HashHex] = job
	}

	needed, err := client.OfferShards(ctx, meta)
	if err != nil {
		e.RemoveActivePeer(peerID)
		e.failJobs(ctx, peerID, jobs)
		return
	}

	if len(needed) > 0 {
		err = client.PrepareUpload(ctx, meta)
		if err != nil {
			e.RemoveActivePeer(peerID)
			e.failJobs(ctx, peerID, jobs)
			return
		}

		var streamWg sync.WaitGroup
		for _, idx := range needed {
			streamWg.Add(1)
			job := jobMap[meta[idx].Hash]
			
			go func(j QueueJob) {
				defer streamWg.Done()
				
				e.streamSemaphore <- struct{}{}
				defer func() { <-e.streamSemaphore }()

				err := e.PushPieceDirectStreamed(ctx, peerID, j)
				if err != nil {
					e.failJob(ctx, peerID, j)
					// challenge result unavailable
					e.DB.ExecContext(ctx, "INSERT INTO challenge_results (peer_id, shard_id, piece_index, status) VALUES (?, ?, ?, 'unavailable')", peerID, j.ShardID, j.PieceIndex)
					log.Printf("OutboundWorker: stream push failed for %s: %v", j.HashHex, err)
					return
				}
				
				e.finalizeJobSuccess(ctx, peerID, j)
				// Remove successfully uploaded piece file, except for mirrored shards 
				// (which are actual storage files and should be kept intact, piece_index=0 represents the whole file).
				// We actually don't want to delete original blobs for mirrored, just QueueDir ones.
				if !j.IsMirrored || strings.Contains(j.FilePath, "server_queue") {
				    os.Remove(j.FilePath)
				}
				e.checkShardCompletion(ctx, j.ShardID)
			}(job)
		}
		streamWg.Wait()
		
		// Any pieces NOT needed were already possessed by the peer! We should mark them as success too.
		neededMap := make(map[int]bool)
		for _, idx := range needed {
			neededMap[int(idx)] = true
		}
		for i, job := range jobs {
			if !neededMap[i] {
				e.finalizeJobSuccess(ctx, peerID, job)
				if !job.IsMirrored || strings.Contains(job.FilePath, "server_queue") {
				    os.Remove(job.FilePath)
				}
				e.checkShardCompletion(ctx, job.ShardID)
			}
		}
	} else {
		// All pieces accepted instantly (none needed transfer)
		for _, j := range jobs {
			e.finalizeJobSuccess(ctx, peerID, j)
			if !j.IsMirrored || strings.Contains(j.FilePath, "server_queue") {
			    os.Remove(j.FilePath)
			}
			e.checkShardCompletion(ctx, j.ShardID)
		}
	}
}

type ZeroReader struct {
	Remaining int64
}

func (r *ZeroReader) Read(p []byte) (n int, err error) {
	if r.Remaining <= 0 {
		return 0, io.EOF
	}
	toRead := int64(len(p))
	if toRead > r.Remaining {
		toRead = r.Remaining
	}
	for i := int64(0); i < toRead; i++ {
		p[i] = 0
	}
	r.Remaining -= toRead
	return int(toRead), nil
}

func (e *Engine) PushPieceDirectStreamed(ctx context.Context, peerID int64, job QueueJob) error {
	f, err := os.Open(job.FilePath)
	if err != nil {
		return fmt.Errorf("open file stream: %w", err)
	}
	defer f.Close()
	
	// Handle special cases where padding to TargetPieceSize is required for Mirrored chunks
	targetPieceSize := e.ShardSize / int64(e.DataShards)
	
	var reader io.Reader = f
	if job.IsMirrored && job.Size == targetPieceSize {
	    // If the file is physically smaller but job.Size is maxed (due to padding in syncMirrored), we must pad the stream.
	    stat, _ := f.Stat()
	    if stat.Size() < targetPieceSize {
	        // Use ZeroReader to append zeros dynamically during stream to avoid massive allocations
	        reader = io.MultiReader(f, &ZeroReader{Remaining: targetPieceSize - stat.Size()})
	    }
	}

	return e.PushPiece(ctx, peerID, reader, job.Size, job.HashHex)
}

func (e *Engine) failJobs(ctx context.Context, peerID int64, jobs []QueueJob) {
	for _, j := range jobs {
		e.failJob(ctx, peerID, j)
	}
}

func (e *Engine) failJob(ctx context.Context, peerID int64, job QueueJob) {
	e.DB.ExecContext(ctx, "UPDATE peers SET outbound_storage_size = outbound_storage_size - ? WHERE id = ?", job.Size, peerID)
	e.DB.ExecContext(ctx, "DELETE FROM outbound_pieces WHERE shard_id = ? AND piece_index = ? AND peer_id = ?", job.ShardID, job.PieceIndex, peerID)
}

func (e *Engine) finalizeJobSuccess(ctx context.Context, peerID int64, job QueueJob) {
	_, err := e.DB.ExecContext(ctx, "INSERT OR REPLACE INTO outbound_pieces (shard_id, piece_index, peer_id, status) VALUES (?, ?, ?, 'uploaded')", job.ShardID, job.PieceIndex, peerID)
	if err != nil {
		log.Printf("OutboundWorker: failed to finalize success in DB: %v", err)
	}
	e.DB.ExecContext(ctx, "INSERT INTO challenge_results (peer_id, shard_id, piece_index, status) VALUES (?, ?, ?, 'ok')", peerID, job.ShardID, job.PieceIndex)
	
	if e.ChallengesPerPiece > 0 {
		maxOffset := int(job.Size) - 32
		if maxOffset > 0 {
			f, err := os.Open(job.FilePath)
			if err == nil {
				defer f.Close()
				for i := 0; i < e.ChallengesPerPiece; i++ {
					offset := rand.Intn(maxOffset)
					expectedData := make([]byte, 32)
					f.ReadAt(expectedData, int64(offset))
					e.DB.ExecContext(ctx, "INSERT INTO piece_challenges (shard_id, piece_index, peer_id, piece_hash, offset, expected_data) VALUES (?, ?, ?, ?, ?, ?)", job.ShardID, job.PieceIndex, peerID, job.HashHex, offset, expectedData)
				}
			}
		}
	}
	
	if e.Verbose {
		log.Printf("OutboundWorker: Successfully uploaded piece %d of shard %d to peer %d", job.PieceIndex, job.ShardID, peerID)
	}
}

func (e *Engine) checkShardCompletion(ctx context.Context, shardID int64) {
	var isMirrored bool
	_ = e.DB.QueryRowContext(ctx, "SELECT mirrored FROM shards WHERE id = ?", shardID).Scan(&isMirrored)

	if isMirrored {
		return
	}

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
