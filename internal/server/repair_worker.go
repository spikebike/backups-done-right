package server

import (
	"context"
	"fmt"
	"log"
	"time"
)

// StartRepairWorker periodically checks the health of the swarm and restores redundancy.
func (e *Engine) StartRepairWorker(ctx context.Context) {
	// Repair check every 15 minutes
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.runRepairCycle(ctx)
		}
	}
}

// TriggerRepair manually triggers a swarm health check and repair cycle.
func (e *Engine) TriggerRepair(ctx context.Context) {
	e.runRepairCycle(ctx)
}

func (e *Engine) runRepairCycle(ctx context.Context) {
	if e.Verbose {
		log.Println("RepairWorker: Starting swarm health check...")
	}

	// 1. Identify "Dead" Peers and mark their pieces as lost.
	// A peer is dead if it hasn't been seen for PeerEvictionHours.
	_, err := e.DB.ExecContext(ctx, `
		UPDATE outbound_pieces 
		SET status = 'lost' 
		WHERE status = 'uploaded' 
		AND peer_id IN (
			SELECT id FROM peers 
			WHERE last_seen < datetime('now', '-' || ? || ' hours')
			AND status != 'blocked'
		)
	`, e.PeerEvictionHours)
	if err != nil {
		log.Printf("RepairWorker: failed to evict dead peers: %v", err)
	}

	// 2. Identify shards that need repair.
	// A shard needs repair if it has fewer than (DataShards + ParityShards) healthy uploaded pieces.
	targetRedundancy := e.DataShards + e.ParityShards
	
	// We only repair 'sealed' shards. 'open' shards are still being filled.
	rows, err := e.DB.QueryContext(ctx, `
		SELECT s.id, s.mirrored, COUNT(op.shard_id) as healthy_pieces
		FROM shards s
		LEFT JOIN outbound_pieces op ON s.id = op.shard_id AND op.status = 'uploaded'
		WHERE s.status = 'sealed'
		GROUP BY s.id
		HAVING healthy_pieces < ?
	`, targetRedundancy)
	if err != nil {
		log.Printf("RepairWorker: failed to query unhealthy shards: %v", err)
		return
	}
	defer rows.Close()

	type repairJob struct {
		id         int64
		isMirrored bool
	}
	var jobs []repairJob
	for rows.Next() {
		var j repairJob
		var healthyCount int
		if err := rows.Scan(&j.id, &j.isMirrored, &healthyCount); err == nil {
			// For mirrored shards, the target is "all peers". 
			// We handle them slightly differently in the repair logic.
			jobs = append(jobs, j)
		}
	}
	rows.Close()

	if len(jobs) > 0 {
		log.Printf("RepairWorker: Found %d shards requiring redundancy restoration.", len(jobs))
	}

	for _, job := range jobs {
		if err := e.repairShard(ctx, job.id, job.isMirrored); err != nil {
			log.Printf("RepairWorker: Failed to repair shard %d: %v", job.id, err)
		}
	}
}

func (e *Engine) repairShard(ctx context.Context, shardID int64, isMirrored bool) error {
	if e.Verbose {
		log.Printf("RepairWorker: Repairing shard %d (mirrored=%v)...", shardID, isMirrored)
	}

	// 1. Ensure the full shard is available locally.
	// This will use EnsureShardLocal which handles both "already local" and "reconstruct from swarm" cases.
	if err := e.EnsureShardLocal(ctx, shardID); err != nil {
		return fmt.Errorf("ensure local: %w", err)
	}

	// 2. Re-trigger the encoding process.
	// This will re-generate all piece files in the queue directory.
	// Our OutboundWorker will then see these files and offer them to peers who don't have them.
	// We run this synchronously here to ensure the files are ready.
	e.encodeShard(shardID)

	if e.Verbose {
		log.Printf("RepairWorker: Successfully enqueued repair pieces for shard %d", shardID)
	}
	return nil
}
