package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"p2p-backup/internal/crypto"
)

// StartAdoptionWorker periodically checks for peers that have completed their testing period
// and issues retrieval challenges to promote them to 'trusted'.
func (e *Engine) StartAdoptionWorker(ctx context.Context) {
	if e.Verbose {
		log.Println("AdoptionWorker: Starting...")
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if e.AdoptionEnabled {
				e.runAdoptionCycle(ctx)
			}
		}
	}
}

func (e *Engine) runAdoptionCycle(ctx context.Context) {
	// Find peers in 'testing' status whose period has elapsed
	query := `
		SELECT id, public_key, adoption_start_at 
		FROM peers 
		WHERE adoption_status = 'testing' 
		  AND datetime(adoption_start_at, '+' || ? || ' minutes') <= CURRENT_TIMESTAMP
	`
	rows, err := e.DB.QueryContext(ctx, query, e.AdoptionPeriodMinutes)
	if err != nil {
		log.Printf("AdoptionWorker: failed to query testing peers: %v", err)
		return
	}
	defer rows.Close()

	type targetPeer struct {
		id        int64
		pubKeyHex string
	}
	var targets []targetPeer
	for rows.Next() {
		var t targetPeer
		var startAt string
		if err := rows.Scan(&t.id, &t.pubKeyHex, &startAt); err == nil {
			targets = append(targets, t)
		}
	}
	rows.Close()

	for _, t := range targets {
		if err := e.verifyAdoption(ctx, t.id); err != nil {
			log.Printf("AdoptionWorker: Peer %d FAILED adoption: %v", t.id, err)
			e.DB.ExecContext(ctx, "UPDATE peers SET adoption_status = 'failed', status = 'discovered' WHERE id = ?", t.id)
		} else {
			if e.Verbose {
				log.Printf("AdoptionWorker: Peer %d PASSED adoption and is now TRUSTED", t.id)
			}
			e.DB.ExecContext(ctx, "UPDATE peers SET adoption_status = 'completed', status = 'trusted' WHERE id = ?", t.id)
		}
	}
}

func (e *Engine) verifyAdoption(ctx context.Context, peerID int64) error {
	// 1. Get all test pieces for this peer
	rows, err := e.DB.QueryContext(ctx, "SELECT hash, size FROM hosted_shards WHERE peer_id = ? AND is_special = 2", peerID)
	if err != nil {
		return err
	}
	defer rows.Close()

	type testPiece struct {
		hash string
		size int64
	}
	var pieces []testPiece
	for rows.Next() {
		var p testPiece
		if err := rows.Scan(&p.hash, &p.size); err == nil {
			pieces = append(pieces, p)
		}
	}
	rows.Close()

	if len(pieces) == 0 {
		return fmt.Errorf("no test pieces found for peer %d", peerID)
	}

	var pubKeyHex string
	err = e.DB.QueryRowContext(ctx, "SELECT public_key FROM peers WHERE id = ?", peerID).Scan(&pubKeyHex)
	if err != nil {
		return fmt.Errorf("failed to get pubkey for peer: %w", err)
	}
	pid, err := crypto.PeerIDFromPubKeyHex(pubKeyHex)
	if err != nil {
		return fmt.Errorf("failed to get peer.ID: %w", err)
	}

	for _, p := range pieces {
		if e.Verbose {
			log.Printf("AdoptionWorker: Requesting retrieval of test piece %s from peer %d...", p.hash[:12], peerID)
		}

		err := e.PullPieceRaw(ctx, pid, p.hash, io.Discard)
		if err != nil {
			return fmt.Errorf("pull error for %s: %w", p.hash, err)
		}
	}

	return nil
}
