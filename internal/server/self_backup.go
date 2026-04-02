package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
	"p2p-backup/internal/crypto"
	"p2p-backup/internal/rpc"
)

// PeerDiscoveryInfo stores endpoint information for a peer during recovery.
type PeerDiscoveryInfo struct {
	PublicKey string   `json:"pubkey"`
	Endpoints []string `json:"endpoints"`
	Status    string   `json:"status"`
}

// StartSelfBackupWorker periodically snapshots and backups the server's own database.
func (e *Engine) StartSelfBackupWorker(ctx context.Context) {
	if e.SelfBackupIntervalMinutes <= 0 || len(e.MasterKey) == 0 {
		if e.Verbose {
			log.Println("SelfBackupWorker: disabled (no interval or no master key)")
		}
		return
	}

	ticker := time.NewTicker(time.Duration(e.SelfBackupIntervalMinutes) * time.Minute)
	defer ticker.Stop()

	// Run once at startup
	go e.RunSelfBackup(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.RunSelfBackup(ctx)
		}
	}
}

// RunSelfBackup creates a consistent snapshot of the server database, encrypts it, and stores it in a special shard.
func (e *Engine) RunSelfBackup(ctx context.Context) {
	if e.Verbose {
		log.Println("SelfBackupWorker: Starting backup of server database...")
	}

	// 1. Create a consistent snapshot using VACUUM INTO
	tempSnapshot := filepath.Join(os.TempDir(), fmt.Sprintf("server_backup_%d.db", time.Now().Unix()))
	defer os.Remove(tempSnapshot)

	_, err := e.DB.ExecContext(ctx, fmt.Sprintf("VACUUM INTO '%s'", tempSnapshot))
	if err != nil {
		log.Printf("SelfBackupWorker: failed to create snapshot: %v", err)
		return
	}

	// 2. Read and Compress snapshot
	dbData, err := os.ReadFile(tempSnapshot)
	if err != nil {
		log.Printf("SelfBackupWorker: failed to read snapshot: %v", err)
		return
	}

	encoder, _ := zstd.NewWriter(nil)
	compressedDB := encoder.EncodeAll(dbData, nil)

	// 3. Collect Peer discovery info
	rows, err := e.DB.QueryContext(ctx, "SELECT public_key, ip_address, status FROM peers WHERE status != 'blocked'")
	if err != nil {
		log.Printf("SelfBackupWorker: failed to query peers: %v", err)
		return
	}
	defer rows.Close()

	var discoveryList []PeerDiscoveryInfo
	for rows.Next() {
		var pubKey, ip, status string
		if err := rows.Scan(&pubKey, &ip, &status); err == nil {
			discoveryList = append(discoveryList, PeerDiscoveryInfo{
				PublicKey: pubKey,
				Endpoints: []string{ip},
				Status:    status,
			})
		}
	}
	rows.Close()

	peerMapJSON, err := json.Marshal(discoveryList)
	if err != nil {
		log.Printf("SelfBackupWorker: failed to marshal peer map: %v", err)
		return
	}

	// 4. Construct Recovery Bundle
	// Format: [4-byte JSON len][4-byte compressed DB len][JSON][Compressed DB]
	bundleLen := 8 + len(peerMapJSON) + len(compressedDB)
	
	// Padding Logic: Ensure the final encrypted piece is a multiple of 256KB.
	// overhead = 24 (nonce) + 16 (tag) = 40 bytes.
	const blockSize = 256 * 1024
	const overhead = 40
	
	targetSize := ((bundleLen + overhead + blockSize - 1) / blockSize) * blockSize
	paddingLen := targetSize - overhead - bundleLen

	bundle := make([]byte, bundleLen+paddingLen)
	binary.BigEndian.PutUint32(bundle[0:4], uint32(len(peerMapJSON)))
	binary.BigEndian.PutUint32(bundle[4:8], uint32(len(compressedDB)))
	copy(bundle[8:8+len(peerMapJSON)], peerMapJSON)
	copy(bundle[8+len(peerMapJSON):8+len(peerMapJSON)+len(compressedDB)], compressedDB)

	if paddingLen > 0 {
		if _, err := crypto.ReadRand(bundle[bundleLen:]); err != nil {
			log.Printf("SelfBackupWorker: failed to generate padding: %v", err)
			// Non-fatal, we'll just have zero padding
		}
	}

	// 5. Encrypt Bundle
	ciphertext, err := crypto.Encrypt(e.MasterKey, bundle)
	if err != nil {
		log.Printf("SelfBackupWorker: failed to encrypt bundle: %v", err)
		return
	}

	// 6. Ingest into a Special Shard
	blobHash := crypto.Hash(ciphertext)
	blobHashHex := fmt.Sprintf("%x", blobHash)
	nowSeq := uint64(time.Now().Unix())

	blobs := []rpc.LocalBlobData{
		{
			Hash:           blobHashHex,
			Data:           ciphertext,
			IsSpecial:      true,
			SequenceNumber: nowSeq,
		},
	}

	err = e.IngestBlobs(ctx, "system-self-backup", blobs, true)
	if err != nil {
		log.Printf("SelfBackupWorker: failed to ingest backup: %v", err)
		return
	}

	// Find the shard(s) where this blob was stored and mark them as mirrored/special
	_, err = e.DB.ExecContext(ctx, `
		UPDATE shards SET mirrored = 1, sequence = ?
		WHERE id IN (SELECT shard_id FROM blob_locations WHERE blob_hash = ?)
	`, nowSeq, blobHashHex)
	if err != nil {
		log.Printf("SelfBackupWorker: failed to mark shards as special: %v", err)
	}

	if e.Verbose {
		log.Printf("SelfBackupWorker: Server database backup completed. Hash: %s", blobHashHex)
	}
}
