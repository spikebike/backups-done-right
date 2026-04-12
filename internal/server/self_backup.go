package server

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
	"p2p-backup/internal/crypto"
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

	// 2. Read snapshot
	dbData, err := os.ReadFile(tempSnapshot)
	if err != nil {
		log.Printf("SelfBackupWorker: failed to read snapshot: %v", err)
		return
	}

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
	var buf bytes.Buffer
	enc, _ := zstd.NewWriter(&buf)
	tw := tar.NewWriter(enc)

	if dbData != nil {
		hdr := &tar.Header{Name: "server.db", Size: int64(len(dbData)), Mode: 0644}
		tw.WriteHeader(hdr)
		tw.Write(dbData)
	}

	if peerMapJSON != nil {
		hdr := &tar.Header{Name: "peers.json", Size: int64(len(peerMapJSON)), Mode: 0644}
		tw.WriteHeader(hdr)
		tw.Write(peerMapJSON)
	}

	if e.ConfigPath != "" {
		configData, err := os.ReadFile(e.ConfigPath)
		if err == nil {
			hdr := &tar.Header{Name: "server.yaml", Size: int64(len(configData)), Mode: 0644}
			tw.WriteHeader(hdr)
			tw.Write(configData)
		}
	}

	tw.Close()
	enc.Close()

	bundleLen := buf.Len()

	// Padding Logic: Ensure the final encrypted piece is exactly 256MB.
	// overhead = 24 (nonce) + 16 (tag) = 40 bytes.
	const blockSize = 256 * 1024 * 1024
	const overhead = 40

	targetSize := ((bundleLen + overhead + blockSize - 1) / blockSize) * blockSize
	paddingLen := targetSize - overhead - bundleLen

	bundle := make([]byte, bundleLen+paddingLen)
	copy(bundle[:bundleLen], buf.Bytes())

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

	// 5.5 Sign the ciphertext to prove authenticity
	signature := crypto.SignRecoveryShard(e.MasterKey, ciphertext)
	// Prepend signature (64 bytes)
	signedCiphertext := make([]byte, 64+len(ciphertext))
	copy(signedCiphertext[:64], signature)
	copy(signedCiphertext[64:], ciphertext)
	ciphertext = signedCiphertext

	// 6. Ingest into a Special Dedicated Shard
	blobHash := crypto.Hash(ciphertext)
	blobHashHex := fmt.Sprintf("%x", blobHash)
	nowSeq := uint64(time.Now().Unix())

	e.mu.Lock()
	res, err := e.DB.ExecContext(ctx, "INSERT INTO shards (status, size, sequence, mirrored, total_pieces) VALUES ('sealed', ?, ?, 1, 1)", len(ciphertext), nowSeq)
	if err != nil {
		e.mu.Unlock()
		log.Printf("SelfBackupWorker: failed to create dedicated mirrored shard: %v", err)
		return
	}
	shardID, _ := res.LastInsertId()
	
	_, err = e.DB.ExecContext(ctx, "INSERT OR IGNORE INTO blobs (hash, size, special) VALUES (?, ?, 1)", blobHashHex, len(ciphertext))
	if err != nil {
		e.mu.Unlock()
		log.Printf("SelfBackupWorker: failed to insert blob: %v", err)
		return
	}

	_, err = e.DB.ExecContext(ctx, "INSERT INTO blob_locations (blob_hash, shard_id, piece_index, offset, length, sequence) VALUES (?, ?, 0, 0, ?, 0)", blobHashHex, shardID, len(ciphertext))
	if err != nil {
		e.mu.Unlock()
		log.Printf("SelfBackupWorker: failed to insert blob location: %v", err)
		return
	}
	e.mu.Unlock()

	shardKey := fmt.Sprintf("shard_%d_piece_0", shardID)
	if err := e.BlobStore.Put(ctx, shardKey, bytes.NewReader(ciphertext), int64(len(ciphertext))); err != nil {
		log.Printf("SelfBackupWorker: failed to write shard file: %v", err)
		return
	}

	e.wg.Add(1)
	go e.encodeShard(shardID)

	if e.Verbose {
		log.Printf("SelfBackupWorker: Server database backup completed in dedicated shard %d. Hash: %s", shardID, blobHashHex)
	}
}
