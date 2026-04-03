package server

import (
	"context"
	"crypto/x509"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/reedsolomon"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/time/rate"
	"lukechampine.com/blake3"
	"p2p-backup/internal/config"
	"p2p-backup/internal/crypto"
	"p2p-backup/internal/rpc"
)

// Engine handles the core server logic for receiving and storing blobs.
type Engine struct {
	DB                         *sql.DB
	SQLitePath                 string
	BlobStoreDir               string
	QueueDir                   string
	DataShards                 int
	ParityShards               int
	Verbose                    bool
	ExtraVerbose               bool
	ShardSize                  int64
	Host                       host.Host
	ListenAddress              string
	KeepLocalCopy              bool
	KeepDeletedMinutes         int
	KeepMetadataMinutes        int
	WasteThreshold             float64
	GCIntervalMinutes          int
	SelfBackupIntervalMinutes  int
	PeerEvictionHours          int
	MaxStorageBytes            int64
	UntrustedPeerUploadLimitMB int64
	BasePieceBuffer            int
	ChallengesPerPiece         int
	AdminPublicKey             string
	ContactInfo                string
	// Bandwidth Throttling
	UploadLimiter   *rate.Limiter
	DownloadLimiter *rate.Limiter
	// Peer Management
	LocalPeerNode   rpc.PeerNode
	ActivePeers     map[int64]rpc.PeerNode
	ActivePeersMu   sync.RWMutex
	StartTime       time.Time
	MasterKey                  []byte
	mu                         sync.Mutex // Protects pendingBlobs and shard writing
	pendingBlobs    map[string]rpc.BlobMeta
	wg              sync.WaitGroup
	streamSemaphore chan struct{}
	pendingInboundStreams sync.Map
}

// NewEngine creates a new server Engine.
func NewEngine(db *sql.DB, sqlitePath string, blobStoreDir, queueDir string, dataShards, parityShards int, shardSize int64, keepLocalCopy bool, p2pHost host.Host, listenAddress string, untrustedLimitMB int, verbose bool, extraVerbose bool, challengesPerPiece int, keepDeletedMinutes int, keepMetadataMinutes int, wasteThreshold float64, gcIntervalMinutes int, selfBackupIntervalMinutes int, peerEvictionHours int, basePieceBuffer int, maxStorageGB int, maxUploadKBPS int, maxDownloadKBPS int, masterKey []byte, adminPublicKey string, contactInfo string, maxConcurrentStreams int) *Engine {
	if untrustedLimitMB <= 0 {
		untrustedLimitMB = 1024
	}
	if shardSize <= 0 {
		shardSize = int64(dataShards) * 256 * 1024 * 1024 // Default piece size to 256MB
	}
	if maxConcurrentStreams <= 0 {
		maxConcurrentStreams = 4 // Default to 4 concurrent streams
	}
	if dataShards <= 0 {
		dataShards = 10
	}
	if parityShards <= 0 {
		parityShards = 4
	}
	if challengesPerPiece <= 0 {
		challengesPerPiece = 8
	}
	if wasteThreshold <= 0 {
		wasteThreshold = 0.5
	}
	if gcIntervalMinutes <= 0 {
		gcIntervalMinutes = 720 // 12 hours
	}
	if selfBackupIntervalMinutes <= 0 {
		selfBackupIntervalMinutes = 1440 // 24 hours
	}
	if peerEvictionHours <= 0 {
		peerEvictionHours = 24 // 24 hours default
	}
	if basePieceBuffer <= 0 {
		basePieceBuffer = 4 // Default 4 pieces buffer
	}
	if keepMetadataMinutes <= 0 {
		keepMetadataMinutes = 60 * 24 * 7 // 7 days default
	}

	var maxBytes int64
	if maxStorageGB > 0 {
		maxBytes = int64(maxStorageGB) * 1024 * 1024 * 1024
	}

	var uploadLimiter, downloadLimiter *rate.Limiter
	if maxUploadKBPS > 0 {
		uploadLimiter = rate.NewLimiter(rate.Limit(maxUploadKBPS*1024), maxUploadKBPS*1024)
	}
	if maxDownloadKBPS > 0 {
		downloadLimiter = rate.NewLimiter(rate.Limit(maxDownloadKBPS*1024), maxDownloadKBPS*1024)
	}

	return &Engine{
		DB:                         db,
		SQLitePath:                 sqlitePath,
		ChallengesPerPiece:         challengesPerPiece,
		ContactInfo:                contactInfo,
		BlobStoreDir:               blobStoreDir,
		QueueDir:                   queueDir,
		DataShards:                 dataShards,
		ParityShards:               parityShards,
		Verbose:                    verbose,
		ExtraVerbose:               extraVerbose,
		ShardSize:                  shardSize,
		Host:                       p2pHost,
		ListenAddress:              listenAddress,
		KeepLocalCopy:              keepLocalCopy,
		KeepDeletedMinutes:         keepDeletedMinutes,
		KeepMetadataMinutes:        keepMetadataMinutes,
		WasteThreshold:             wasteThreshold,
		GCIntervalMinutes:          gcIntervalMinutes,
		SelfBackupIntervalMinutes:  selfBackupIntervalMinutes,
		PeerEvictionHours:          peerEvictionHours,
		MaxStorageBytes:            maxBytes,
		BasePieceBuffer:            basePieceBuffer,
		UploadLimiter:              uploadLimiter,
		DownloadLimiter:            downloadLimiter,
		UntrustedPeerUploadLimitMB: int64(untrustedLimitMB),
		ActivePeers:                make(map[int64]rpc.PeerNode),
		StartTime:                  time.Now(),
		MasterKey:                  masterKey,
		AdminPublicKey:             adminPublicKey,
		pendingBlobs:               make(map[string]rpc.BlobMeta),
		streamSemaphore:            make(chan struct{}, maxConcurrentStreams),
	}
}

// Wait blocks until all background tasks (like encoding) are finished.
func (e *Engine) Wait() {
	e.wg.Wait()
}

// AttemptRescue attempts to recover the server database from a peer that has a mirrored copy of the special metadata shard.
func (e *Engine) AttemptRescue(ctx context.Context, peer rpc.PeerNode, pid peer.ID) error {
	if e.Verbose {
		log.Println("RESCUE: Attempting to recover database from peer...")
	}

	// 1. Get special pieces from peer
	req, release := peer.ListSpecialPieces(ctx, nil)
	defer release()
	res, err := req.Struct()
	if err != nil {
		return fmt.Errorf("list special pieces: %w", err)
	}
	shards, _ := res.Shards()
	if shards.Len() == 0 {
		return fmt.Errorf("no special pieces found on peer")
	}

	// 2. Identify the newest mirrored shard
	var highestSeq uint64
	var targetPiece rpc.PeerShardMetadata
	for i := 0; i < shards.Len(); i++ {
		s := shards.At(i)
		if s.IsSpecial() && s.SequenceNumber() > highestSeq {
			highestSeq = s.SequenceNumber()
			targetPiece = s
		}
	}

	if highestSeq == 0 {
		return fmt.Errorf("no special mirrored shards found")
	}

	// 3. Download the piece
	hashBytes, _ := targetPiece.Checksum()
	hashHex := hex.EncodeToString(hashBytes)

	data, err := e.PullPieceDirect(ctx, pid, hashHex)
	if err != nil {
		return fmt.Errorf("pull piece direct: %w", err)
	}

	if e.Verbose {
		log.Printf("RESCUE: Downloaded %d bytes from peer", len(data))
	}

	// Trim trailing zeros added by shard padding
	// AEAD tags are random, so they are extremely unlikely to end in many zeros.
	actualData := data
	for len(actualData) > 0 && actualData[len(actualData)-1] == 0 {
		actualData = actualData[:len(actualData)-1]
	}

	if e.Verbose {
		log.Printf("RESCUE: Trimmed data length: %d", len(actualData))
	}

	// 4. Decrypt Bundle
	decryptedBundle, err := crypto.Decrypt(e.MasterKey, actualData)
	if err != nil {
		return fmt.Errorf("bundle decryption failed (wrong mnemonic or corruption): %w", err)
	}

	// 5. Unpack Bundle
	if len(decryptedBundle) < 8 {
		return fmt.Errorf("invalid bundle size")
	}
	jsonLen := binary.BigEndian.Uint32(decryptedBundle[0:4])
	dbLen := binary.BigEndian.Uint32(decryptedBundle[4:8])
	
	if uint32(len(decryptedBundle)) < 8+jsonLen+dbLen {
		return fmt.Errorf("bundle truncated (len=%d, expected %d)", len(decryptedBundle), 8+jsonLen+dbLen)
	}
	
	// peerJSON := decryptedBundle[8 : 8+jsonLen]
	compressedDB := decryptedBundle[8+jsonLen : 8+jsonLen+dbLen]

	// 6. Decompress and Save DB
	decoder, _ := zstd.NewReader(nil)
	defer decoder.Close()
	
	dbData, err := decoder.DecodeAll(compressedDB, nil)
	if err != nil {
		return fmt.Errorf("decompression failed: %w", err)
	}

	// Close current DB handle before overwriting
	if e.DB != nil {
		e.DB.Close()
	}

	if err := os.WriteFile(e.SQLitePath, dbData, 0644); err != nil {
		return fmt.Errorf("failed to write recovered database: %w", err)
	}

	// Re-open DB
	newDB, err := InitDB(e.SQLitePath)
	if err != nil {
		return fmt.Errorf("failed to re-open database after recovery: %w", err)
	}
	e.DB = newDB

	if e.Verbose {
		log.Printf("RESCUE: SUCCESS! Recovered database to %s", e.SQLitePath)
	}
	return nil
}


// AuthorizeAndCheckQuota verifies that a client is trusted and has enough quota.
func (e *Engine) AuthorizeAndCheckQuota(ctx context.Context, pubKeyHex string, incomingBytes int64) error {
	if pubKeyHex == "insecure-local-client" {
		return nil // Always allow insecure local testing
	}

	// Check if the caller is a known Peer (peers are allowed to backup their DBs to us)
	var isPeer bool
	err := e.DB.QueryRowContext(ctx, "SELECT 1 FROM peers WHERE public_key = ?", pubKeyHex).Scan(&isPeer)
	if err == nil {
		return nil // Peer is authorized (constrained by untrusted_peer_upload_limit during shard offer)
	}

	status, quota, current, err := e.AuthorizeClient(ctx, pubKeyHex)
	if err != nil {
		return err
	}

	if status != "trusted" {
		return fmt.Errorf("client %s is not trusted (status: %s)", pubKeyHex[:16], status)
	}

	if current+incomingBytes > quota {
		return fmt.Errorf("client quota exceeded: %d/%d MB", (current+incomingBytes)/(1024*1024), quota/(1024*1024))
	}

	return nil
}

// OfferBlobs checks which of the offered blobs the server already has.
// It returns a list of indices of the blobs that are missing and need to be uploaded.
func (e *Engine) OfferBlobs(ctx context.Context, clientPubKey string, blobs []rpc.BlobMeta) ([]uint32, error) {
	var totalSize int64
	for _, b := range blobs {
		totalSize += b.Size
	}

	if err := e.AuthorizeAndCheckQuota(ctx, clientPubKey, totalSize); err != nil {
		return nil, err
	}

	var neededIndices []uint32

	stmtCheck, err := e.DB.PrepareContext(ctx, "SELECT 1 FROM blobs WHERE hash = ?")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare check statement: %w", err)
	}
	defer stmtCheck.Close()

	stmtInc, err := e.DB.PrepareContext(ctx, "UPDATE blobs SET ref_count = ref_count + 1, deleted_at = NULL WHERE hash = ?")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare increment statement: %w", err)
	}
	defer stmtInc.Close()

	e.mu.Lock()
	defer e.mu.Unlock()

	for i, blob := range blobs {
		var exists int
		err := stmtCheck.QueryRowContext(ctx, blob.Hash).Scan(&exists)
		if err == sql.ErrNoRows {
			// We don't have this blob, request it
			neededIndices = append(neededIndices, uint32(i))
			// Store metadata in memory so we know it's special when uploaded
			e.pendingBlobs[blob.Hash] = blob
		} else if err != nil {
			log.Printf("Error checking blob existence %s: %v", blob.Hash, err)
			// Safest to request it if there's an error
			neededIndices = append(neededIndices, uint32(i))
			e.pendingBlobs[blob.Hash] = blob
		} else {
			// Blob exists, increment its reference count safely
			// And un-delete it if it was previously marked for deletion.
			if _, err := stmtInc.ExecContext(ctx, blob.Hash); err != nil {
				log.Printf("Failed to increment ref_count for blob %s: %v", blob.Hash, err)
			}
		}
	}

	if e.Verbose {
		log.Printf("Offered %d blobs, requesting %d missing blobs", len(blobs), len(neededIndices))
	}

	return neededIndices, nil
}

// Hash returns a BLAKE3 hash of the data.
func (e *Engine) Hash(data []byte) []byte {
	return crypto.Hash(data)
}

// EnsureShardLocal ensures that a shard file exists on the local disk.
// If the file is missing, it attempts to reconstruct it from the swarm.
func (e *Engine) EnsureShardLocal(ctx context.Context, shardID int64) error {
	shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d.dat", shardID))
	
	// 1. Check if already local
	if _, err := os.Stat(shardPath); err == nil {
		return nil
	}

	if e.Verbose {
		log.Printf("EnsureShardLocal: Shard %d missing locally. Attempting swarm reconstruction.", shardID)
	}

	// 2. Find which peers have which pieces
	rows, err := e.DB.QueryContext(ctx, "SELECT piece_index, peer_id FROM outbound_pieces WHERE shard_id = ? AND status = 'uploaded'", shardID)
	if err != nil {
		return err
	}
	defer rows.Close()

	type pieceInfo struct {
		index  int
		peerID int64
	}
	var pieces []pieceInfo
	for rows.Next() {
		var p pieceInfo
		if err := rows.Scan(&p.index, &p.peerID); err == nil {
			pieces = append(pieces, p)
		}
	}
	rows.Close()

	// Special case: check if it's a mirrored shard
	var isMirrored bool
	_ = e.DB.QueryRowContext(ctx, "SELECT mirrored FROM shards WHERE id = ?", shardID).Scan(&isMirrored)

	if isMirrored {
		// For mirrored shards, any single piece 0 is the full shard.
		for _, p := range pieces {
			if p.index != 0 {
				continue
			}
			
			var pieceHash string
			_ = e.DB.QueryRowContext(ctx, "SELECT piece_hash FROM piece_challenges WHERE shard_id = ? AND piece_index = 0 AND peer_id = ? LIMIT 1", shardID, p.peerID).Scan(&pieceHash)
			
			data, err := e.PullPiece(ctx, p.peerID, pieceHash)
			if err != nil {
				continue
			}

			// Verify hash
			if hex.EncodeToString(e.Hash(data)) == pieceHash {
				return os.WriteFile(shardPath, data, 0644)
			}
		}
		return fmt.Errorf("failed to recover mirrored special shard %d", shardID)
	}

	// 3. RS Reconstruction for standard shards
	if len(pieces) < e.DataShards {
		return fmt.Errorf("insufficient pieces available to reconstruct shard %d (have %d, need %d)", shardID, len(pieces), e.DataShards)
	}

	shards := make([][]byte, e.DataShards+e.ParityShards)
	piecesFound := 0

	for _, p := range pieces {
		if piecesFound >= e.DataShards {
			break
		}

		var pieceHash string
		err := e.DB.QueryRowContext(ctx, "SELECT piece_hash FROM piece_challenges WHERE shard_id = ? AND piece_index = ? AND peer_id = ? LIMIT 1", shardID, p.index, p.peerID).Scan(&pieceHash)
		if err != nil {
			continue
		}

		data, err := e.PullPiece(ctx, p.peerID, pieceHash)
		if err != nil {
			log.Printf("EnsureShardLocal: failed to download piece %d from peer %d: %v", p.index, p.peerID, err)
			continue
		}

		// Verify hash
		if hex.EncodeToString(e.Hash(data)) != pieceHash {
			log.Printf("EnsureShardLocal: hash mismatch for piece %d from peer %d", p.index, p.peerID)
			continue
		}

		shards[p.index] = data
		piecesFound++
	}

	if piecesFound < e.DataShards {
		return fmt.Errorf("failed to download enough pieces for reconstruction (have %d, need %d)", piecesFound, e.DataShards)
	}

	// Reconstruct
	rs, err := reedsolomon.New(e.DataShards, e.ParityShards)
	if err != nil {
		return err
	}

	if err := rs.Reconstruct(shards); err != nil {
		return fmt.Errorf("RS reconstruction failed: %w", err)
	}

	// Save reconstructed shard back to disk
	outFile, err := os.Create(shardPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// Assuming fixed piece size based on shard history
	pieceSize := len(shards[0])
	return rs.Join(outFile, shards, int(int64(e.DataShards)*int64(pieceSize)))
}

func hexToBytes(h string) []byte {
	b, _ := hex.DecodeString(h)
	return b
}

// UploadBlobs receives the actual blob data and saves it to disk within shards.
func (e *Engine) UploadBlobs(ctx context.Context, clientPubKey string, blobs []rpc.LocalBlobData) error {
	var totalSize int64
	for _, b := range blobs {
		totalSize += int64(len(b.Data))
	}

	if err := e.AuthorizeAndCheckQuota(ctx, clientPubKey, totalSize); err != nil {
		return err
	}

	return e.IngestBlobs(ctx, clientPubKey, blobs, false)
}

// IngestBlobs handles the actual saving of blob data into shards.
// If isGC is true, it skips checksum verification and pendingBlobs cleanup.
func (e *Engine) IngestBlobs(ctx context.Context, clientPubKey string, blobs []rpc.LocalBlobData, isGC bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Get the current total disk usage (local shards + peer shards)
	var localTotal, peerTotal int64
	_ = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(size), 0) FROM shards").Scan(&localTotal)
	_ = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(size), 0) FROM hosted_shards").Scan(&peerTotal)
	
	incomingSize := int64(0)
	for _, b := range blobs {
		incomingSize += int64(len(b.Data))
	}

	if e.MaxStorageBytes > 0 && (localTotal + peerTotal + incomingSize) > e.MaxStorageBytes {
		return fmt.Errorf("global server storage limit exceeded (%d GB)", e.MaxStorageBytes / (1024*1024*1024))
	}

	// Get the current open shard, or create one
	var activeShardID int64
	var activeShardSize int64
	err = tx.QueryRowContext(ctx, "SELECT id, size FROM shards WHERE status = 'open' ORDER BY id ASC LIMIT 1").Scan(&activeShardID, &activeShardSize)
	if err == sql.ErrNoRows {
		res, err := tx.ExecContext(ctx, "INSERT INTO shards (status, size, sequence) VALUES ('open', 0, ?)", time.Now().Unix())
		if err != nil {
			return fmt.Errorf("failed to create initial shard: %w", err)
		}
		activeShardID, _ = res.LastInsertId()
		activeShardSize = 0
	} else if err != nil {
		return fmt.Errorf("failed to query open shards: %w", err)
	}

	stmtInsertBlob, err := tx.PrepareContext(ctx, "INSERT OR IGNORE INTO blobs (hash, size, special) VALUES (?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare insert blob statement: %w", err)
	}
	defer stmtInsertBlob.Close()

	stmtInsertLoc, err := tx.PrepareContext(ctx, "INSERT INTO blob_locations (blob_hash, shard_id, offset, length, sequence) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare insert location statement: %w", err)
	}
	defer stmtInsertLoc.Close()

	stmtUpdateShard, err := tx.PrepareContext(ctx, "UPDATE shards SET size = ?, status = ? WHERE id = ?")
	if err != nil {
		return fmt.Errorf("failed to prepare update shard statement: %w", err)
	}
	defer stmtUpdateShard.Close()

	anySpecial := false
	// Track shards sealed during this ingest to trigger encoding after commit
	var sealedShards []int64

	// 1. Detect if this batch contains any Special blobs
	batchMetas := make([]rpc.BlobMeta, 0, len(blobs))
	var ingestBlobs []rpc.LocalBlobData

	for _, blob := range blobs {
		if !isGC {
			// Check if already ingested (race condition protection)
			var exists int
			err := tx.QueryRowContext(ctx, "SELECT 1 FROM blobs WHERE hash = ?", blob.Hash).Scan(&exists)
			if err == nil {
				// Blob already exists, skip ingestion but update metadata if needed
				if e.Verbose {
					log.Printf("IngestBlobs: skipping blob %s, already ingested", blob.Hash)
				}
				continue
			}

			calculatedHash := hex.EncodeToString(crypto.Hash(blob.Data))
			if calculatedHash != blob.Hash {
				log.Printf("WARNING: Checksum mismatch for blob! claimed=%s, actual=%s", blob.Hash, calculatedHash)
				continue
			} else if e.Verbose {
				log.Printf("Successfully verified checksum for blob %s", blob.Hash)
			}
		}

		// Retrieve metadata from pendingBlobs
		meta, ok := e.pendingBlobs[blob.Hash]
		if !ok {
			var special bool
			err := tx.QueryRowContext(ctx, "SELECT special FROM blobs WHERE hash = ?", blob.Hash).Scan(&special)
			if err == nil {
				meta = rpc.BlobMeta{Hash: blob.Hash, Size: int64(len(blob.Data)), Special: special}
			} else {
				meta = rpc.BlobMeta{Hash: blob.Hash, Size: int64(len(blob.Data)), Special: blob.IsSpecial}
			}
		}
		
		batchMetas = append(batchMetas, meta)
		ingestBlobs = append(ingestBlobs, blob)
		if meta.Special {
			anySpecial = true
		}
	}

	isSystemMirrored := (clientPubKey == "system-self-backup")

	// 2. If this is a SYSTEM special batch (Rescue Bundle), isolate it by sealing the current shard
	if isSystemMirrored && anySpecial && activeShardSize > 0 {
		totalPieces := e.DataShards + e.ParityShards
		if _, err := tx.ExecContext(ctx, "UPDATE shards SET size = ?, status = 'sealed', total_pieces = ? WHERE id = ?", activeShardSize, totalPieces, activeShardID); err != nil {
			return fmt.Errorf("failed to seal previous shard for system rescue: %w", err)
		}
		sealedShards = append(sealedShards, activeShardID)

		// Open a new shard for the system rescue bundle
		res, err := tx.ExecContext(ctx, "INSERT INTO shards (status, size, sequence) VALUES ('open', 0, ?)", time.Now().Unix())
		if err != nil {
			return fmt.Errorf("failed to create new shard for system rescue: %w", err)
		}
		activeShardID, _ = res.LastInsertId()
		activeShardSize = 0
	}

	for i, blob := range ingestBlobs {
		meta := batchMetas[i]

		// Update DB blobs table
		_, err = stmtInsertBlob.ExecContext(ctx, blob.Hash, len(blob.Data), meta.Special)
		if err != nil {
			return fmt.Errorf("failed to insert blob %s into db: %w", blob.Hash, err)
		}

		remaining := int64(len(blob.Data))
		dataOffset := int64(0)
		sequence := 0

		for remaining > 0 {
			if activeShardSize >= e.ShardSize {
				// Mark current shard as sealed
				totalPieces := e.DataShards + e.ParityShards
				if _, err := tx.ExecContext(ctx, "UPDATE shards SET size = ?, status = 'sealed', total_pieces = ? WHERE id = ?", activeShardSize, totalPieces, activeShardID); err != nil {
					return fmt.Errorf("failed to seal shard %d: %w", activeShardID, err)
				}
				sealedShards = append(sealedShards, activeShardID)

				// Open a new shard
				res, err := tx.ExecContext(ctx, "INSERT INTO shards (status, size, sequence) VALUES ('open', 0, ?)", time.Now().Unix())
				if err != nil {
					return fmt.Errorf("failed to create new shard: %w", err)
				}
				activeShardID, _ = res.LastInsertId()
				activeShardSize = 0
			}

			spaceInShard := e.ShardSize - activeShardSize
			toWrite := remaining
			if toWrite > spaceInShard {
				toWrite = spaceInShard
			}

			// Open shard file, seek to end, write
			shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d.dat", activeShardID))
			f, err := os.OpenFile(shardPath, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("failed to open shard file %s: %w", shardPath, err)
			}

			if _, err := f.Seek(activeShardSize, 0); err != nil {
				f.Close()
				return fmt.Errorf("failed to seek shard file %s: %w", shardPath, err)
			}

			if _, err := f.Write(blob.Data[dataOffset : dataOffset+toWrite]); err != nil {
				f.Close()
				return fmt.Errorf("failed to write to shard file %s: %w", shardPath, err)
			}
			f.Close()

			// Insert location
			if _, err := stmtInsertLoc.ExecContext(ctx, blob.Hash, activeShardID, activeShardSize, toWrite, sequence); err != nil {
				return fmt.Errorf("failed to insert blob location for %s: %w", blob.Hash, err)
			}

			activeShardSize += toWrite
			remaining -= toWrite
			dataOffset += toWrite
			sequence++
		}

		// Update active shard size in DB
		if _, err := stmtUpdateShard.ExecContext(ctx, activeShardSize, "open", activeShardID); err != nil {
			return fmt.Errorf("failed to update open shard size %d: %w", activeShardID, err)
		}

		// Remove from pending
		if !isGC {
			delete(e.pendingBlobs, blob.Hash)
		}
	}

	// 3. If this was a SYSTEM special batch, seal it immediately for mirroring
	if isSystemMirrored && anySpecial {
		if e.Verbose {
			log.Printf("IngestBlobs: Sealing system rescue shard %d (mirrored=true).", activeShardID)
		}
		
		// For mirrored shards, we always use 1 piece (the full shard)
		totalPieces := 1

		// Update status to sealed and set mirrored flag
		if _, err := tx.ExecContext(ctx, "UPDATE shards SET status = 'sealed', mirrored = 1, total_pieces = ?, size = ? WHERE id = ?", totalPieces, activeShardSize, activeShardID); err != nil {
			return fmt.Errorf("failed to force seal system rescue shard %d: %w", activeShardID, err)
		}
		sealedShards = append(sealedShards, activeShardID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// 7. Post-commit: Trigger Encoders
	for _, sid := range sealedShards {
		e.wg.Add(1)
		go e.encodeShard(sid)
	}

	// Update client storage usage
	if !isGC && clientPubKey != "" && clientPubKey != "insecure-local-client" {
		var totalUploaded int64
		for i := range ingestBlobs {
			totalUploaded += int64(len(ingestBlobs[i].Data))
		}
		_, _ = e.DB.ExecContext(ctx, "UPDATE clients SET current_storage_size = current_storage_size + ? WHERE public_key = ?", totalUploaded, clientPubKey)
	}

	if e.Verbose {
		log.Printf("Successfully saved %d blobs to shards (isGC=%v)", len(blobs), isGC)
	}

	return nil
}

// GetBlobs retrieves requested blobs by checksum from the shard files.
func (e *Engine) GetBlobs(ctx context.Context, hashes []string) ([]rpc.LocalBlobData, []string, error) {
	var foundBlobs []rpc.LocalBlobData
	var missingBlobs []string

	// Prepared statement to find all locations for a blob
	stmt, err := e.DB.PrepareContext(ctx, "SELECT shard_id, offset, length FROM blob_locations WHERE blob_hash = ? ORDER BY sequence ASC")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare blob location query: %w", err)
	}
	defer stmt.Close()

	for _, hash := range hashes {
		rows, err := stmt.QueryContext(ctx, hash)
		if err != nil {
			missingBlobs = append(missingBlobs, hash)
			continue
		}

		var blobData []byte
		var foundParts int

		for rows.Next() {
			var shardID int64
			var offset, length int64
			if err := rows.Scan(&shardID, &offset, &length); err != nil {
				continue
			}

			// Ensure shard is local (reconstruct if missing)
			if err := e.EnsureShardLocal(ctx, shardID); err != nil {
				log.Printf("GetBlobs: failed to ensure shard %d is local: %v", shardID, err)
				continue
			}

			shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d.dat", shardID))
			f, err := os.Open(shardPath)
			if err != nil {
				log.Printf("Error opening shard %d: %v", shardID, err)
				continue
			}

			buf := make([]byte, length)
			_, err = f.ReadAt(buf, offset)
			f.Close()

			if err != nil {
				log.Printf("CRITICAL DEBUG: Error reading from shard %d (offset %d, requested %d): %v", shardID, offset, length, err)
				continue
			}

			blobData = append(blobData, buf...)
			foundParts++
		}
		rows.Close()

		if foundParts > 0 {
			foundBlobs = append(foundBlobs, rpc.LocalBlobData{
				Hash: hash,
				Data: blobData,
			})
		} else {
			missingBlobs = append(missingBlobs, hash)
		}
	}

	return foundBlobs, missingBlobs, nil
}

// ListSpecialBlobs returns a list of all special blobs (e.g., encrypted SQLite DBs) for disaster recovery.
func (e *Engine) ListSpecialBlobs(ctx context.Context) ([]rpc.BlobMeta, error) {
	query := `
		SELECT b.hash, b.size, MAX(s.sequence)
		FROM blobs b
		JOIN blob_locations bl ON b.hash = bl.blob_hash
		JOIN shards s ON bl.shard_id = s.id
		WHERE b.special = 1
		GROUP BY b.hash, b.size
		ORDER BY MAX(s.sequence) DESC
	`
	rows, err := e.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query special blobs: %w", err)
	}
	defer rows.Close()

	var specialBlobs []rpc.BlobMeta
	for rows.Next() {
		var b rpc.BlobMeta
		if err := rows.Scan(&b.Hash, &b.Size, &b.SequenceNumber); err != nil {
			return nil, fmt.Errorf("failed to scan special blob: %w", err)
		}
		b.Special = true
		specialBlobs = append(specialBlobs, b)
	}

	return specialBlobs, nil
}

// encodeShard applies Reed-Solomon erasure coding to a sealed shard.
// PadReader wraps an io.Reader and pads the output with zeros until it reaches a specific size.
type PadReader struct {
	Reader    io.Reader
	TotalSize int64
	readSoFar int64
}

func (p *PadReader) Read(buf []byte) (int, error) {
	if p.readSoFar >= p.TotalSize {
		return 0, io.EOF
	}

	n, err := p.Reader.Read(buf)
	if n > 0 {
		p.readSoFar += int64(n)
	}

	// If we hit EOF but haven't reached TotalSize, start padding
	if err == io.EOF && p.readSoFar < p.TotalSize {
		paddingNeeded := int(p.TotalSize - p.readSoFar)
		if paddingNeeded > len(buf)-n {
			paddingNeeded = len(buf) - n
		}
		for i := 0; i < paddingNeeded; i++ {
			buf[n+i] = 0
		}
		p.readSoFar += int64(paddingNeeded)
		return n + paddingNeeded, nil
	}

	return n, err
}

func (e *Engine) encodeShard(shardID int64) {
	defer e.wg.Done()
	if e.Verbose {
		log.Printf("Starting erasure coding for shard %d", shardID)
	}

	shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d.dat", shardID))
	inFile, err := os.Open(shardPath)
	if err != nil {
		log.Printf("encodeShard error: failed to open shard %d: %v", shardID, err)
		return
	}
	defer inFile.Close()

	_, err = inFile.Stat()
	if err != nil {
		log.Printf("encodeShard error: failed to stat shard %d: %v", shardID, err)
		return
	}

	// Calculate target piece size
	targetPieceSize := e.ShardSize / int64(e.DataShards)

	// Check if this is a mirrored (metadata) shard
	var isMirrored bool
	err = e.DB.QueryRow("SELECT mirrored FROM shards WHERE id = ?", shardID).Scan(&isMirrored)
	if err != nil {
		log.Printf("encodeShard error: failed to check mirrored status: %v", err)
		return
	}

	if isMirrored {
		// Mirroring profile: Every peer gets a full copy of the shard data,
		// padded to the target piece size (e.g. 256MB).
		if e.Verbose {
			log.Printf("encodeShard: Shard %d is MIRRORED. Padded to %d MB piece.", shardID, targetPieceSize/(1024*1024))
		}
		
		// For special shards, Piece 0 is just a copy of the shard itself, padded to targetPieceSize.
		piecePath := filepath.Join(e.QueueDir, fmt.Sprintf("shard_%d_piece_0", shardID))
		dst, err := os.Create(piecePath)
		if err != nil {
			log.Printf("encodeShard error: failed to create mirrored piece: %v", err)
			return
		}

		// Use PadReader to ensure the piece is exactly targetPieceSize
		pr := &PadReader{Reader: inFile, TotalSize: targetPieceSize}
		hasher := blake3.New(32, nil)
		teeReader := io.TeeReader(pr, hasher)

		if _, err := io.Copy(dst, teeReader); err != nil {
			log.Printf("encodeShard error: failed to copy mirrored piece: %v", err)
			dst.Close()
			return
		}
		dst.Close()

		// Save the padded hash to DB so syncMirroredShards can find it
		shardHash := hex.EncodeToString(hasher.Sum(nil))
		_, err = e.DB.Exec("UPDATE shards SET hash = ? WHERE id = ?", shardHash, shardID)
		if err != nil {
			log.Printf("encodeShard error: failed to update shard hash in DB: %v", err)
		}

		if !e.KeepLocalCopy {
			os.Remove(shardPath)
		}
		log.Printf("Successfully prepared special shard %d for mirroring (padded to %d MB, hash: %s)", shardID, targetPieceSize/(1024*1024), shardHash[:16])
		return
	}

	// --- Standard RS Encoding for Data Shards ---
	
	enc, err := reedsolomon.NewStream(e.DataShards, e.ParityShards)
	if err != nil {
		log.Printf("encodeShard error: failed to create stream encoder: %v", err)
		return
	}

	if err := os.MkdirAll(e.QueueDir, 0755); err != nil {
		log.Printf("encodeShard error: failed to create queue dir: %v", err)
		return
	}

	outWriters := make([]io.Writer, e.DataShards+e.ParityShards)
	outFiles := make([]*os.File, e.DataShards+e.ParityShards)

	for i := 0; i < e.DataShards+e.ParityShards; i++ {
		// Use .tmp suffix so OutboundWorker doesn't pick them up yet
		outPath := filepath.Join(e.QueueDir, fmt.Sprintf("shard_%d_piece_%d.tmp", shardID, i))
		f, err := os.Create(outPath)
		if err != nil {
			log.Printf("encodeShard error: failed to create piece %d: %v", i, err)
			// Cleanup
			for j := 0; j < i; j++ {
				outFiles[j].Close()
			}
			return
		}
		outFiles[i] = f
		outWriters[i] = f
	}

	// 1. Split data into N files and compute full shard hash
	// Use PadReader to ensure we always split exactly e.ShardSize bytes
	pr := &PadReader{Reader: inFile, TotalSize: e.ShardSize}
	hasher := blake3.New(32, nil)
	teeReader := io.TeeReader(pr, hasher)

	err = enc.Split(teeReader, outWriters[:e.DataShards], e.ShardSize)
	if err != nil {
		log.Printf("encodeShard error: failed to split shard %d: %v", shardID, err)
	} else {
		// Save shard hash to DB
		shardHash := hex.EncodeToString(hasher.Sum(nil))
		_, err = e.DB.Exec("UPDATE shards SET hash = ? WHERE id = ?", shardHash, shardID)
		if err != nil {
			log.Printf("encodeShard error: failed to update shard hash in DB: %v", err)
		}

		// Seek data files back to beginning so we can use them as readers for parity encoding
		inReaders := make([]io.Reader, e.DataShards)
		for i := 0; i < e.DataShards; i++ {
			if _, seekErr := outFiles[i].Seek(0, 0); seekErr != nil {
				log.Printf("encodeShard error: failed to seek piece %d: %v", i, seekErr)
				// Cleanup and abort
				for _, f := range outFiles {
					f.Close()
				}
				return
			}
			inReaders[i] = outFiles[i]
		}

		// 2. Encode to generate K parity pieces
		err = enc.Encode(inReaders, outWriters[e.DataShards:])
		if err != nil {
			log.Printf("encodeShard error: failed to encode parity for shard %d: %v", shardID, err)
		} else if e.Verbose {
			log.Printf("Successfully erasure coded shard %d into %d pieces", shardID, e.DataShards+e.ParityShards)
		}
	}

	// 3. Finalize all pieces: Close and rename to remove .tmp suffix
	for i := 0; i < e.DataShards+e.ParityShards; i++ {
		outFiles[i].Close()
		
		// If encoding succeeded, rename to final path so OutboundWorker sees them
		if err == nil {
			tmpPath := filepath.Join(e.QueueDir, fmt.Sprintf("shard_%d_piece_%d.tmp", shardID, i))
			finalPath := filepath.Join(e.QueueDir, fmt.Sprintf("shard_%d_piece_%d", shardID, i))
			if renameErr := os.Rename(tmpPath, finalPath); renameErr != nil {
				log.Printf("encodeShard error: failed to finalize piece %d: %v", i, renameErr)
			}
		}
	}
}

// SyncPeers registers or updates the configured peers in the database.
func (e *Engine) SyncPeers(peers []config.PeerConfig) error {
	for _, p := range peers {
		if p.TLSPublicKey == "" {
			continue
		}

		expectedCertPEM, err := os.ReadFile(p.TLSPublicKey)
		if err != nil {
			log.Printf("SyncPeers: Failed to read peer %s public key file: %v", p.Name, err)
			continue
		}

		block, _ := pem.Decode(expectedCertPEM)
		if block == nil {
			log.Printf("SyncPeers: Failed to decode PEM block from %s", p.TLSPublicKey)
			continue
		}

		var peerKey []byte
		if block.Type == "CERTIFICATE" {
			cert, err := x509.ParseCertificate(block.Bytes)
			if err == nil {
				peerKey, _ = x509.MarshalPKIXPublicKey(cert.PublicKey)
			}
		} else if block.Type == "PUBLIC KEY" {
			peerKey = block.Bytes
		}

		if len(peerKey) == 0 {
			log.Printf("SyncPeers: Failed to extract public key for peer %s", p.Name)
			continue
		}

		pubKeyHex := hex.EncodeToString(peerKey)

		// SQLite UPSERT
		query := `
			INSERT INTO peers (ip_address, public_key, max_storage_size)
			VALUES (?, ?, ?)
			ON CONFLICT(public_key) DO UPDATE SET 
				ip_address=excluded.ip_address,
				max_storage_size=excluded.max_storage_size
		`
		_, err = e.DB.Exec(query, p.Address, pubKeyHex, p.StorageLimitGB)
		if err != nil {
			return fmt.Errorf("failed to sync peer %s to DB: %w", p.Name, err)
		}
	}
	
	if e.Verbose {
		log.Printf("Successfully synchronized %d peers to database", len(peers))
	}
	return nil
}

// OfferShards checks which of the offered peer shards the server already has.
func (e *Engine) OfferShards(ctx context.Context, pubKeyHex string, shards []rpc.PeerShardMeta) ([]uint32, error) {
	var neededIndices []uint32

	if pubKeyHex != "" {
		// 1. Auto-register peer if they are calling this API
		_, err := e.DB.ExecContext(ctx, `
			INSERT INTO peers (ip_address, public_key, status)
			VALUES ('unknown', ?, 'untrusted')
			ON CONFLICT(public_key) DO UPDATE SET last_seen = CURRENT_TIMESTAMP
		`, pubKeyHex)
		if err != nil {
			log.Printf("Warning: failed to auto-register peer: %v", err)
		}

		// 2. Quota check
		var status string
		var currentSize int64
		var maxStorageBytes int64
		var peerID int64
		err = e.DB.QueryRowContext(ctx, "SELECT id, status, current_storage_size, max_storage_size FROM peers WHERE public_key = ?", pubKeyHex).Scan(&peerID, &status, &currentSize, &maxStorageBytes)
		if err == nil && status == "untrusted" {
			var incomingSize int64
			for _, shard := range shards {
				incomingSize += shard.Size
			}

			// Dynamic Reciprocity: Limit = (MyPiecesOnPeer + BasePieceBuffer) * PieceSize
			var myPiecesOnPeer int
			_ = e.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbound_pieces WHERE peer_id = ? AND status = 'uploaded'", peerID).Scan(&myPiecesOnPeer)
			
			pieceSize := e.ShardSize / int64(e.DataShards)
			limitBytes := (int64(myPiecesOnPeer) + int64(e.BasePieceBuffer)) * pieceSize

			if currentSize+incomingSize > limitBytes {
				return nil, fmt.Errorf("peer upload quota exceeded: %d bytes over limit (stored %d pieces for us, allowed %d pieces total)", (currentSize+incomingSize)-limitBytes, myPiecesOnPeer, myPiecesOnPeer+e.BasePieceBuffer)
			}
		} else if err == nil && status == "trusted" && maxStorageBytes > 0 {
			// Trusted peer with explicit quota
			var incomingSize int64
			for _, shard := range shards {
				incomingSize += shard.Size
			}
			if currentSize+incomingSize > maxStorageBytes {
				return nil, fmt.Errorf("trusted peer upload quota exceeded")
			}
		}
	} else {
		return nil, fmt.Errorf("unidentified peer rejected")
	}

	stmt, err := e.DB.PrepareContext(ctx, "SELECT 1 FROM hosted_shards WHERE hash = ?")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for i, shard := range shards {
		var exists int
		err := stmt.QueryRowContext(ctx, shard.Hash).Scan(&exists)
		if err == sql.ErrNoRows {
			// We don't have this shard, request it
			neededIndices = append(neededIndices, uint32(i))
		} else if err != nil {
			log.Printf("Error checking hosted shard existence %s: %v", shard.Hash, err)
			// Safest to request it if there's an error
			neededIndices = append(neededIndices, uint32(i))
		}
	}

	if e.Verbose {
		log.Printf("Peer offered %d shards, requesting %d missing shards", len(shards), len(neededIndices))
	}

	return neededIndices, nil
}

// UploadShards receives the actual peer shard data and saves it to disk.
func (e *Engine) UploadShards(ctx context.Context, pubKeyHex string, shards []rpc.LocalBlobData) error {
	if pubKeyHex != "" {
		// Re-verify quota
		var status string
		var currentSize, maxStorageBytes int64
		var peerID int64
		err := e.DB.QueryRowContext(ctx, "SELECT id, status, current_storage_size, max_storage_size FROM peers WHERE public_key = ?", pubKeyHex).Scan(&peerID, &status, &currentSize, &maxStorageBytes)
		if err == nil {
			var incomingSize int64
			for _, shard := range shards {
				incomingSize += int64(len(shard.Data))
			}

			limitBytes := maxStorageBytes
			if status == "untrusted" {
				var myPiecesOnPeer int
				_ = e.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbound_pieces WHERE peer_id = ? AND status = 'uploaded'", peerID).Scan(&myPiecesOnPeer)
				
				pieceSize := e.ShardSize / int64(e.DataShards)
				limitBytes = (int64(myPiecesOnPeer) + int64(e.BasePieceBuffer)) * pieceSize
			}

			if limitBytes > 0 && currentSize+incomingSize > limitBytes {
				return fmt.Errorf("peer upload quota exceeded")
			}

			// Global server limit check
			if e.MaxStorageBytes > 0 {
				var localTotal, peerTotal int64
				_ = e.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(size), 0) FROM shards").Scan(&localTotal)
				_ = e.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(size), 0) FROM hosted_shards").Scan(&peerTotal)
				if (localTotal + peerTotal + incomingSize) > e.MaxStorageBytes {
					return fmt.Errorf("global server storage limit exceeded (%d GB)", e.MaxStorageBytes / (1024*1024*1024))
				}
			}
		}
	}

	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, "INSERT OR IGNORE INTO hosted_shards (hash, size, peer_id, is_special, piece_index, parent_shard_hash, sequence, total_pieces) VALUES (?, ?, (SELECT id FROM peers WHERE public_key = ?), ?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, shard := range shards {
		shardPath := filepath.Join(e.BlobStoreDir, "peer_"+shard.Hash)
		
		// Write to disk
		if err := os.WriteFile(shardPath, shard.Data, 0644); err != nil {
			return fmt.Errorf("failed to write peer shard %s to disk: %w", shard.Hash, err)
		}

		// Update DB
		res, err := stmt.ExecContext(ctx, shard.Hash, len(shard.Data), pubKeyHex, shard.IsSpecial, shard.PieceIndex, shard.ParentShardHash, shard.SequenceNumber, shard.TotalPieces)
		if err != nil {
			return fmt.Errorf("failed to insert peer shard %s into db: %w", shard.Hash, err)
		}

		rowsAffected, _ := res.RowsAffected()
		if rowsAffected > 0 && pubKeyHex != "" && pubKeyHex != "insecure-local-client" {
			_, err = tx.ExecContext(ctx, "UPDATE peers SET current_storage_size = current_storage_size + ?, total_shards = total_shards + 1, current_shards = current_shards + 1 WHERE public_key = ?", int64(len(shard.Data)), pubKeyHex)
			if err != nil {
				return fmt.Errorf("failed to update peer storage size: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	if e.Verbose {
		log.Printf("Successfully saved %d peer shards to storage", len(shards))
	}

	return nil
}

// GetStatus returns the current runtime metrics and replication state of the server.
func (e *Engine) GetStatus(ctx context.Context) (rpc.StatusInfo, error) {
	status := rpc.StatusInfo{
		UptimeSeconds: uint64(time.Since(e.StartTime).Seconds()),
	}

	// 1. Total local shards (sealed and open)
	err := e.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM shards").Scan(&status.TotalShards)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("GetStatus: failed to count shards: %v", err)
	}

	// 2. Replication stats (checking outbound_pieces per shard)
	// For mirrored shards: we want at least 3 copies (or total available peers if < 3).
	// For standard shards: we want exactly DataShards + ParityShards unique pieces.
	query := `
		SELECT 
			COUNT(CASE WHEN is_full = 1 THEN 1 END) as fully_replicated,
			COUNT(CASE WHEN is_full = 0 AND piece_count > 0 THEN 1 END) as partially_replicated
		FROM (
			SELECT 
				s.id, 
				s.mirrored,
				COUNT(DISTINCT op.peer_id) as piece_count,
				CASE 
					WHEN s.mirrored = 1 AND COUNT(DISTINCT op.peer_id) >= 3 THEN 1
					WHEN s.mirrored = 0 AND COUNT(DISTINCT op.piece_index) >= ? THEN 1
					ELSE 0 
				END as is_full
			FROM shards s
			LEFT JOIN outbound_pieces op ON s.id = op.shard_id AND op.status = 'uploaded'
			GROUP BY s.id
		)
	`
	requiredPieces := e.DataShards + e.ParityShards
	err = e.DB.QueryRowContext(ctx, query, requiredPieces).Scan(
		&status.FullyReplicatedShards, 
		&status.PartiallyReplicatedShards,
	)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("GetStatus: failed to count replication: %v", err)
	}

	// 3. Hosted Peer Shards (shards other people have sent us)
	err = e.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM hosted_shards").Scan(&status.HostedPeerShards)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("GetStatus: failed to count hosted shards: %v", err)
	}

	// 4. Queued Data (size of 'open' shards)
	err = e.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(size), 0) FROM shards WHERE status = 'open'").Scan(&status.QueuedBytes)
	if err != nil {
		log.Printf("GetStatus: failed to calculate queued bytes: %v", err)
	}

	return status, nil
}

// ChallengePiece natively streams exactly 32 byte sequences from a specifically identified offset inherently testing active disk persistence.
func (e *Engine) ChallengePiece(ctx context.Context, pubKeyHex string, checksum []byte, offset uint64) ([]byte, error) {
	hashStr := hex.EncodeToString(checksum)
	
	var count int
	err := e.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM hosted_shards WHERE hash = ? AND peer_id = (SELECT id FROM peers WHERE public_key = ?)", hashStr, pubKeyHex).Scan(&count)
	if err != nil || count == 0 {
		return nil, fmt.Errorf("shard %s not actively tracked for this peer", hashStr)
	}

	shardPath := filepath.Join(e.BlobStoreDir, "peer_"+hashStr)
	f, err := os.Open(shardPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open shard file: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat shard file: %w", err)
	}
	if int64(offset)+32 > fi.Size() {
		return nil, fmt.Errorf("offset %d + 32 exceeds file size %d for shard %s", offset, fi.Size(), hashStr)
	}

	if _, err := f.Seek(int64(offset), 0); err != nil {
		return nil, fmt.Errorf("seek failed at offset %d: %w", offset, err)
	}

	buf := make([]byte, 32)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, fmt.Errorf("read failed at offset %d (file size %d): %w", offset, fi.Size(), err)
	}

	return buf, nil
}

// DeleteBlobs marks blobs as deleted by decrementing their reference count.
// If the count reaches 0, it sets the deleted_at timestamp.
// Actual removal happens later via a garbage collection process.
func (e *Engine) DeleteBlobs(ctx context.Context, hashes []string) error {
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Update ref_count, preventing it from dropping below 0
	stmtDec, err := tx.PrepareContext(ctx, "UPDATE blobs SET ref_count = MAX(0, ref_count - 1) WHERE hash = ?")
	if err != nil {
		return fmt.Errorf("failed to prepare decrement statement: %w", err)
	}
	defer stmtDec.Close()

	// Mark deleted_at if ref_count is 0
	stmtDel, err := tx.PrepareContext(ctx, "UPDATE blobs SET deleted_at = CURRENT_TIMESTAMP WHERE hash = ? AND ref_count = 0 AND deleted_at IS NULL")
	if err != nil {
		return fmt.Errorf("failed to prepare delete statement: %w", err)
	}
	defer stmtDel.Close()

	for _, hash := range hashes {
		if _, err := stmtDec.ExecContext(ctx, hash); err != nil {
			return fmt.Errorf("failed to decrement ref count for %s: %w", hash, err)
		}
		if _, err := stmtDel.ExecContext(ctx, hash); err != nil {
			return fmt.Errorf("failed to set deleted_at for %s: %w", hash, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	if e.Verbose {
		log.Printf("Marked %d blobs for potential deletion (decremented ref counts)", len(hashes))
	}
	return nil
}

// ListAllBlobs returns a list of all blob hashes currently tracked by the server.
func (e *Engine) ListAllBlobs(ctx context.Context) ([]string, error) {
	rows, err := e.DB.QueryContext(ctx, "SELECT hash FROM blobs")
	if err != nil {
		return nil, fmt.Errorf("failed to query all blobs: %w", err)
	}
	defer rows.Close()

	var hashes []string
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, fmt.Errorf("failed to scan blob hash: %w", err)
		}
		hashes = append(hashes, hash)
	}

	return hashes, nil
}

func (e *Engine) AddPeer(ctx context.Context, address string) error {
	maddr, err := multiaddr.NewMultiaddr(address)
	if err != nil {
		return fmt.Errorf("invalid multiaddress format %s: %w", address, err)
	}

	addrInfo, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return fmt.Errorf("multiaddress must include /p2p/ PeerID component: %w", err)
	}

	if err := e.Host.Connect(ctx, *addrInfo); err != nil {
		return fmt.Errorf("failed to dial peer at %s: %w", address, err)
	}

	pubKeyHex, err := crypto.PubKeyHexFromPeerID(addrInfo.ID)
	if err != nil {
		return fmt.Errorf("failed to extract public key from peer ID: %w", err)
	}

	// SQLite UPSERT
	query := `
		INSERT INTO peers (ip_address, public_key, status)
		VALUES (?, ?, 'untrusted')
		ON CONFLICT(public_key) DO UPDATE SET 
			ip_address=excluded.ip_address
	`
	// Store the base address without the peer ID for easier connection later
	baseAddr, _ := multiaddr.SplitFunc(maddr, func(c multiaddr.Component) bool {
		return c.Protocol().Code == multiaddr.P_P2P
	})
	
	_, err = e.DB.ExecContext(ctx, query, baseAddr.String(), pubKeyHex)
	return err
}

// AddOrUpdatePeer registers a peer based on handshake data without dialing out.
func (e *Engine) AddOrUpdatePeer(ctx context.Context, address, pubKeyHex string) (int64, error) {
	// Use ON CONFLICT to update last_seen, but only set ip_address if it's currently empty or unknown.
	// In this simple implementation, we'll check if the peer exists first to avoid overwriting a good port with an ephemeral one.
	var id int64
	var existingAddr string
	err := e.DB.QueryRowContext(ctx, "SELECT id, ip_address FROM peers WHERE public_key = ?", pubKeyHex).Scan(&id, &existingAddr)
	
	if err == sql.ErrNoRows {
		// New peer, we have to use what we have (even if the port is ephemeral)
		res, err := e.DB.ExecContext(ctx, "INSERT INTO peers (ip_address, public_key, status) VALUES (?, ?, 'untrusted')", address, pubKeyHex)
		if err != nil {
			return 0, err
		}
		return res.LastInsertId()
	} else if err != nil {
		return 0, err
	}

	// Existing peer: update last_seen but preserve the address if it looks like a real listener (not ephemeral)
	// For now, we just never overwrite the address once we have one.
	_, err = e.DB.ExecContext(ctx, "UPDATE peers SET last_seen = CURRENT_TIMESTAMP WHERE id = ?", id)
	return id, err
}

func (e *Engine) UpdatePeer(ctx context.Context, id int64, status string, maxStorageBytes int64) error {
	_, err := e.DB.ExecContext(ctx, "UPDATE peers SET status = ?, max_storage_size = ? WHERE id = ?", status, maxStorageBytes/(1024*1024*1024), id)
	return err
}

// GetPeerIDByPubKey returns the database ID for a peer public key.
func (e *Engine) GetPeerIDByPubKey(ctx context.Context, pubKeyHex string) (int64, error) {
	var id int64
	err := e.DB.QueryRowContext(ctx, "SELECT id FROM peers WHERE public_key = ?", pubKeyHex).Scan(&id)
	return id, err
}

// AnnouncePeer registers or updates a peer based on their self-reported listener address.
func (e *Engine) AnnouncePeer(ctx context.Context, pubKeyHex, listenAddress, contactInfo string) (int64, error) {
	if pubKeyHex == "" || listenAddress == "" || pubKeyHex == "insecure-local-client" {
		return 0, nil
	}

	// Dynamic registration or update
	_, err := e.DB.ExecContext(ctx, `
		INSERT INTO peers (ip_address, public_key, status, contact_info)
		VALUES (?, ?, 'untrusted', ?)
		ON CONFLICT(public_key) DO UPDATE SET 
			ip_address=excluded.ip_address,
			contact_info=excluded.contact_info,
			last_seen=CURRENT_TIMESTAMP
	`, listenAddress, pubKeyHex, contactInfo)
	
	if err != nil {
		return 0, fmt.Errorf("failed to announce peer: %w", err)
	}

	return e.GetPeerIDByPubKey(ctx, pubKeyHex)
}

// AuthorizeClient registers a client connection and returns its status and quota.
func (e *Engine) AuthorizeClient(ctx context.Context, pubKeyHex string) (status string, quotaBytes int64, currentSize int64, err error) {
	// Auto-register if new
	_, err = e.DB.ExecContext(ctx, "INSERT OR IGNORE INTO clients (public_key, status) VALUES (?, 'pending')", pubKeyHex)
	if err != nil {
		return "", 0, 0, fmt.Errorf("failed to register client: %w", err)
	}

	err = e.DB.QueryRowContext(ctx, "SELECT status, max_storage_size, current_storage_size FROM clients WHERE public_key = ?", pubKeyHex).Scan(&status, &quotaBytes, &currentSize)
	if err == sql.ErrNoRows {
		return "", 0, 0, nil
	} else if err != nil {
		return "", 0, 0, fmt.Errorf("failed to query client status: %w", err)
	}

	return status, quotaBytes, currentSize, nil
}

// UpdateClient changes a client's status and quota. Only the Admin can do this.
func (e *Engine) UpdateClient(ctx context.Context, callerPubKey string, id uint64, status string, maxStorageBytes uint64) error {
	if e.AdminPublicKey != "" && callerPubKey != e.AdminPublicKey {
		return fmt.Errorf("unauthorized: only admin can update clients")
	}

	if id == 0 {
		return fmt.Errorf("invalid client ID")
	}

	_, err := e.DB.ExecContext(ctx, "UPDATE clients SET status = ?, max_storage_size = ? WHERE id = ?", status, maxStorageBytes, id)
	return err
}

// AddClient manually adds a client by their public key.
func (e *Engine) AddClient(ctx context.Context, callerPubKey string, clientPubKey string, status string, maxStorageBytes uint64) error {
	if e.AdminPublicKey != "" && callerPubKey != e.AdminPublicKey {
		return fmt.Errorf("unauthorized: only admin can add clients")
	}

	_, err := e.DB.ExecContext(ctx, `
		INSERT INTO clients (public_key, status, max_storage_size)
		VALUES (?, ?, ?)
		ON CONFLICT(public_key) DO UPDATE SET 
			status=excluded.status,
			max_storage_size=excluded.max_storage_size
	`, clientPubKey, status, maxStorageBytes)
	return err
}

type ClientDBInfo struct {
	ID                 uint64
	PublicKey          string
	Status             string
	LastSeen           string
	MaxStorageSize     uint64
	CurrentStorageSize uint64
}

func (e *Engine) ListClients(ctx context.Context) ([]ClientDBInfo, error) {
	// Query clients that are NOT also in the peers table.
	// This separates management of the Swarm (peers) from management of Users (clients).
	query := `
		SELECT c.id, c.public_key, c.status, c.last_seen, c.max_storage_size, c.current_storage_size 
		FROM clients c
		LEFT JOIN peers p ON c.public_key = p.public_key
		WHERE p.id IS NULL
		ORDER BY c.last_seen DESC
	`
	rows, err := e.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var clients []ClientDBInfo
	for rows.Next() {
		var c ClientDBInfo
		if err := rows.Scan(&c.ID, &c.PublicKey, &c.Status, &c.LastSeen, &c.MaxStorageSize, &c.CurrentStorageSize); err != nil {
			return nil, err
		}
		clients = append(clients, c)
	}
	return clients, nil
}

// ListSpecialPieces returns a list of all shard pieces tagged as special for a peer.
func (e *Engine) ListSpecialPieces(ctx context.Context, pubKeyHex string) ([]rpc.PeerShardMeta, error) {
	rows, err := e.DB.QueryContext(ctx, "SELECT hash, size, piece_index, parent_shard_hash, sequence, total_pieces FROM hosted_shards WHERE peer_id = (SELECT id FROM peers WHERE public_key = ?) AND is_special = 1", pubKeyHex)
	if err != nil {
		return nil, fmt.Errorf("failed to query special pieces: %w", err)
	}
	defer rows.Close()

	var pieces []rpc.PeerShardMeta
	for rows.Next() {
		var p rpc.PeerShardMeta
		if err := rows.Scan(&p.Hash, &p.Size, &p.PieceIndex, &p.ParentShardHash); err != nil {
			return nil, err
		}
		p.IsSpecial = true
		pieces = append(pieces, p)
	}
	return pieces, nil
}

// ReleasePiece deletes a hosted shard piece from storage and updates the peer's quota.
func (e *Engine) ReleasePiece(ctx context.Context, hashBytes []byte, pubKeyHex string) error {
	hashStr := hex.EncodeToString(hashBytes)

	// 1. Verify we host this shard for this peer
	var size int64
	err := e.DB.QueryRowContext(ctx, "SELECT size FROM hosted_shards WHERE hash = ? AND peer_id = (SELECT id FROM peers WHERE public_key = ?)", hashStr, pubKeyHex).Scan(&size)
	if err == sql.ErrNoRows {
		return nil // Already deleted or not found
	} else if err != nil {
		return fmt.Errorf("failed to query hosted shard: %w", err)
	}

	// 2. Transactional cleanup
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete from hosted_shards
	_, err = tx.ExecContext(ctx, "DELETE FROM hosted_shards WHERE hash = ? AND peer_id = (SELECT id FROM peers WHERE public_key = ?)", hashStr, pubKeyHex)
	if err != nil {
		return fmt.Errorf("failed to delete hosted shard record: %w", err)
	}

	// Update peer quota
	if pubKeyHex != "" && pubKeyHex != "insecure-local-client" {
		_, err = tx.ExecContext(ctx, "UPDATE peers SET current_storage_size = MAX(0, current_storage_size - ?), current_shards = MAX(0, current_shards - 1) WHERE public_key = ?", size, pubKeyHex)
		if err != nil {
			return fmt.Errorf("failed to update peer quota: %w", err)
		}
	}

	// 3. Commit and Delete File
	if err := tx.Commit(); err != nil {
		return err
	}

	shardPath := filepath.Join(e.BlobStoreDir, "peer_"+hashStr)
	if err := os.Remove(shardPath); err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: failed to remove shard file %s: %v", shardPath, err)
	}

	if e.Verbose {
		log.Printf("Successfully released piece %s for peer %s", hashStr, pubKeyHex[:16])
	}

	return nil
}

// DownloadPiece retrieves a hosted shard piece from storage.

type PeerDBInfo struct {
	ID                  int64
	Address             string
	PublicKey           string
	Status              string
	FirstSeen           string
	LastSeen            string
	MaxStorageSize      int64
	CurrentStorageSize  int64
	OutboundStorageSize int64
	ContactInfo         string
	TotalShards         uint64
	CurrentShards       uint64
	ChallengesMade      uint32
	ChallengesPassed    uint32
	ConnectionsOk       uint32
	IntegrityAttempts   uint32
}

func (e *Engine) ListPeers(ctx context.Context) ([]PeerDBInfo, error) {
	// Query all peers from the registry.
	// We include peers we dial out to AND peers that dial in to us.
	rows, err := e.DB.QueryContext(ctx, "SELECT id, ip_address, public_key, status, COALESCE(first_seen, last_seen), last_seen, max_storage_size, current_storage_size, outbound_storage_size, contact_info, total_shards, current_shards FROM peers ORDER BY last_seen DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var peers []PeerDBInfo
	for rows.Next() {
		var p PeerDBInfo
		if err := rows.Scan(&p.ID, &p.Address, &p.PublicKey, &p.Status, &p.FirstSeen, &p.LastSeen, &p.MaxStorageSize, &p.CurrentStorageSize, &p.OutboundStorageSize, &p.ContactInfo, &p.TotalShards, &p.CurrentShards); err != nil {
			return nil, err
		}
		peers = append(peers, p)
	}
	rows.Close()

	// Compute 7-day challenge stats per peer
	for i := range peers {
		var total, reachable, passed, integrityAttempts int
		err := e.DB.QueryRowContext(ctx, `
			SELECT 
				COUNT(*),
				SUM(CASE WHEN status IN ('ok', 'pass', 'fail') THEN 1 ELSE 0 END),
				SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END),
				SUM(CASE WHEN status IN ('pass', 'fail') THEN 1 ELSE 0 END)
			FROM challenge_results
			WHERE peer_id = ? AND timestamp >= datetime('now', '-7 days')
		`, peers[i].ID).Scan(&total, &reachable, &passed, &integrityAttempts)
		if err == nil {
			peers[i].ChallengesMade = uint32(total)
			peers[i].ConnectionsOk = uint32(reachable)
			peers[i].ChallengesPassed = uint32(passed)
			peers[i].IntegrityAttempts = uint32(integrityAttempts)
		}
	}

	return peers, nil
}

func (e *Engine) RegisterActivePeer(peerID int64, peerNode rpc.PeerNode) {
	e.ActivePeersMu.Lock()
	defer e.ActivePeersMu.Unlock()
	e.ActivePeers[peerID] = peerNode
}

func (e *Engine) GetActivePeer(peerID int64) rpc.PeerNode {
	e.ActivePeersMu.RLock()
	defer e.ActivePeersMu.RUnlock()
	return e.ActivePeers[peerID]
}

func (e *Engine) RemoveActivePeer(peerID int64) {
	e.ActivePeersMu.Lock()
	defer e.ActivePeersMu.Unlock()
	delete(e.ActivePeers, peerID)
}

// GetOrDialPeer returns an active RPC client for a peer, dialing them if necessary.
func (e *Engine) GetOrDialPeer(ctx context.Context, peerID int64) (*CapnpPeerClient, error) {
	stub := e.GetActivePeer(peerID)
	if stub.IsValid() {
		return NewPeerClientFromStub(stub.AddRef()), nil
	}

	// Lookup in DB
	var address, pubKeyHex string
	err := e.DB.QueryRowContext(ctx, "SELECT ip_address, public_key FROM peers WHERE id = ?", peerID).Scan(&address, &pubKeyHex)
	if err != nil {
		return nil, fmt.Errorf("peer %d not found in database: %w", peerID, err)
	}

	// Dial using libp2p
	client, err := NewCapnpPeerClient(ctx, e, address, pubKeyHex, e.LocalPeerNode)
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer %d at %s: %w", peerID, address, err)
	}
	client.Permanent = true // We manage this in the background goroutine

	// Register it so others can reuse this connection
	e.RegisterActivePeer(peerID, client.clientStub.AddRef())

	// Announce our own listener address so they can dial us back.
	if e.ListenAddress != "" {
		cbHandler := NewRPCHandler(e, pubKeyHex)
		cbNode := rpc.PeerNode_ServerToClient(cbHandler)
		if err := client.Announce(ctx, e.ListenAddress, e.ContactInfo, cbNode); err != nil {
			log.Printf("Warning: failed to auto-announce to peer %d: %v", peerID, err)
		}
		cbNode.Release()
	}

	// Setup cleanup when connection drops
	go func(pid int64, c *CapnpPeerClient) {
		<-c.rpcConn.Done()
		e.RemoveActivePeer(pid)
		c.ForceClose()
	}(peerID, client)

	return client, nil
}

// PrepareUpload validates quota and saves expected metadata for incoming streams.
func (e *Engine) PrepareUpload(ctx context.Context, pubKeyHex string, metas []PendingStreamMeta) error {
	if pubKeyHex != "" {
		var status string
		var currentSize, maxStorageBytes int64
		var peerID int64
		err := e.DB.QueryRowContext(ctx, "SELECT id, status, current_storage_size, max_storage_size FROM peers WHERE public_key = ?", pubKeyHex).Scan(&peerID, &status, &currentSize, &maxStorageBytes)
		if err == nil {
			var incomingSize int64
			for _, m := range metas {
				incomingSize += int64(m.Size)
			}

			limitBytes := maxStorageBytes
			if status == "untrusted" {
				var myPiecesOnPeer int
				_ = e.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbound_pieces WHERE peer_id = ? AND status = 'uploaded'", peerID).Scan(&myPiecesOnPeer)
				
				pieceSize := e.ShardSize / int64(e.DataShards)
				limitBytes = (int64(myPiecesOnPeer) + int64(e.BasePieceBuffer)) * pieceSize
			}

			if limitBytes > 0 && currentSize+incomingSize > limitBytes {
				return fmt.Errorf("peer upload quota exceeded")
			}

			if e.MaxStorageBytes > 0 {
				var localTotal, peerTotal int64
				_ = e.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(size), 0) FROM shards").Scan(&localTotal)
				_ = e.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(size), 0) FROM hosted_shards").Scan(&peerTotal)
				if (localTotal + peerTotal + incomingSize) > e.MaxStorageBytes {
					return fmt.Errorf("global server storage limit exceeded (%d GB)", e.MaxStorageBytes / (1024*1024*1024))
				}
			}
		}
	}
	return nil
}
