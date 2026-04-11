package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"golang.org/x/time/rate"
	"lukechampine.com/blake3"
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
	StandaloneMode             bool
	ConfigPath                 string
	// Automatic Adoption
	AdoptionEnabled         bool
	AdoptionPeriodMinutes   int
	AdoptionChallengePieces int
	// Bandwidth Throttling
	UploadLimiter   *rate.Limiter
	DownloadLimiter *rate.Limiter
	// Discovery
	BootstrapPeers []string
	// Peer Management
	LocalPeerNode         rpc.PeerNode
	ActivePeers           map[int64]rpc.PeerNode
	ActivePeersMu         sync.RWMutex
	StartTime             time.Time
	MasterKey             []byte
	mu                    sync.Mutex // Protects pendingItems and shard reservation
	pendingItems          map[string]rpc.Metadata
	activeShardID         int64
	activeShardSize       int64
	wg                    sync.WaitGroup
	streamSemaphore       chan struct{}
	pendingInboundStreams sync.Map
	StreamBufferPool      sync.Pool
	DHT                   *dht.IpfsDHT // Shared DHT for peer lookups
}

// EngineConfig holds all configuration parameters for the server Engine.
type EngineConfig struct {
	DB                         *sql.DB
	ConfigPath                 string
	SQLitePath                 string
	BlobStoreDir               string
	QueueDir                   string
	DataShards                 int
	ParityShards               int
	ShardSize                  int64
	KeepLocalCopy              bool
	P2PHost                    host.Host
	ListenAddress              string
	UntrustedPeerUploadLimitMB int
	Verbose                    bool
	ExtraVerbose               bool
	ChallengesPerPiece         int
	KeepDeletedMinutes         int
	KeepMetadataMinutes        int
	WasteThreshold             float64
	GCIntervalMinutes          int
	SelfBackupIntervalMinutes  int
	PeerEvictionHours          int
	BasePieceBuffer            int
	MaxStorageGB               int
	MaxUploadKBPS              int
	MaxDownloadKBPS            int
	MasterKey                  []byte
	AdminPublicKey             string
	ContactInfo                string
	MaxConcurrentStreams       int
	StandaloneMode             bool
	AdoptionEnabled            bool
	AdoptionPeriodMinutes      int
	AdoptionChallengePieces    int
	BootstrapPeers             []string
}

// NewEngine creates a new server Engine from the provided configuration.
func NewEngine(cfg EngineConfig) *Engine {
	if cfg.UntrustedPeerUploadLimitMB <= 0 {
		cfg.UntrustedPeerUploadLimitMB = 1024
	}
	if cfg.DataShards <= 0 {
		cfg.DataShards = 10
	}
	if cfg.ParityShards < 0 {
		cfg.ParityShards = 4
	}
	if cfg.ShardSize <= 0 {
		cfg.ShardSize = int64(cfg.DataShards) * 256 * 1024 * 1024 // Default piece size to 256MB
	}
	if cfg.ShardSize < int64(cfg.DataShards) {
		cfg.ShardSize = int64(cfg.DataShards)
	}
	if cfg.MaxConcurrentStreams <= 0 {
		cfg.MaxConcurrentStreams = 4 // Default to 4 concurrent streams
	}
	if cfg.ChallengesPerPiece <= 0 {
		cfg.ChallengesPerPiece = 8
	}
	if cfg.WasteThreshold <= 0 {
		cfg.WasteThreshold = 0.5
	}
	if cfg.GCIntervalMinutes <= 0 {
		cfg.GCIntervalMinutes = 720 // 12 hours
	}
	if cfg.SelfBackupIntervalMinutes <= 0 {
		cfg.SelfBackupIntervalMinutes = 1440 // 24 hours
	}
	if cfg.PeerEvictionHours <= 0 {
		cfg.PeerEvictionHours = 24 // 24 hours default
	}
	if cfg.BasePieceBuffer <= 0 {
		cfg.BasePieceBuffer = 4 // Default 4 pieces buffer
	}
	if cfg.KeepMetadataMinutes <= 0 {
		cfg.KeepMetadataMinutes = 60 * 24 * 7 // 7 days default
	}

	var maxBytes int64
	if cfg.MaxStorageGB > 0 {
		maxBytes = int64(cfg.MaxStorageGB) * 1024 * 1024 * 1024
	}

	var uploadLimiter, downloadLimiter *rate.Limiter
	if cfg.MaxUploadKBPS > 0 {
		uploadLimiter = rate.NewLimiter(rate.Limit(cfg.MaxUploadKBPS*1024), cfg.MaxUploadKBPS*1024)
	}
	if cfg.MaxDownloadKBPS > 0 {
		downloadLimiter = rate.NewLimiter(rate.Limit(cfg.MaxDownloadKBPS*1024), cfg.MaxDownloadKBPS*1024)
	}

	return &Engine{
		DB:                         cfg.DB,
		SQLitePath:                 cfg.SQLitePath,
		BlobStoreDir:               cfg.BlobStoreDir,
		QueueDir:                   cfg.QueueDir,
		DataShards:                 cfg.DataShards,
		ParityShards:               cfg.ParityShards,
		Verbose:                    cfg.Verbose,
		ExtraVerbose:               cfg.ExtraVerbose,
		ShardSize:                  cfg.ShardSize,
		Host:                       cfg.P2PHost,
		ListenAddress:              cfg.ListenAddress,
		KeepLocalCopy:              cfg.KeepLocalCopy,
		KeepDeletedMinutes:         cfg.KeepDeletedMinutes,
		KeepMetadataMinutes:        cfg.KeepMetadataMinutes,
		WasteThreshold:             cfg.WasteThreshold,
		GCIntervalMinutes:          cfg.GCIntervalMinutes,
		SelfBackupIntervalMinutes:  cfg.SelfBackupIntervalMinutes,
		PeerEvictionHours:          cfg.PeerEvictionHours,
		MaxStorageBytes:            maxBytes,
		BasePieceBuffer:            cfg.BasePieceBuffer,
		ChallengesPerPiece:         cfg.ChallengesPerPiece,
		UploadLimiter:              uploadLimiter,
		DownloadLimiter:            downloadLimiter,
		UntrustedPeerUploadLimitMB: int64(cfg.UntrustedPeerUploadLimitMB),
		ActivePeers:                make(map[int64]rpc.PeerNode),
		StartTime:                  time.Now(),
		MasterKey:                  cfg.MasterKey,
		AdminPublicKey:             cfg.AdminPublicKey,
		ContactInfo:                cfg.ContactInfo,
		StandaloneMode:             cfg.StandaloneMode,
		ConfigPath:                 cfg.ConfigPath,
		AdoptionEnabled:            cfg.AdoptionEnabled,
		AdoptionPeriodMinutes:      cfg.AdoptionPeriodMinutes,
		AdoptionChallengePieces:    cfg.AdoptionChallengePieces,
		BootstrapPeers:             cfg.BootstrapPeers,
		pendingItems:               make(map[string]rpc.Metadata),
		streamSemaphore:            make(chan struct{}, cfg.MaxConcurrentStreams),
		StreamBufferPool: sync.Pool{
			New: func() any {
				return make([]byte, 4*1024*1024) // 4MB buffer specifically for io.CopyBuffer
			},
		},
	}
}

// Wait blocks until all background tasks (like encoding) are finished.
func (e *Engine) Wait() {
	e.wg.Wait()
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

// PrepareUpload checks which of the offered blobs the server already has.
// It returns an UploadPlan containing indices of blobs that need to be uploaded.
// OfferItems checks which of the offered items the server already has.
func (e *Engine) OfferItems(ctx context.Context, pubKey string, items []rpc.Metadata) (rpc.UploadPlan, error) {
	var totalSize int64
	for _, b := range items {
		totalSize += b.Size
	}

	// 1. Quota Check
	isClient, _ := e.isClient(ctx, pubKey)
	if isClient {
		if err := e.AuthorizeAndCheckQuota(ctx, pubKey, totalSize); err != nil {
			return rpc.UploadPlan{}, err
		}
	} else if pubKey != "" {
		// Peer reciprocity check if not client
		if err := e.checkPeerQuota(ctx, pubKey, totalSize); err != nil {
			return rpc.UploadPlan{}, err
		}
	}

	var plan rpc.UploadPlan
	if len(items) == 0 {
		return plan, nil
	}

	// 2. Existence Check (LOCKED for pendingItems update)
	e.mu.Lock()
	defer e.mu.Unlock()

	for i, item := range items {
		exists := false
		if isClient {
			exists = e.HasItem(ctx, item.Hash)
		} else {
			// Peer existence and deduplication check
			var existsGlobally bool
			// Check if we have this hash anywhere in hosted_shards (from any peer)
			e.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM hosted_shards WHERE hash = ?)", item.Hash).Scan(&existsGlobally)

			if existsGlobally {
				peerID, _ := e.GetPeerIDByPubKey(ctx, pubKey)
				if peerID > 0 {
					// Use UPSERT to increment ref_count if already exists for this peer,
					// or insert a new record with ref_count=1 if it doesn't.
					_, err := e.DB.ExecContext(ctx,
						`INSERT INTO hosted_shards (hash, size, peer_id, is_special, piece_index, parent_shard_hash, sequence, total_pieces, ref_count)
						 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
						 ON CONFLICT(hash, peer_id) DO UPDATE SET ref_count = ref_count + 1`,
						item.Hash, item.Size, peerID, item.IsSpecial, item.PieceIndex, item.ParentShardHash, item.SequenceNumber, item.TotalPieces)
					if err == nil {
						// Update peer metrics
						_, _ = e.DB.ExecContext(ctx, "UPDATE peers SET current_storage_size = current_storage_size + ?, total_shards = total_shards + 1, current_shards = current_shards + 1 WHERE id = ?", item.Size, peerID)
						exists = true
					}
				}
			}
		}

		if !exists {
			plan.NeededIndices = append(plan.NeededIndices, int32(i))
			e.pendingItems[item.Hash] = item
		} else if isClient {
			// Ref count bump for existing client blobs
			e.DB.ExecContext(ctx, "UPDATE blobs SET ref_count = ref_count + 1, deleted_at = NULL WHERE hash = ?", item.Hash)
		}
	}

	if e.Verbose {
		log.Printf("Offered %d items, requesting %d missing items from %s", len(items), len(plan.NeededIndices), pubKey)
	}

	return plan, nil
}

func (e *Engine) isClient(ctx context.Context, pubKey string) (bool, error) {
	if pubKey == "insecure-local-client" {
		return true, nil
	}
	var exists bool
	err := e.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM clients WHERE public_key = ?)", pubKey).Scan(&exists)
	return exists, err
}

func (e *Engine) checkPeerQuota(ctx context.Context, pubKeyHex string, incomingSize int64) error {
	var peerID int64
	var status string
	var currentSize int64
	err := e.DB.QueryRowContext(ctx, "SELECT id, status, current_storage_size FROM peers WHERE public_key = ?", pubKeyHex).Scan(&peerID, &status, &currentSize)
	if err != nil {
		return fmt.Errorf("unregistered peer rejected: %w", err)
	}

	if status == "untrusted" {
		myPiecesOnPeer := e.CountUploadedPiecesForPeer(ctx, peerID)
		pieceSize := e.ShardSize / int64(e.DataShards)
		limitBytes := (int64(myPiecesOnPeer) + int64(e.BasePieceBuffer)) * pieceSize
		if currentSize+incomingSize > limitBytes {
			return fmt.Errorf("peer upload quota exceeded: %d bytes over limit", (currentSize+incomingSize)-limitBytes)
		}
	}
	return nil
}

// Hash returns a BLAKE3 hash of the data.
func (e *Engine) Hash(data []byte) []byte {
	return crypto.Hash(data)
}

// EnsureShardLocal ensures that all data piece files for a shard exist on the local disk.
// If any are missing, it attempts to reconstruct them from the swarm.
func (e *Engine) EnsureShardLocal(ctx context.Context, shardID int64) error {
	isMirrored := e.IsShardMirrored(ctx, shardID)

	if isMirrored {
		piecePath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d_piece_0", shardID))
		if _, err := os.Stat(piecePath); err == nil {
			return nil
		}

		if e.Verbose {
			log.Printf("EnsureShardLocal: Mirrored Shard %d piece 0 missing locally. Attempting swarm reconstruction.", shardID)
		}

		rows, err := e.DB.QueryContext(ctx, "SELECT peer_id FROM outbound_pieces WHERE shard_id = ? AND piece_index = 0 AND status = 'uploaded'", shardID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var peerID int64
			if err := rows.Scan(&peerID); err != nil {
				continue
			}

			pieceHash := e.GetPieceHash(ctx, shardID, 0, peerID)

			f, err := os.OpenFile(piecePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				continue
			}

			err = e.PullPiece(ctx, peerID, pieceHash, f)
			f.Close()
			if err != nil {
				os.Remove(piecePath)
				continue
			}
			return nil
		}
		return fmt.Errorf("failed to recover mirrored special shard %d", shardID)
	}

	// For standard shards, check if all DataShards exist locally
	allLocal := true
	shardsData := make([][]byte, e.DataShards+e.ParityShards)
	for i := 0; i < e.DataShards; i++ {
		piecePath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d_piece_%d", shardID, i))
		data, err := os.ReadFile(piecePath)
		if err == nil {
			shardsData[i] = data
		} else {
			allLocal = false
		}
	}

	if allLocal {
		return nil
	}

	if e.Verbose {
		log.Printf("EnsureShardLocal: Shard %d missing pieces locally. Attempting swarm reconstruction.", shardID)
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

	// Fill shardsData from swarm
	piecesFound := 0
	for i := 0; i < e.DataShards; i++ {
		if shardsData[i] != nil {
			piecesFound++
		}
	}

	for _, p := range pieces {
		if piecesFound >= e.DataShards {
			break
		}
		if shardsData[p.index] != nil {
			continue
		}

		pieceHash := e.GetPieceHash(ctx, shardID, p.index, p.peerID)
		err := error(nil)
		if pieceHash == "" {
			err = fmt.Errorf("no piece hash found")
		}
		if err != nil {
			continue
		}

		buf := new(bytes.Buffer)
		err = e.PullPiece(ctx, p.peerID, pieceHash, buf)
		if err != nil {
			log.Printf("EnsureShardLocal: failed to download piece %d from peer %d: %v", p.index, p.peerID, err)
			continue
		}
		data := buf.Bytes()

		// Verify hash
		if hex.EncodeToString(e.Hash(data)) != pieceHash {
			log.Printf("EnsureShardLocal: hash mismatch for piece %d from peer %d", p.index, p.peerID)
			continue
		}

		shardsData[p.index] = data
		piecesFound++
	}

	if piecesFound < e.DataShards {
		return fmt.Errorf("failed to download enough pieces for reconstruction (have %d, need %d)", piecesFound, e.DataShards)
	}

	// Reconstruct missing pieces
	rs, err := reedsolomon.New(e.DataShards, e.ParityShards)
	if err != nil {
		return err
	}

	if err := rs.Reconstruct(shardsData); err != nil {
		return fmt.Errorf("RS reconstruction failed: %w", err)
	}

	// Save reconstructed data pieces back to disk
	for i := 0; i < e.DataShards; i++ {
		piecePath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d_piece_%d", shardID, i))
		if _, err := os.Stat(piecePath); os.IsNotExist(err) {
			if err := os.WriteFile(piecePath, shardsData[i], 0644); err != nil {
				return fmt.Errorf("failed to write reconstructed piece %d: %w", i, err)
			}
		}
	}

	return nil
}

func (e *Engine) HasItem(ctx context.Context, hash string) bool {
	var exists bool
	_ = e.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM blobs WHERE hash = ?)", hash).Scan(&exists)
	return exists
}

// UploadItems saves the provided items to the blob store (client-server data path).
func (e *Engine) UploadItems(ctx context.Context, clientPubKey string, items []rpc.ItemData) error {
	var totalSize int64
	for _, b := range items {
		totalSize += int64(len(b.Data))
	}

	if err := e.AuthorizeAndCheckQuota(ctx, clientPubKey, totalSize); err != nil {
		return err
	}

	return e.IngestItems(ctx, clientPubKey, items, false)
}

// IngestItems handles the saving of multiple items by wrapping IngestItemsStreamed.
// If isGC is true, it skips checksum verification.
func (e *Engine) IngestItems(ctx context.Context, clientPubKey string, items []rpc.ItemData, isGC bool) error {
	for _, b := range items {
		if !isGC {
			calculatedHash := hex.EncodeToString(crypto.Hash(b.Data))
			if calculatedHash != b.Meta.Hash {
				log.Printf("WARNING: Checksum mismatch for item! claimed=%s, actual=%s", b.Meta.Hash, calculatedHash)
				continue
			}
		}

		err := e.IngestItemsStreamed(ctx, clientPubKey, b.Meta.Hash, uint64(len(b.Data)), bytes.NewReader(b.Data))
		if err != nil {
			return fmt.Errorf("failed to ingest item %s: %w", b.Meta.Hash, err)
		}
	}
	return nil
}

// ShardReservation tracks a pre-allocated segment in a shard file.
type ShardReservation struct {
	ShardID    int64
	PieceIndex int
	Offset     int64
	Length     int64
}

// IngestItemsStreamed saves a single item from a stream directly into shards.
func (e *Engine) IngestItemsStreamed(ctx context.Context, clientPubKey string, hash string, size uint64, r io.Reader) error {
	// 1. PHASE 1: Validation & Space Reservation (LOCKED)
	e.mu.Lock()

	// Check if already ingested (using DB)
	var exists int
	err := e.DB.QueryRowContext(ctx, "SELECT 1 FROM blobs WHERE hash = ?", hash).Scan(&exists)
	if err == nil {
		e.mu.Unlock()
		return nil // Already exists
	}

	// Quota Check
	if err := e.CheckGlobalStorageLimit(ctx, int64(size)); err != nil {
		e.mu.Unlock()
		return err
	}

	// Initialize in-memory shard state if needed
	if e.activeShardID == 0 {
		err = e.DB.QueryRowContext(ctx, "SELECT id, size FROM shards WHERE status = 'open' ORDER BY id ASC LIMIT 1").Scan(&e.activeShardID, &e.activeShardSize)
		if err == sql.ErrNoRows {
			res, err := e.DB.ExecContext(ctx, "INSERT INTO shards (status, size, sequence) VALUES ('open', 0, ?)", time.Now().Unix())
			if err != nil {
				e.mu.Unlock()
				return err
			}
			e.activeShardID, _ = res.LastInsertId()
			e.activeShardSize = 0
		} else if err != nil {
			e.mu.Unlock()
			return err
		}
	}

	isSpecial := false
	if meta, ok := e.pendingItems[hash]; ok {
		isSpecial = meta.IsSpecial
	}

	// Perform reservations
	remaining := int64(size)
	var reservations []ShardReservation
	var sealedShards []int64
	targetPieceSize := e.ShardSize / int64(e.DataShards)
	if targetPieceSize == 0 {
		targetPieceSize = 1
	}

	for remaining > 0 {
		if e.activeShardSize >= e.ShardSize {
			// Seal current shard in DB
			totalPieces := e.DataShards + e.ParityShards
			e.DB.ExecContext(ctx, "UPDATE shards SET size = ?, status = 'sealed', total_pieces = ? WHERE id = ?", e.activeShardSize, totalPieces, e.activeShardID)
			sealedShards = append(sealedShards, e.activeShardID)

			// Create new shard
			res, _ := e.DB.ExecContext(ctx, "INSERT INTO shards (status, size, sequence) VALUES ('open', 0, ?)", time.Now().Unix())
			e.activeShardID, _ = res.LastInsertId()
			e.activeShardSize = 0
		}

		spaceInShard := e.ShardSize - e.activeShardSize
		pieceIndex := int(e.activeShardSize / targetPieceSize)
		localOffset := e.activeShardSize % targetPieceSize
		spaceInPiece := targetPieceSize - localOffset

		toWrite := remaining
		if toWrite > spaceInShard {
			toWrite = spaceInShard
		}
		if toWrite > spaceInPiece {
			toWrite = spaceInPiece
		}

		reservations = append(reservations, ShardReservation{
			ShardID:    e.activeShardID,
			PieceIndex: pieceIndex,
			Offset:     localOffset,
			Length:     toWrite,
		})

		e.activeShardSize += toWrite
		remaining -= toWrite
	}

	// Important: Immediately update the current open shard size in DB so other engine instances (if any) see it
	e.DB.ExecContext(ctx, "UPDATE shards SET size = ? WHERE id = ?", e.activeShardSize, e.activeShardID)

	e.mu.Unlock() // <<< RELEASE LOCK FOR STREAMING >>>

	// 2. PHASE 2: Data Streaming (UNLOCKED)
	for _, res := range reservations {
		shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d_piece_%d", res.ShardID, res.PieceIndex))
		f, err := os.OpenFile(shardPath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		// Phase 2: Unlocked Streaming
		// We write to the reserved segment using WriteAt to ensure concurrency safety.
		// Multiple streams can safely write to different parts of the same file concurrently.
		buf := e.StreamBufferPool.Get().([]byte)

		var written int64
		for written < res.Length {
			toRead := int64(len(buf))
			if toRead > res.Length-written {
				toRead = res.Length - written
			}

			nr, er := r.Read(buf[:toRead])
			if nr > 0 {
				nw, ew := f.WriteAt(buf[:nr], res.Offset+written)
				if nw > 0 {
					written += int64(nw)
				}
				if ew != nil {
					e.StreamBufferPool.Put(buf)
					return fmt.Errorf("shard write failed at offset %d: %w", res.Offset+written, ew)
				}
			}
			if er != nil {
				if er == io.EOF && written < res.Length {
					e.StreamBufferPool.Put(buf)
					return fmt.Errorf("unexpected EOF: got %d, expected %d", written, res.Length)
				}
				if er != io.EOF {
					e.StreamBufferPool.Put(buf)
					return er
				}
				break
			}
		}
		e.StreamBufferPool.Put(buf)
		f.Close()
	}

	// 3. PHASE 3: DB Finalization (Re-Lock)
	e.mu.Lock()
	defer e.mu.Unlock()

	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO blobs (hash, size, special) VALUES (?, ?, ?)", hash, size, isSpecial)
	if err != nil {
		return err
	}

	for i, res := range reservations {
		tx.ExecContext(ctx, "INSERT INTO blob_locations (blob_hash, shard_id, piece_index, offset, length, sequence) VALUES (?, ?, ?, ?, ?, ?)", hash, res.ShardID, res.PieceIndex, res.Offset, res.Length, i)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Trigger async encoding for any shards we sealed
	for _, sid := range sealedShards {
		e.wg.Add(1)
		go e.encodeShard(sid)
	}

	if clientPubKey != "" && clientPubKey != "insecure-local-client" {
		e.DB.ExecContext(ctx, "UPDATE clients SET current_storage_size = current_storage_size + ? WHERE public_key = ?", size, clientPubKey)
	}

	delete(e.pendingItems, hash)
	return nil
}

// GetHostedItems retrieves peer shards hosted by this server. Returns found items, missing hashes, and error.
func (e *Engine) GetHostedItems(ctx context.Context, hashes []string) ([]rpc.ItemData, []string, error) {
	var found []rpc.ItemData
	var missing []string

	for _, h := range hashes {
		path := filepath.Join(e.BlobStoreDir, "peer_"+h)
		fullData, err := os.ReadFile(path)
		if err != nil {
			missing = append(missing, h)
			continue
		}

		found = append(found, rpc.ItemData{
			Meta: rpc.Metadata{
				Hash:      h,
				Size:      int64(len(fullData)),
				IsSpecial: false,
			},
			Data: fullData,
		})
	}

	return found, missing, nil
}

// GetItems retrieves the requested items. Returns found items, missing hashes, and error.
func (e *Engine) GetItems(ctx context.Context, hashes []string) ([]rpc.ItemData, []string, error) {
	var found []rpc.ItemData
	var missing []string

	for _, h := range hashes {
		var isSpecial bool
		err := e.DB.QueryRowContext(ctx, "SELECT special FROM blobs WHERE hash = ?", h).Scan(&isSpecial)
		if err != nil {
			log.Printf("GetItems: blob not found for %s: %v", h, err)
			missing = append(missing, h)
			continue
		}

		// Find shard locations
		rows, err := e.DB.QueryContext(ctx, "SELECT shard_id, piece_index, offset, length FROM blob_locations WHERE blob_hash = ? ORDER BY sequence ASC", h)
		if err != nil {
			log.Printf("GetItems: failed to query locations for %s: %v", h, err)
			missing = append(missing, h)
			continue
		}

		var fullData []byte
		foundLocations := false
		for rows.Next() {
			var shardID, pieceIndex, offset, length int64
			if err := rows.Scan(&shardID, &pieceIndex, &offset, &length); err != nil {
				continue
			}
			foundLocations = true

			shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d_piece_%d", shardID, pieceIndex))
			f, err := os.Open(shardPath)
			if err != nil {
				log.Printf("GetItems error: failed to open shard piece %d for shard %d: %v", pieceIndex, shardID, err)
				break
			}
			data := make([]byte, length)
			_, err = f.ReadAt(data, offset)
			f.Close()
			if err != nil {
				log.Printf("GetItems error: failed to read from shard piece %d for shard %d: %v", pieceIndex, shardID, err)
				break
			}
			fullData = append(fullData, data...)
		}
		rows.Close()

		if !foundLocations {
			// Fallback to direct file if no locations (legacy or standalone)
			path := filepath.Join(e.BlobStoreDir, h)
			var err error
			fullData, err = os.ReadFile(path)
			if err != nil {
				log.Printf("GetItems: failed to read legacy blob %s: %v", h, err)
				missing = append(missing, h)
				continue
			}
		}

		found = append(found, rpc.ItemData{
			Meta: rpc.Metadata{
				Hash:      h,
				Size:      int64(len(fullData)),
				IsSpecial: isSpecial,
			},
			Data: fullData,
		})
	}

	return found, missing, nil
}

// DeleteItems marks blobs as deleted in the database.
func (e *Engine) DeleteItems(ctx context.Context, hashes []string) error {
	if len(hashes) == 0 {
		return nil
	}
	placeholders := make([]string, len(hashes))
	args := make([]interface{}, len(hashes))
	for i, h := range hashes {
		placeholders[i] = "?"
		args[i] = h
	}
	query := fmt.Sprintf("UPDATE blobs SET deleted_at = CURRENT_TIMESTAMP WHERE hash IN (%s)", strings.Join(placeholders, ","))
	_, err := e.DB.ExecContext(ctx, query, args...)
	return err
}

// ListAllItems returns hashes of all non-deleted blobs.
func (e *Engine) ListAllItems(ctx context.Context) ([]string, error) {
	rows, err := e.DB.QueryContext(ctx, "SELECT hash FROM blobs WHERE deleted_at IS NULL")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hashes []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err == nil {
			hashes = append(hashes, h)
		}
	}
	return hashes, nil
}

// ListSpecialItems returns metadata for all special items (client-server data path).
func (e *Engine) ListSpecialItems(ctx context.Context, pubKey string) ([]rpc.Metadata, error) {
	// First check if it's a client
	var isClient bool
	e.DB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM clients WHERE public_key = ?)", pubKey).Scan(&isClient)

	if isClient || pubKey == "insecure-local-client" {
		rows, err := e.DB.QueryContext(ctx, "SELECT hash, size, special FROM blobs WHERE special = 1 AND deleted_at IS NULL")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var items []rpc.Metadata
		for rows.Next() {
			var m rpc.Metadata
			if err := rows.Scan(&m.Hash, &m.Size, &m.IsSpecial); err == nil {
				m.Type = 0 // ClientBlob
				items = append(items, m)
			}
		}
		return items, nil
	}

	// Otherwise treat as a peer
	rows, err := e.DB.QueryContext(ctx, "SELECT hash, size, piece_index, parent_shard_hash, sequence, total_pieces FROM hosted_shards WHERE peer_id = (SELECT id FROM peers WHERE public_key = ?) AND is_special = 1", pubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to query special pieces: %w", err)
	}
	defer rows.Close()

	var items []rpc.Metadata
	for rows.Next() {
		var m rpc.Metadata
		if err := rows.Scan(&m.Hash, &m.Size, &m.PieceIndex, &m.ParentShardHash, &m.SequenceNumber, &m.TotalPieces); err == nil {
			m.IsSpecial = true
			m.Type = 1 // PeerShard
			items = append(items, m)
		}
	}
	return items, nil
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

	// In standalone mode, skip erasure coding entirely
	if e.StandaloneMode {
		if e.Verbose {
			log.Printf("encodeShard: skipping shard %d (standalone mode)", shardID)
		}
		return
	}
	if e.Verbose {
		log.Printf("Starting erasure coding for shard %d", shardID)
	}

	shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d_piece_0", shardID))
	inFile, err := os.Open(shardPath)
	if err != nil {
		log.Printf("encodeShard error: failed to open piece_0 for shard %d: %v", shardID, err)
		return
	}
	defer inFile.Close()

	_, err = inFile.Stat()
	if err != nil {
		log.Printf("encodeShard error: failed to stat piece_0 for shard %d: %v", shardID, err)
		return
	}

	// Calculate target piece size
	targetPieceSize := e.ShardSize / int64(e.DataShards)

	// Check if this is a mirrored (metadata) shard
	isMirrored := e.IsShardMirrored(context.Background(), shardID)

	if isMirrored {
		// Mirroring profile: Every peer gets a full copy of the shard data,
		// padded to the target piece size (e.g. 256MB).
		if e.Verbose {
			log.Printf("encodeShard: Shard %d is MIRRORED. Padded to %d MB piece.", shardID, targetPieceSize/(1024*1024))
		}

		// For special shards, Piece 0 is just a copy of the shard itself, padded to targetPieceSize.
		dstPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d_piece_0.padded", shardID))
		dst, err := os.Create(dstPath)
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
		inFile.Close() // Close early so we can remove it

		// Save the padded hash to DB so syncMirroredShards can find it
		shardHash := hex.EncodeToString(hasher.Sum(nil))
		_, err = e.DB.Exec("UPDATE shards SET hash = ? WHERE id = ?", shardHash, shardID)
		if err != nil {
			log.Printf("encodeShard error: failed to update shard hash in DB: %v", err)
		}

		// Replace the unpadded piece with the padded one in BlobStoreDir
		os.Rename(dstPath, shardPath)

		if !e.KeepLocalCopy {
			// syncMirroredShards will pick it up from BlobStoreDir and then outbound_worker handles deleting it
			// Wait, outbound_worker only deletes if count >= threshold.
		}

		log.Printf("ErasureCoder: Successfully prepared special shard %d for mirroring (padded to %d MB, hash: %s)", shardID, targetPieceSize/(1024*1024), shardHash[:16])
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

	inFiles := make([]*os.File, e.DataShards)
	inReaders := make([]io.Reader, e.DataShards)
	hasher := blake3.New(32, nil)

	// Close inFile since we are going to open all N pieces
	inFile.Close()

	for i := 0; i < e.DataShards; i++ {
		srcPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d_piece_%d", shardID, i))
		var reader io.Reader
		inf, err := os.Open(srcPath)
		if err != nil {
			if os.IsNotExist(err) {
				reader = bytes.NewReader([]byte{})
			} else {
				log.Printf("encodeShard error: failed to open source piece %d: %v", i, err)
				for j := 0; j < i; j++ {
					inFiles[j].Close()
				}
				return
			}
		} else {
			reader = inf
		}

		dstPath := filepath.Join(e.QueueDir, fmt.Sprintf("shard_%d_piece_%d.tmp", shardID, i))
		outFile, err := os.Create(dstPath)
		if err != nil {
			if inf != nil {
				inf.Close()
			}
			for j := 0; j < i; j++ {
				inFiles[j].Close()
			}
			return
		}

		// Pad to exactly targetPieceSize
		pr := &PadReader{Reader: reader, TotalSize: targetPieceSize}
		teeReader := io.TeeReader(pr, hasher)

		if _, err := io.Copy(outFile, teeReader); err != nil {
			log.Printf("encodeShard error: failed to copy/pad piece %d: %v", i, err)
			outFile.Close()
			if inf != nil {
				inf.Close()
			}
			for j := 0; j < i; j++ {
				inFiles[j].Close()
			}
			return
		}

		if inf != nil {
			inf.Close()
			if !e.KeepLocalCopy {
				os.Remove(srcPath)
			}
		}

		outFile.Seek(0, 0)
		inFiles[i] = outFile
		inReaders[i] = outFile
	}

	shardHash := hex.EncodeToString(hasher.Sum(nil))
	_, err = e.DB.Exec("UPDATE shards SET hash = ? WHERE id = ?", shardHash, shardID)
	if err != nil {
		log.Printf("encodeShard error: failed to update shard hash in DB: %v", err)
	}

	outFiles := make([]*os.File, e.ParityShards)
	outWriters := make([]io.Writer, e.ParityShards)

	for i := 0; i < e.ParityShards; i++ {
		outPath := filepath.Join(e.QueueDir, fmt.Sprintf("shard_%d_piece_%d.tmp", shardID, e.DataShards+i))
		f, err := os.Create(outPath)
		if err != nil {
			log.Printf("encodeShard error: failed to create parity piece %d: %v", i, err)
			// Cleanup
			for j := 0; j < i; j++ {
				outFiles[j].Close()
			}
			for j := 0; j < e.DataShards; j++ {
				inFiles[j].Close()
			}
			return
		}
		outFiles[i] = f
		outWriters[i] = f
	}

	// 2. Encode to generate K parity pieces
	err = enc.Encode(inReaders, outWriters)
	if err != nil {
		log.Printf("encodeShard error: failed to encode parity for shard %d: %v", shardID, err)
	} else if e.Verbose {
		log.Printf("ErasureCoder: Successfully erasure coded shard %d into %d pieces", shardID, e.DataShards+e.ParityShards)
	}

	// 3. Finalize all pieces: Close and rename to remove .tmp suffix
	for i := 0; i < e.DataShards; i++ {
		inFiles[i].Close()
		if err == nil {
			tmpPath := filepath.Join(e.QueueDir, fmt.Sprintf("shard_%d_piece_%d.tmp", shardID, i))
			finalPath := filepath.Join(e.QueueDir, fmt.Sprintf("shard_%d_piece_%d", shardID, i))
			os.Rename(tmpPath, finalPath)
		}
	}

	for i := 0; i < e.ParityShards; i++ {
		outFiles[i].Close()
		if err == nil {
			tmpPath := filepath.Join(e.QueueDir, fmt.Sprintf("shard_%d_piece_%d.tmp", shardID, e.DataShards+i))
			finalPath := filepath.Join(e.QueueDir, fmt.Sprintf("shard_%d_piece_%d", shardID, e.DataShards+i))
			os.Rename(tmpPath, finalPath)
		}
	}
}

// PreparePeerUpload records metadata for shards that a peer is about to push via a stream.
func (e *Engine) PreparePeerUpload(ctx context.Context, pubKeyHex string, items []rpc.Metadata) error {
	var peerID int64
	err := e.DB.QueryRowContext(ctx, "SELECT id FROM peers WHERE public_key = ?", pubKeyHex).Scan(&peerID)
	if err != nil {
		return fmt.Errorf("unregistered peer rejected: %w", err)
	}

	for _, m := range items {
		// Record metadata for subsequent push streams
		e.pendingInboundStreams.Store(m.Hash, PendingStreamMeta{
			PeerID:          peerID,
			IsSpecial:       m.IsSpecial,
			PieceIndex:      m.PieceIndex,
			ParentShardHash: m.ParentShardHash,
			SequenceNumber:  m.SequenceNumber,
			TotalPieces:     m.TotalPieces,
			Size:            uint64(m.Size),
		})
	}
	return nil
}

// UploadPeerItems receives the actual peer shard data and saves it to disk (peer-to-peer data path).
func (e *Engine) UploadPeerItems(ctx context.Context, pubKeyHex string, items []rpc.ItemData) error {
	if pubKeyHex != "" {
		// Re-verify quota
		var status string
		var currentSize, maxStorageBytes int64
		var peerID int64
		err := e.DB.QueryRowContext(ctx, "SELECT id, status, current_storage_size, max_storage_size FROM peers WHERE public_key = ?", pubKeyHex).Scan(&peerID, &status, &currentSize, &maxStorageBytes)
		if err == nil {
			var incomingSize int64
			for _, item := range items {
				incomingSize += int64(len(item.Data))
			}

			limitBytes := maxStorageBytes
			if status == "untrusted" {
				myPiecesOnPeer := e.CountUploadedPiecesForPeer(ctx, peerID)
				pieceSize := e.ShardSize / int64(e.DataShards)
				limitBytes = (int64(myPiecesOnPeer) + int64(e.BasePieceBuffer)) * pieceSize
			}

			if limitBytes > 0 && currentSize+incomingSize > limitBytes {
				return fmt.Errorf("peer upload quota exceeded")
			}

			// Global server limit check
			if err := e.CheckGlobalStorageLimit(ctx, incomingSize); err != nil {
				return err
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

	for _, item := range items {
		shardPath := filepath.Join(e.BlobStoreDir, "peer_"+item.Meta.Hash)

		// Write to disk
		if err := os.WriteFile(shardPath, item.Data, 0644); err != nil {
			return fmt.Errorf("failed to write peer shard %s to disk: %w", item.Meta.Hash, err)
		}

		// Update DB
		res, err := stmt.ExecContext(ctx, item.Meta.Hash, len(item.Data), pubKeyHex, item.Meta.IsSpecial, item.Meta.PieceIndex, item.Meta.ParentShardHash, item.Meta.SequenceNumber, item.Meta.TotalPieces)
		if err != nil {
			return fmt.Errorf("failed to insert peer shard %s into db: %w", item.Meta.Hash, err)
		}

		rowsAffected, _ := res.RowsAffected()
		if rowsAffected > 0 && pubKeyHex != "" && pubKeyHex != "insecure-local-client" {
			_, err = tx.ExecContext(ctx, "UPDATE peers SET current_storage_size = current_storage_size + ?, total_shards = total_shards + 1, current_shards = current_shards + 1 WHERE public_key = ?", int64(len(item.Data)), pubKeyHex)
			if err != nil {
				return fmt.Errorf("failed to update peer storage size: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	if e.Verbose {
		log.Printf("Successfully saved %d peer shards to storage", len(items))
	}

	return nil
}

// GetStatus returns the current runtime metrics and replication state of the server.
func (e *Engine) GetStatus(ctx context.Context) (rpc.StatusInfo, error) {
	status := rpc.StatusInfo{
		UptimeSeconds: uint64(time.Since(e.StartTime).Seconds()),
	}

	// 1. Total local full (sealed) shards
	err := e.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM shards WHERE status = 'sealed'").Scan(&status.TotalShards)
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
					WHEN s.mirrored = 1 AND COUNT(DISTINCT op.peer_id) >= 2 THEN 1
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

	var peerID int64
	_ = e.DB.QueryRowContext(ctx, "SELECT id FROM peers WHERE public_key = ?", pubKeyHex).Scan(&peerID)

	var count int
	err := e.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM hosted_shards WHERE hash = ? AND peer_id = ?", hashStr, peerID).Scan(&count)
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

	// Decrement ref_count and delete only if it reaches 0
	_, err = tx.ExecContext(ctx, `
		UPDATE hosted_shards 
		SET ref_count = MAX(0, ref_count - 1) 
		WHERE hash = ? AND peer_id = (SELECT id FROM peers WHERE public_key = ?)
	`, hashStr, pubKeyHex)
	if err != nil {
		return fmt.Errorf("failed to decrement ref_count: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		DELETE FROM hosted_shards 
		WHERE hash = ? AND peer_id = (SELECT id FROM peers WHERE public_key = ?) AND ref_count <= 0
	`, hashStr, pubKeyHex)
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

	// 3. Check if anyone else hosts this shard piece before deleting the physical file
	var othersCount int
	_ = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM hosted_shards WHERE hash = ?", hashStr).Scan(&othersCount)

	if err := tx.Commit(); err != nil {
		return err
	}

	if othersCount == 0 {
		shardPath := filepath.Join(e.BlobStoreDir, "peer_"+hashStr)
		if err := os.Remove(shardPath); err != nil && !os.IsNotExist(err) {
			log.Printf("Warning: failed to remove shard file %s: %v", shardPath, err)
		}
	}

	if e.Verbose {
		log.Printf("Successfully released piece %s for peer %s", hashStr, pubKeyHex[:16])
	}

	return nil
}
