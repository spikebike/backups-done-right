package server

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	crypto_rand "crypto/rand"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/reedsolomon"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"golang.org/x/time/rate"
	"lukechampine.com/blake3"
	"p2p-backup/internal/config"
	"p2p-backup/internal/crypto"
	capnp "capnproto.org/go/capnp/v3"
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
	AdoptionEnabled        bool
	AdoptionPeriodMinutes  int
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

// NewEngine creates a new server Engine.
func NewEngine(db *sql.DB, configPath string, sqlitePath string, blobStoreDir, queueDir string, dataShards, parityShards int, shardSize int64, keepLocalCopy bool, p2pHost host.Host, listenAddress string, untrustedLimitMB int, verbose bool, extraVerbose bool, challengesPerPiece int, keepDeletedMinutes int, keepMetadataMinutes int, wasteThreshold float64, gcIntervalMinutes int, selfBackupIntervalMinutes int, peerEvictionHours int, basePieceBuffer int, maxStorageGB int, maxUploadKBPS int, maxDownloadKBPS int, masterKey []byte, adminPublicKey string, contactInfo string, maxConcurrentStreams int, standaloneMode bool, adoptionEnabled bool, adoptionPeriodMinutes int, adoptionChallengePieces int, bootstrapPeers []string) *Engine {
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
		ChallengesPerPiece:         challengesPerPiece,
		UploadLimiter:              uploadLimiter,
		DownloadLimiter:            downloadLimiter,
		UntrustedPeerUploadLimitMB: int64(untrustedLimitMB),
		ActivePeers:                make(map[int64]rpc.PeerNode),
		StartTime:                  time.Now(),
		MasterKey:                  masterKey,
		AdminPublicKey:             adminPublicKey,
		ContactInfo:                contactInfo,
		StandaloneMode:             standaloneMode,
		ConfigPath:                 configPath,
		AdoptionEnabled:            adoptionEnabled,
		AdoptionPeriodMinutes:      adoptionPeriodMinutes,
		AdoptionChallengePieces:    adoptionChallengePieces,
		BootstrapPeers:             bootstrapPeers,
		pendingItems:               make(map[string]rpc.Metadata),
		streamSemaphore:            make(chan struct{}, maxConcurrentStreams),
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

// AttemptRescue attempts to recover the server database from a peer that has a mirrored copy of the special metadata shard.
func (e *Engine) AttemptRescue(ctx context.Context, peer rpc.PeerNode, pid peer.ID, destDir string) error {
	if e.Verbose {
		log.Println("RESCUE: Attempting to recover database from peer...")
	}

	// 1. Get special pieces from peer
	req, release := peer.ListSpecialItems(ctx, nil)
	defer release()
	res, err := req.Struct()
	if err != nil {
		return fmt.Errorf("list special items: %w", err)
	}
	items, _ := res.Items()
	if items.Len() == 0 {
		return fmt.Errorf("no special pieces found on peer")
	}

	// 2. Identify the newest mirrored shard
	var highestSeq uint64
	var targetPiece rpc.Metadata
	found := false
	for i := 0; i < items.Len(); i++ {
		s := rpc.MetadataFromCapnp(items.At(i))
		if s.IsSpecial && (s.SequenceNumber > highestSeq || !found) {
			highestSeq = s.SequenceNumber
			targetPiece = s
			found = true
		}
	}

	if !found {
		return fmt.Errorf("no special mirrored shards found")
	}

	// 3. Download the piece
	hashHex := targetPiece.Hash

	buf := new(bytes.Buffer)
	err = e.PullPieceRaw(ctx, pid, hashHex, buf)
	if err != nil {
		return fmt.Errorf("pull piece direct: %w", err)
	}
	data := buf.Bytes()

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

	// 3.5 Verify Signature (first 64 bytes)
	if len(actualData) < 64 {
		return fmt.Errorf("recovery shard is too small to contain signature")
	}
	signature := actualData[:64]
	ciphertext := actualData[64:]

	if !crypto.VerifyRecoveryShard(e.MasterKey, signature, ciphertext) {
		return fmt.Errorf("recovery shard signature verification failed (corruption or malicious peer)")
	}

	// 4. Decrypt Bundle
	decryptedBundle, err := crypto.Decrypt(e.MasterKey, ciphertext)
	if err != nil {
		return fmt.Errorf("bundle decryption failed (wrong mnemonic or corruption): %w", err)
	}

	// 5. Unpack Bundle
	decoder, err := zstd.NewReader(bytes.NewReader(decryptedBundle))
	if err != nil {
		return fmt.Errorf("zstd init failed: %w", err)
	}
	defer decoder.Close()

	tr := tar.NewReader(decoder)

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination dir: %w", err)
	}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar read error: %w", err)
		}

		outPath := filepath.Join(destDir, hdr.Name)
		outFile, err := os.Create(outPath)
		if err != nil {
			return fmt.Errorf("failed to create recovered file %s: %w", outPath, err)
		}
		
		if _, err := io.Copy(outFile, tr); err != nil {
			outFile.Close()
			return fmt.Errorf("failed to write data to %s: %w", outPath, err)
		}
		outFile.Close()
		
		if e.Verbose {
			log.Printf("RESCUE: Recovered file: %s", outPath)
		}
	}

	// Generate add_recovered_peers.sh script
	peersJSONPath := filepath.Join(destDir, "peers.json")
	if peersData, err := os.ReadFile(peersJSONPath); err == nil {
		scriptPath := filepath.Join(destDir, "add_recovered_peers.sh")
		scriptContent := "#!/bin/bash\necho 'Adding recovered peers...'\n"
		
		var discoveryList []PeerDiscoveryInfo
		if err := json.Unmarshal(peersData, &discoveryList); err == nil {
			for _, p := range discoveryList {
				if len(p.Endpoints) > 0 {
					scriptContent += fmt.Sprintf("./client addpeer %s\n", p.Endpoints[0])
				}
			}
			os.WriteFile(scriptPath, []byte(scriptContent), 0755)
			if e.Verbose {
				log.Printf("RESCUE: Generated peer recovery script at %s", scriptPath)
			}
		}
	}

	if e.Verbose {
		log.Printf("RESCUE: SUCCESS! Recovered bundle to %s", destDir)
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
		var myPiecesOnPeer int
		_ = e.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbound_pieces WHERE peer_id = ? AND status = 'uploaded'", peerID).Scan(&myPiecesOnPeer)
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

			f, err := os.OpenFile(shardPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				continue
			}

			err = e.PullPiece(ctx, p.peerID, pieceHash, f)
			f.Close()
			if err != nil {
				os.Remove(shardPath)
				continue
			}

			return nil
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
	ShardID int64
	Offset  int64
	Length  int64
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
	var localTotal, peerTotal int64
	_ = e.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(size), 0) FROM shards").Scan(&localTotal)
	_ = e.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(size), 0) FROM hosted_shards").Scan(&peerTotal)
	if e.MaxStorageBytes > 0 && (localTotal+peerTotal+int64(size)) > e.MaxStorageBytes {
		e.mu.Unlock()
		return fmt.Errorf("global server storage limit exceeded")
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
		toWrite := remaining
		if toWrite > spaceInShard {
			toWrite = spaceInShard
		}

		reservations = append(reservations, ShardReservation{
			ShardID: e.activeShardID,
			Offset:  e.activeShardSize,
			Length:  toWrite,
		})

		e.activeShardSize += toWrite
		remaining -= toWrite
	}

	// Important: Immediately update the current open shard size in DB so other engine instances (if any) see it
	e.DB.ExecContext(ctx, "UPDATE shards SET size = ? WHERE id = ?", e.activeShardSize, e.activeShardID)

	e.mu.Unlock() // <<< RELEASE LOCK FOR STREAMING >>>

	// 2. PHASE 2: Data Streaming (UNLOCKED)
	for _, res := range reservations {
		shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d.dat", res.ShardID))
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
		tx.ExecContext(ctx, "INSERT INTO blob_locations (blob_hash, shard_id, offset, length, sequence) VALUES (?, ?, ?, ?, ?)", hash, res.ShardID, res.Offset, res.Length, i)
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
			log.Printf("GetHostedItems failed for %s: %v", h, err); missing = append(missing, h)
			continue
		}

		// Find shard locations
		rows, err := e.DB.QueryContext(ctx, "SELECT shard_id, offset, length FROM blob_locations WHERE blob_hash = ? ORDER BY sequence ASC", h)
		if err != nil {
			log.Printf("GetItems error: failed to query locations for %s: %v", h, err)
			log.Printf("GetHostedItems failed for %s: %v", h, err); missing = append(missing, h)
			continue
		}

		var fullData []byte
		foundLocations := false
		for rows.Next() {
			var shardID, offset, length int64
			if err := rows.Scan(&shardID, &offset, &length); err != nil {
				continue
			}
			foundLocations = true

			shardPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("shard_%d.dat", shardID))
			f, err := os.Open(shardPath)
			if err != nil {
				log.Printf("GetItems error: failed to open shard %d: %v", shardID, err)
				break
			}
			data := make([]byte, length)
			_, err = f.ReadAt(data, offset)
			f.Close()
			if err != nil {
				log.Printf("GetItems error: failed to read from shard %d: %v", shardID, err)
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
				log.Printf("GetHostedItems failed for %s: %v", h, err); missing = append(missing, h)
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
					return fmt.Errorf("global server storage limit exceeded (%d GB)", e.MaxStorageBytes/(1024*1024*1024))
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

	pubKeyHex, err := crypto.PubKeyHexFromPeerID(addrInfo.ID)
	if err != nil {
		return fmt.Errorf("failed to extract public key from peer ID: %w", err)
	}

	// 1. Validate and register in database first (Resilient Registration)
	// Store the base address without the peer ID for cleaner dialing later
	baseAddr, _ := multiaddr.SplitFunc(maddr, func(c multiaddr.Component) bool {
		return c.Protocol().Code == multiaddr.P_P2P
	})

	query := `
		INSERT INTO peers (ip_address, public_key, status, first_seen, last_seen, is_manual, source)
		VALUES (?, ?, 'untrusted', CURRENT_TIMESTAMP, NULL, 1, 'manual')
		ON CONFLICT(public_key) DO UPDATE SET 
			ip_address=excluded.ip_address,
			is_manual=1,
			source='manual'
	`
	_, err = e.DB.ExecContext(ctx, query, baseAddr.String(), pubKeyHex)
	if err != nil {
		return fmt.Errorf("failed to register peer in database: %w", err)
	}

	// 2. Fetch the internal ID to use for the RPC dialer
	var internalID int64
	err = e.DB.QueryRowContext(ctx, "SELECT id FROM peers WHERE public_key = ?", pubKeyHex).Scan(&internalID)
	if err != nil {
		return fmt.Errorf("failed to retrieve peer id: %w", err)
	}

	// 3. Proactively trigger a handshake (Announce)
	// GetOrDialPeer will connect, announce us, and receive their announce (updating contact_info)
	client, err := e.GetOrDialPeer(ctx, internalID)
	if err != nil {
		log.Printf("AddPeer: Registered peer %s, but handshake failed: %v. Metadata will update later.", pubKeyHex, err)
		return nil
	}
	client.Close() // Release the ref since we don't need it immediately

	return nil
}

// RegisterAndHandshakeDHT is called by the DiscoveryWorker when a new peer is found on the DHT.
// It registers the peer with 'source=dht' and triggers a handshake to get their metadata.
func (e *Engine) RegisterAndHandshakeDHT(ctx context.Context, info peer.AddrInfo) error {
	pubKeyHex, err := crypto.PubKeyHexFromPeerID(info.ID)
	if err != nil {
		return err
	}

	// 1. Initial registration as 'discovered'/'dht'
	// We use e.AnnouncePeer logic but we don't have contact info yet.
	// Note: e.AnnouncePeer uses ON CONFLICT DO UPDATE, which is exactly what we want.
	addrStr := ""
	if len(info.Addrs) > 0 {
		addrStr = info.Addrs[0].String()
	}
	if addrStr == "" {
		return fmt.Errorf("no address for discovered peer %s", info.ID)
	}

	peerID, err := e.AnnouncePeer(ctx, pubKeyHex, addrStr, "")
	if err != nil {
		return err
	}

	// 2. Trigger handshake to get their actual metadata (contact_info)
	// GetOrDialPeer will call Announce, and they will Announce back.
	client, err := e.GetOrDialPeer(ctx, peerID)
	if err != nil {
		return err
	}
	defer client.Close()

	// 3. Trigger Automatic Adoption if enabled
	if e.AdoptionEnabled && e.AdoptionChallengePieces > 0 {
		var status string
		err = e.DB.QueryRowContext(ctx, "SELECT adoption_status FROM peers WHERE id = ?", peerID).Scan(&status)
		if err == nil && status == "none" {
			go func() {
				if err := e.StartAdoptionTest(context.Background(), peerID); err != nil {
					log.Printf("Adoption: Failed to start test for peer %d: %v", peerID, err)
				}
			}()
		}
	}

	return nil
}

func (e *Engine) StartAdoptionTest(ctx context.Context, peerID int64) error {
	if e.Verbose {
		log.Printf("Adoption: Starting test for peer %d (%d pieces)...", peerID, e.AdoptionChallengePieces)
	}

	_, err := e.DB.ExecContext(ctx, "UPDATE peers SET adoption_status = 'testing' WHERE id = ?", peerID)
	if err != nil {
		return err
	}

	pieceSize := int64(256 * 1024 * 1024) // 256MB adoption test pieces

	for i := 0; i < e.AdoptionChallengePieces; i++ {
		// Generate random data stream and calculate hash on the fly
		hasher := blake3.New(32, nil)
		// We use a repeatable but unique seed for this piece
		seed := make([]byte, 32)
		crypto_rand.Read(seed)
		
		// Create a reader that generates random data
		randomReader := &randomDataStream{
			total:  pieceSize,
			seed:   seed,
			hasher: hasher,
		}

		hashHex := ""
		
		// Wrap in a reader that captures the hash when fully read
		var streamItems []rpc.StreamItem
		streamItems = append(streamItems, rpc.StreamItem{
			Header: rpc.StreamItemHeader{
				OpCode: rpc.OpCodePush,
				Flags:  rpc.FlagTypePeerShard,
				Size:   uint64(pieceSize),
			},
			Data: randomReader,
		})

		// We need the hash BEFORE we tell the peer the hash in the header...
		// Wait, PushPieceBatched takes the hash in the header.
		// So we actually have to generate the hash first if we want to use the standard Push mechanism.
		// Since it's only 256MB and we want to avoid disk, let's just generate it in a quick pass.
		// Actually, we can just pre-calculate it by running the random generator once.
		
		preHasher := blake3.New(32, nil)
		preReader := &randomDataStream{total: pieceSize, seed: seed, hasher: preHasher}
		io.Copy(io.Discard, preReader)
		hashHex = hex.EncodeToString(preHasher.Sum(nil))
		hashBytes, _ := hex.DecodeString(hashHex)
		copy(streamItems[0].Header.Hash[:], hashBytes)

		// RESET the reader for the actual upload
		randomReader.reset()

		if e.Verbose {
			log.Printf("Adoption: Uploading test piece %d/%d (hash=%s) to peer %d", i+1, e.AdoptionChallengePieces, hashHex[:12], peerID)
		}

		client, err := e.GetOrDialPeer(ctx, peerID)
		if err != nil {
			e.DB.ExecContext(ctx, "UPDATE peers SET adoption_status = 'failed' WHERE id = ?", peerID)
			return fmt.Errorf("failed to dial peer for adoption: %w", err)
		}

		meta := []rpc.Metadata{
			{
				Hash:       hashHex,
				Size:       pieceSize,
				IsSpecial:  false, // Adoption piece doesn't need to be strictly special on the receiver side
				PieceIndex: 0,
			},
		}

		if err := client.PrepareUpload(ctx, meta); err != nil {
			e.DB.ExecContext(ctx, "UPDATE peers SET adoption_status = 'failed' WHERE id = ?", peerID)
			return fmt.Errorf("failed to prepare upload for adoption piece: %w", err)
		}

		err = e.PushPieceBatched(ctx, peerID, streamItems)
		if err != nil {
			e.DB.ExecContext(ctx, "UPDATE peers SET adoption_status = 'failed' WHERE id = ?", peerID)
			return fmt.Errorf("failed to upload adoption piece: %w", err)
		}

		// Record the piece in hosted_shards with is_special = 2
		_, err = e.DB.ExecContext(ctx, "INSERT INTO hosted_shards (hash, size, peer_id, is_special) VALUES (?, ?, ?, 2)", hashHex, pieceSize, peerID)
		if err != nil {
			return err
		}
	}

	e.DB.ExecContext(ctx, "UPDATE peers SET adoption_start_at = CURRENT_TIMESTAMP WHERE id = ?", peerID)

	return nil
}

type randomDataStream struct {
	total  int64
	read   int64
	seed   []byte
	hasher io.Writer
	source *rand.Rand
}

func (s *randomDataStream) Read(p []byte) (n int, err error) {
	if s.read >= s.total {
		return 0, io.EOF
	}
	if s.source == nil {
		s.reset()
	}

	remaining := s.total - s.read
	toRead := len(p)
	if int64(toRead) > remaining {
		toRead = int(remaining)
	}

	n, _ = s.source.Read(p[:toRead])
	if s.hasher != nil {
		s.hasher.Write(p[:n])
	}
	s.read += int64(n)
	return n, nil
}

func (s *randomDataStream) reset() {
	var seedInt64 int64
	for i := 0; i < 8 && i < len(s.seed); i++ {
		seedInt64 = (seedInt64 << 8) | int64(s.seed[i])
	}
	s.source = rand.New(rand.NewSource(seedInt64))
	s.read = 0
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

	// Dynamic registration or update.
	// Logic: If it's a new peer, it defaults to 'discovered' status and 'dht' source.
	// If it already exists, we preserve the existing source (so 'manual' stays 'manual').
	_, err := e.DB.ExecContext(ctx, `
		INSERT INTO peers (ip_address, public_key, status, contact_info, source, is_manual)
		VALUES (?, ?, 'discovered', ?, 'dht', 0)
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
	// Auto-register if new. If it's the admin, trust it immediately.
	defaultStatus := "pending"
	defaultQuota := int64(0)
	if e.AdminPublicKey != "" && pubKeyHex == e.AdminPublicKey {
		defaultStatus = "trusted"
		defaultQuota = 100 * 1024 * 1024 * 1024 // 100GB default for admin
	}

	_, err = e.DB.ExecContext(ctx, "INSERT OR IGNORE INTO clients (public_key, status, max_storage_size) VALUES (?, ?, ?)", pubKeyHex, defaultStatus, defaultQuota)
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

// ListPeerSpecialItems returns a list of all shard pieces tagged as special for a peer (peer-to-peer data path).


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
	Source              string
	IsManual            bool
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
	rows, err := e.DB.QueryContext(ctx, "SELECT id, ip_address, public_key, status, first_seen, COALESCE(last_seen, ''), max_storage_size, current_storage_size, outbound_storage_size, contact_info, total_shards, current_shards, is_manual, source FROM peers ORDER BY last_seen DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var peers []PeerDBInfo
	for rows.Next() {
		var p PeerDBInfo
		if err := rows.Scan(&p.ID, &p.Address, &p.PublicKey, &p.Status, &p.FirstSeen, &p.LastSeen, &p.MaxStorageSize, &p.CurrentStorageSize, &p.OutboundStorageSize, &p.ContactInfo, &p.TotalShards, &p.CurrentShards, &p.IsManual, &p.Source); err != nil {
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
		cbNode := rpc.PeerNode(capnp.NewClient(cbHandler.NewServer()))
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
