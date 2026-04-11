package server

import (
	"context"
	crypto_rand "crypto/rand"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"

	capnp "capnproto.org/go/capnp/v3"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"lukechampine.com/blake3"
	"p2p-backup/internal/config"
	"p2p-backup/internal/crypto"
	"p2p-backup/internal/rpc"
)

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

	// 4. Trigger Automatic Adoption if enabled
	e.MaybeStartAdoption(ctx, internalID)


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

	peerID, err := e.AnnouncePeer(ctx, pubKeyHex, addrStr, "", "dht")
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
	e.MaybeStartAdoption(ctx, peerID)


	return nil
}

// MaybeStartAdoption checks if adoption is enabled and the peer hasn't been tested yet,
// then starts the adoption test in a background goroutine. Safe to call from multiple places
// concurrently — uses an atomic DB update to prevent duplicate tests.
func (e *Engine) MaybeStartAdoption(ctx context.Context, peerID int64) {
	if !e.AdoptionEnabled || e.AdoptionChallengePieces <= 0 {
		return
	}

	// Atomic claim: only one goroutine can transition from 'none' to 'testing'
	res, err := e.DB.ExecContext(ctx, "UPDATE peers SET adoption_status = 'testing' WHERE id = ? AND adoption_status = 'none'", peerID)
	if err != nil {
		return
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return // Already claimed by another goroutine
	}

	go func() {
		// Push any pending mirrored shards (e.g. self-backup/recovery) to the new peer
		// BEFORE starting the adoption test, so challenge data is available immediately.
		e.syncMirroredShards(context.Background())

		if err := e.StartAdoptionTest(context.Background(), peerID); err != nil {
			log.Printf("Adoption: Failed to start test for peer %d: %v", peerID, err)
			// Revert status on failure so it can be retried
			e.DB.ExecContext(context.Background(), "UPDATE peers SET adoption_status = 'none' WHERE id = ? AND adoption_status = 'testing'", peerID)
		}
	}()
}

func (e *Engine) StartAdoptionTest(ctx context.Context, peerID int64) error {
	if e.Verbose {
		log.Printf("Adoption: Starting test for peer %d (%d pieces)...", peerID, e.AdoptionChallengePieces)
	}

	// Status was already set to 'testing' by MaybeStartAdoption, no need to set it again.

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
				Type:       1, // 1 = PeerShard
				IsSpecial:  false, // Adoption piece doesn't need to be strictly special on the receiver side
				PieceIndex: 0,
			},
		}

		if err := client.PrepareUpload(ctx, meta); err != nil {
			e.DB.ExecContext(ctx, "UPDATE peers SET adoption_status = 'failed' WHERE id = ?", peerID)
			return fmt.Errorf("failed to prepare upload for adoption piece: %w", err)
		}

		// Update outbound_storage_size for the peer BEFORE the upload starts so it reflects in the UI immediately
		_, err = e.DB.ExecContext(ctx, "UPDATE peers SET outbound_storage_size = outbound_storage_size + ? WHERE id = ?", pieceSize, peerID)
		if err != nil {
			log.Printf("Adoption: Failed to update outbound_storage_size for peer %d: %v", peerID, err)
		}

		err = e.PushPieceBatched(ctx, peerID, streamItems)
		if err != nil {
			e.DB.ExecContext(ctx, "UPDATE peers SET adoption_status = 'failed' WHERE id = ?", peerID)
			e.DB.ExecContext(ctx, "UPDATE peers SET outbound_storage_size = MAX(0, outbound_storage_size - ?) WHERE id = ?", pieceSize, peerID)
			return fmt.Errorf("failed to upload adoption piece: %w", err)
		}

		// Record the piece in adoption_tests
		_, err = e.DB.ExecContext(ctx, "INSERT OR REPLACE INTO adoption_tests (hash, size, peer_id) VALUES (?, ?, ?)", hashHex, pieceSize, peerID)
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
func (e *Engine) AnnouncePeer(ctx context.Context, pubKeyHex, listenAddress, contactInfo, source string) (int64, error) {
	if pubKeyHex == "" || listenAddress == "" || pubKeyHex == "insecure-local-client" {
		return 0, nil
	}

	status := "discovered"
	isManual := 0
	if source == "manual" {
		status = "untrusted"
		isManual = 1
	}

	// Dynamic registration or update.
	// Logic: If it's a new peer, it defaults to the appropriate status and the provided source.
	// If it already exists, we preserve the existing source (so 'manual' stays 'manual').
	_, err := e.DB.ExecContext(ctx, `
		INSERT INTO peers (ip_address, public_key, status, contact_info, source, is_manual)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(public_key) DO UPDATE SET 
			ip_address=excluded.ip_address,
			contact_info=excluded.contact_info,
			last_seen=CURRENT_TIMESTAMP
	`, listenAddress, pubKeyHex, status, contactInfo, source, isManual)

	if err != nil {
		return 0, fmt.Errorf("failed to announce peer: %w", err)
	}

	return e.GetPeerIDByPubKey(ctx, pubKeyHex)
}

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
	InboundBytes        int64
	OutboundBytes       int64
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
	rows, err := e.DB.QueryContext(ctx, "SELECT id, ip_address, public_key, status, first_seen, COALESCE(last_seen, ''), max_storage_size, current_storage_size, outbound_storage_size, inbound_bytes, outbound_bytes, contact_info, total_shards, current_shards, is_manual, source FROM peers ORDER BY last_seen DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var peers []PeerDBInfo
	for rows.Next() {
		var p PeerDBInfo
		if err := rows.Scan(&p.ID, &p.Address, &p.PublicKey, &p.Status, &p.FirstSeen, &p.LastSeen, &p.MaxStorageSize, &p.CurrentStorageSize, &p.OutboundStorageSize, &p.InboundBytes, &p.OutboundBytes, &p.ContactInfo, &p.TotalShards, &p.CurrentShards, &p.IsManual, &p.Source); err != nil {
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
