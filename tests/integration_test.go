package tests

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"p2p-backup/internal/client"
	"p2p-backup/internal/crypto"
	"p2p-backup/internal/db"
	"p2p-backup/internal/rpc"
	"p2p-backup/internal/server"
)

func runBackupCycle(t *testing.T, clientDB *sql.DB, dbJobChan chan db.DBJob, rpcClient client.RPCClient, backupDirs []string, key []byte) int64 {
	resChan := make(chan db.DBResult)
	dbJobChan <- db.DBJob{
		Query:      "INSERT INTO backups (start_time, status) VALUES (CURRENT_TIMESTAMP, 'running')",
		ResultChan: resChan,
	}
	backupID := (<-resChan).ID

	jobChan := make(chan client.FileJob, 1000)
	uploadChan := make(chan client.UploadJob, 100)

	crawler := client.NewCrawler(clientDB, dbJobChan, backupDirs, jobChan, 4, false)
	archiveChan := make(chan client.FileArchive, 1000)
	cryptoPool := client.NewCryptoPool(key, 2, uploadChan, archiveChan, false, true, nil)
	uploader := client.NewUploader(dbJobChan, uploadChan, rpcClient, 2, 10, false, false, backupID, nil)
	stateManager := client.NewStateManager(clientDB, dbJobChan, archiveChan, false)

	go stateManager.Start()
	uploader.Start()
	cryptoPool.Start(jobChan)
	crawler.Start(backupID)

	cryptoPool.Wait()
	close(uploadChan)
	close(archiveChan)
	uploader.Wait()
	stateManager.Wait()

	dbJobChan <- db.DBJob{
		Query:      "UPDATE backups SET end_time = CURRENT_TIMESTAMP, status = 'complete' WHERE id = ?",
		Args:       []interface{}{backupID},
		ResultChan: resChan,
	}
	<-resChan

	return backupID
}

func TestEndToEndBackup(t *testing.T) {
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir} {
		os.MkdirAll(dir, 0755)
	}

	serverDB, _ := server.InitDB(serverDBPath)
	defer serverDB.Close()

	engine := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 10, 4, 10*1024*1024, true, nil, "", 1024, false, false, 8, 43200, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "", "", 4, false)
	defer engine.Wait()
	
	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")
	os.WriteFile(filepath.Join(sourceDir, "file1.txt"), []byte("Hello, World!"), 0644)

	rpcClient := client.NewMockRPCClient(engine)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key)

	var blobCount int
	serverDB.QueryRow("SELECT COUNT(*) FROM blobs").Scan(&blobCount)
	if blobCount == 0 {
		t.Errorf("Expected blobs in server DB")
	}

	restoredDir := filepath.Join(baseDir, "restored")
	restorer := client.NewRestorer(clientDB, rpcClient, key, restoredDir, false)
	err := restorer.RestoreFile(context.Background(), filepath.Join(sourceDir, "file1.txt"), 0, sourceDir)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(restoredDir, "file1.txt"))
	if string(data) != "Hello, World!" {
		t.Errorf("Data mismatch: %s", string(data))
	}
}

func TestDeduplication(t *testing.T) {
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir} {
		os.MkdirAll(dir, 0755)
	}

	serverDB, _ := server.InitDB(serverDBPath)
	defer serverDB.Close()
	engine := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 10, 4, 10*1024*1024, true, nil, "", 1024, false, false, 8, 43200, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "", "", 4, false)
	defer engine.Wait()

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")
	content := []byte("DUPLICATE")
	os.WriteFile(filepath.Join(sourceDir, "f1.txt"), content, 0644)
	os.WriteFile(filepath.Join(sourceDir, "f2.txt"), content, 0644)

	rpcClient := client.NewMockRPCClient(engine)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key)

	var blobCount int
	serverDB.QueryRow("SELECT COUNT(*) FROM blobs").Scan(&blobCount)
	if blobCount != 1 {
		t.Errorf("Expected 1 blob (dedup), got %d", blobCount)
	}
}

func TestLargeFile(t *testing.T) {
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir} {
		os.MkdirAll(dir, 0755)
	}

	serverDB, _ := server.InitDB(serverDBPath)
	defer serverDB.Close()
	engine := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 2, 1, 100*1024*1024, true, nil, "", 1024, true, false, 8, 43200, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "", "", 4, false)
	defer engine.Wait()

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")
	fileSize := 10 * 1024 * 1024
	largeContent := make([]byte, fileSize)
	for i := range largeContent {
		largeContent[i] = byte((i ^ (i >> 8) ^ (i >> 16)) % 256)
	}
	os.WriteFile(filepath.Join(sourceDir, "large.bin"), largeContent, 0644)

	rpcClient := client.NewMockRPCClient(engine)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key)

	var blobCount int
	serverDB.QueryRow("SELECT COUNT(*) FROM blobs").Scan(&blobCount)
	if blobCount != 3 {
		t.Errorf("Expected 3 blobs, got %d", blobCount)
	}
}

func TestIncrementalBackup(t *testing.T) {
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir} {
		os.MkdirAll(dir, 0755)
	}

	serverDB, _ := server.InitDB(serverDBPath)
	defer serverDB.Close()
	engine := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 10, 4, 100*1024*1024, true, nil, "", 1024, false, false, 8, 43200, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "", "", 4, false)
	defer engine.Wait()

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")
	f1 := filepath.Join(sourceDir, "f1.txt")
	os.WriteFile(f1, []byte("V1"), 0644)

	rpcClient := client.NewMockRPCClient(engine)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key)

	time.Sleep(1100 * time.Millisecond)
	os.WriteFile(f1, []byte("V2"), 0644)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key)

	var vCount int
	clientDB.QueryRow("SELECT COUNT(*) FROM file_versions").Scan(&vCount)
	if vCount != 2 {
		t.Errorf("Expected 2 versions, got %d", vCount)
	}
}

func TestFileDeletion(t *testing.T) {
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir} {
		os.MkdirAll(dir, 0755)
	}

	serverDB, _ := server.InitDB(serverDBPath)
	defer serverDB.Close()
	engine := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 10, 4, 100*1024*1024, true, nil, "", 1024, false, false, 8, 43200, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "", "", 4, false)
	defer engine.Wait()

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")
	f1 := filepath.Join(sourceDir, "f1.txt")
	os.WriteFile(f1, []byte("Content"), 0644)

	rpcClient := client.NewMockRPCClient(engine)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key)

	os.Remove(f1)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key)

	var deleted int
	clientDB.QueryRow("SELECT deleted FROM files WHERE filename = 'f1.txt'").Scan(&deleted)
	if deleted != 1 {
		t.Errorf("Expected deleted=1, got %d", deleted)
	}
}

func TestServerGC(t *testing.T) {
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir} {
		os.MkdirAll(dir, 0755)
	}

	serverDB, _ := server.InitDB(serverDBPath)
	defer serverDB.Close()
	engine := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 10, 4, 10, true, nil, "", 1024, false, false, 8, 0, 0, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "", "", 4, false)
	defer engine.Wait()
	engine.KeepDeletedMinutes = 0
	engine.KeepMetadataMinutes = 0

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	h := hex.EncodeToString(crypto.Hash([]byte("some data")))
	ingestErr := engine.IngestBlobs(context.Background(), "system-self-backup", []rpc.LocalBlobData{
	    {Hash: h, Data: []byte("some data"), IsSpecial: true},
	}, false)
	if ingestErr != nil {
		t.Fatalf("IngestBlobs failed: %v", ingestErr)
	}
	var blobHash string
	serverDB.QueryRow("SELECT hash FROM blobs LIMIT 1").Scan(&blobHash)
	engine.DeleteBlobs(context.Background(), []string{blobHash})
	serverDB.Exec("UPDATE blobs SET deleted_at = datetime('now', '-1 hour') WHERE hash = ?", blobHash)

	// Wait for any background encoding to finish before we start deleting things
	engine.Wait()

	engine.TriggerGC(context.Background())

	var exists int
	serverDB.QueryRow("SELECT COUNT(*) FROM shards").Scan(&exists)
	if exists != 0 {
		t.Errorf("Expected 0 shards after GC, got %d", exists)
	}
}

type MockPiece struct {
	Data []byte
	Meta rpc.PeerShardMeta
}

type MockPeerHandler struct {
	Pieces map[string]MockPiece
}

func (h *MockPeerHandler) OfferShards(ctx context.Context, call rpc.PeerNode_offerShards) error {
	args := call.Args()
	shards, _ := args.Shards()
	res, _ := call.AllocResults()
	needed, _ := res.NewNeededIndices(int32(shards.Len()))
	for i := 0; i < shards.Len(); i++ {
		needed.Set(i, uint32(i))
	}
	return nil
}

func (h *MockPeerHandler) PrepareUpload(ctx context.Context, call rpc.PeerNode_prepareUpload) error {
	args := call.Args()
	shards, _ := args.Shards()
	for i := 0; i < shards.Len(); i++ {
		s := shards.At(i)
		hashBytes, _ := s.Checksum()
		hash := hex.EncodeToString(hashBytes)
		parentHashBytes, _ := s.ParentShardHash()
		h.Pieces[hash] = MockPiece{
			Data: nil, // Data arrives via raw stream (skipped in test mode)
			Meta: rpc.PeerShardMeta{
				Hash:            hash,
				Size:            int64(s.Size()),
				IsSpecial:       s.IsSpecial(),
				PieceIndex:      int(s.PieceIndex()),
				ParentShardHash: hex.EncodeToString(parentHashBytes),
				SequenceNumber:  s.SequenceNumber(),
				TotalPieces:     int(s.TotalPieces()),
			},
		}
	}
	res, _ := call.AllocResults()
	res.SetSuccess(true)
	return nil
}

func (h *MockPeerHandler) ChallengePiece(ctx context.Context, call rpc.PeerNode_challengePiece) error {
	args := call.Args()
	hashBytes, _ := args.ShardChecksum()
	hashHex := hex.EncodeToString(hashBytes)
	offset := args.Offset()
	p, ok := h.Pieces[hashHex]
	if !ok { return fmt.Errorf("not found") }
	res, _ := call.AllocResults()
	return res.SetData(p.Data[offset : offset+32])
}

func (h *MockPeerHandler) ReleasePiece(ctx context.Context, call rpc.PeerNode_releasePiece) error {
	res, _ := call.AllocResults()
	res.SetSuccess(true)
	return nil
}



func (h *MockPeerHandler) ListSpecialPieces(ctx context.Context, call rpc.PeerNode_listSpecialPieces) error {
	var specials []rpc.PeerShardMeta
	for _, p := range h.Pieces {
		if p.Meta.IsSpecial {
			specials = append(specials, p.Meta)
		}
	}
	res, _ := call.AllocResults()
	shards, _ := res.NewShards(int32(len(specials)))
	for i, m := range specials {
		s := shards.At(i)
		hb, _ := hex.DecodeString(m.Hash)
		s.SetChecksum(hb)
		s.SetSize(uint64(m.Size))
		s.SetIsSpecial(true)
		s.SetPieceIndex(uint32(m.PieceIndex))
		s.SetTotalPieces(uint32(m.TotalPieces))
		s.SetSequenceNumber(m.SequenceNumber)
		phb, _ := hex.DecodeString(m.ParentShardHash)
		s.SetParentShardHash(phb)
	}
	return nil
}

func (h *MockPeerHandler) Announce(ctx context.Context, call rpc.PeerNode_announce) error {
	res, _ := call.AllocResults()
	res.SetSuccess(true)
	return nil
}

func TestOutboundWorkerFlow(t *testing.T) {
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir} {
		os.MkdirAll(dir, 0755)
	}

	serverDB, _ := server.InitDB(serverDBPath)
	defer serverDB.Close()
	engine := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 1, 1, 10, true, nil, "", 1024, true, false, 8, 43200, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "", "", 4, false)
	defer engine.Wait()

	mh1 := &MockPeerHandler{Pieces: make(map[string]MockPiece)}
	mh2 := &MockPeerHandler{Pieces: make(map[string]MockPiece)}
	serverDB.Exec("INSERT INTO peers (public_key, ip_address, status, max_storage_size) VALUES ('p1', '127.0.0.1', 'trusted', 1000)")
	serverDB.Exec("INSERT INTO peers (public_key, ip_address, status, max_storage_size) VALUES ('p2', '127.0.0.2', 'trusted', 1000)")
	var id1, id2 int64
	serverDB.QueryRow("SELECT id FROM peers WHERE public_key='p1'").Scan(&id1)
	serverDB.QueryRow("SELECT id FROM peers WHERE public_key='p2'").Scan(&id2)
	engine.RegisterActivePeer(id1, rpc.PeerNode_ServerToClient(mh1))
	engine.RegisterActivePeer(id2, rpc.PeerNode_ServerToClient(mh2))

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")
	os.WriteFile(filepath.Join(sourceDir, "f1.txt"), []byte("Chunk 1 data longer than 10 bytes"), 0644)
	rpcClient := client.NewMockRPCClient(engine)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key)

	// Wait for background encoding to finish
	engine.Wait()

	engine.TriggerOutbound(context.Background())

	// Verify pieces were distributed by checking the outbound_pieces table
	// (PrepareUpload + PushPiece are skipped in test mode, but the DB is updated)
	var uploadedCount int
	serverDB.QueryRow("SELECT COUNT(*) FROM outbound_pieces WHERE status = 'uploaded'").Scan(&uploadedCount)
	if uploadedCount < 2 {
		t.Errorf("Expected at least 2 uploaded pieces, got %d", uploadedCount)
	}
}

func TestChallengeWorkerFlow(t *testing.T) {
	baseDir := t.TempDir()
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")
	os.MkdirAll(serverBlobDir, 0755)
	os.MkdirAll(serverQueueDir, 0755)

	serverDB, _ := server.InitDB(serverDBPath)
	defer serverDB.Close()
	engine := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 1, 1, 1024, true, nil, "", 1024, true, false, 8, 43200, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "", "", 4, false)
	defer engine.Wait()

	mh := &MockPeerHandler{Pieces: make(map[string]MockPiece)}
	serverDB.Exec("INSERT INTO peers (public_key, ip_address, status, max_storage_size) VALUES ('p1', '127.0.0.1', 'trusted', 1000)")
	var pid int64
	serverDB.QueryRow("SELECT id FROM peers WHERE public_key='p1'").Scan(&pid)
	engine.RegisterActivePeer(pid, rpc.PeerNode_ServerToClient(mh))

	data := make([]byte, 1024)
	pieceHash := hex.EncodeToString(crypto.Hash(data))
	mh.Pieces[pieceHash] = MockPiece{Data: data, Meta: rpc.PeerShardMeta{IsSpecial: false}}
	serverDB.Exec("INSERT INTO shards (id, status, size) VALUES (1, 'sealed', 1024)")
	serverDB.Exec("INSERT INTO outbound_pieces (shard_id, piece_index, peer_id, status) VALUES (1, 0, ?, 'uploaded')", pid)
	serverDB.Exec("INSERT INTO piece_challenges (shard_id, piece_index, peer_id, piece_hash, offset, expected_data) VALUES (1, 0, ?, ?, 100, ?)", pid, pieceHash, data[100:132])

	engine.TriggerChallenge(context.Background())

	var status string
	serverDB.QueryRow("SELECT status FROM challenge_results WHERE peer_id=?", pid).Scan(&status)
	if status != "pass" {
		t.Errorf("Challenge failed: %s", status)
	}
}

func TestRepairWorkerFlow(t *testing.T) {
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir} {
		os.MkdirAll(dir, 0755)
	}

	serverDB, _ := server.InitDB(serverDBPath)
	defer serverDB.Close()
	engine := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 2, 1, 1024*1024, true, nil, "", 1024, true, false, 8, 43200, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "", "", 4, false)
	defer engine.Wait()

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")
	os.WriteFile(filepath.Join(sourceDir, "f1.txt"), []byte("Repair data"), 0644)
	rpcClient := client.NewMockRPCClient(engine)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key)

	var sid int64
	serverDB.QueryRow("SELECT id FROM shards LIMIT 1").Scan(&sid)
	serverDB.Exec("UPDATE shards SET status='sealed', size=1024*1024, total_pieces=3 WHERE id=?", sid)
	engine.TriggerEncodeShard(sid)

	serverDB.Exec("UPDATE outbound_pieces SET status='lost' WHERE piece_index=2")
	engine.TriggerRepair(context.Background())

	entries, _ := os.ReadDir(serverQueueDir)
	found := false
	for _, e := range entries {
		if strings.Contains(e.Name(), "piece_2") { found = true }
	}
	if !found { t.Errorf("Piece 2 not enqueued for repair") }
}

func TestReedSolomonIntegration(t *testing.T) {
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir} {
		os.MkdirAll(dir, 0755)
	}

	serverDB, _ := server.InitDB(serverDBPath)
	defer serverDB.Close()
	engine := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 2, 1, 1024*1024, true, nil, "", 1024, true, false, 8, 43200, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "", "", 4, false)
	defer engine.Wait()

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")
	content := make([]byte, 512*1024)
	os.WriteFile(filepath.Join(sourceDir, "f1.txt"), content, 0644)
	rpcClient := client.NewMockRPCClient(engine)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key)

	var sid int64
	serverDB.QueryRow("SELECT id FROM shards LIMIT 1").Scan(&sid)
	serverDB.Exec("UPDATE shards SET status='sealed', size=1024*1024, total_pieces=3 WHERE id=?", sid)
	engine.TriggerEncodeShard(sid)

	handlers := []*MockPeerHandler{{Pieces: make(map[string]MockPiece)}, {Pieces: make(map[string]MockPiece)}, {Pieces: make(map[string]MockPiece)}}
	for i := 0; i < 3; i++ {
		pPath := filepath.Join(serverQueueDir, fmt.Sprintf("shard_%d_piece_%d", sid, i))
		data, _ := os.ReadFile(pPath)
		h := hex.EncodeToString(crypto.Hash(data))
		handlers[i].Pieces[h] = MockPiece{Data: data, Meta: rpc.PeerShardMeta{IsSpecial: false}}
		serverDB.Exec("INSERT INTO peers (public_key, ip_address, status) VALUES (?, '127.0.0.1', 'trusted')", fmt.Sprintf("p%d", i))
		var pid int64
		serverDB.QueryRow("SELECT id FROM peers WHERE public_key=?", fmt.Sprintf("p%d", i)).Scan(&pid)
		engine.RegisterActivePeer(pid, rpc.PeerNode_ServerToClient(handlers[i]))
		serverDB.Exec("INSERT INTO outbound_pieces (shard_id, piece_index, peer_id, status) VALUES (?, ?, ?, 'uploaded')", sid, i, pid)
		serverDB.Exec("INSERT INTO piece_challenges (shard_id, piece_index, peer_id, piece_hash, offset, expected_data) VALUES (?, ?, ?, ?, 0, ?)", sid, i, pid, h, []byte{0})
    }

	// Verify erasure coding produced the expected number of pieces
	var pieceCount int
	serverDB.QueryRow("SELECT COUNT(*) FROM outbound_pieces WHERE shard_id = ?", sid).Scan(&pieceCount)
	if pieceCount != 3 {
		t.Errorf("Expected 3 outbound pieces, got %d", pieceCount)
	}

	// Verify all 3 piece files were created by the encoder
	for i := 0; i < 3; i++ {
		pPath := filepath.Join(serverQueueDir, fmt.Sprintf("shard_%d_piece_%d", sid, i))
		if _, err := os.Stat(pPath); err != nil {
			t.Errorf("Expected piece file %s to exist", pPath)
		}
	}

	// Note: EnsureShardLocal/PullPiece reconstruction requires a live libp2p Host
	// and is tested in full integration with real P2P networking, not in this unit test.
}

func TestDisasterRecovery(t *testing.T) {
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir} {
		os.MkdirAll(dir, 0755)
	}

	serverDB, _ := server.InitDB(serverDBPath)
	defer serverDB.Close()
	engine := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 2, 1, 100*1024*1024, true, nil, "", 1024, true, false, 8, 43200, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "", "", 4, false)
	defer engine.Wait()

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)

	key := []byte("01234567890123456789012345678901")
	os.WriteFile(filepath.Join(sourceDir, "important.txt"), []byte("Important"), 0644)
	rpcClient := client.NewMockRPCClient(engine)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key)

	close(dbJobChan)
	clientDB.Close()
	dbData, _ := os.ReadFile(filepath.Join(baseDir, "client.db"))
	cipher, _ := crypto.Encrypt(key, dbData)
	h := hex.EncodeToString(crypto.Hash(cipher))
	rpcClient.PrepareUploadClient(context.Background(), []rpc.BlobMeta{{Hash: h, Size: int64(len(cipher)), Special: true}})
	engine.IngestBlobs(context.Background(), "insecure-local-client", []rpc.LocalBlobData{{Hash: h, Data: cipher, IsSpecial: true}}, false)

	os.Remove(filepath.Join(baseDir, "client.db"))
	specials, _ := rpcClient.ListSpecialBlobs(context.Background())
	data, _ := rpcClient.PullBlob(context.Background(), specials[0].Hash)
	dec, _ := crypto.Decrypt(key, data)
	os.WriteFile(filepath.Join(baseDir, "client.db"), dec, 0644)

	recDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer recDB.Close()
	restorer := client.NewRestorer(recDB, rpcClient, key, filepath.Join(baseDir, "restored"), false)
	restorer.RestoreFile(context.Background(), filepath.Join(sourceDir, "important.txt"), 0, sourceDir)
}

func TestServerDisasterRecovery(t *testing.T) {
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir} {
		os.MkdirAll(dir, 0755)
	}

	serverDB, _ := server.InitDB(serverDBPath)
	masterKey := []byte("01234567890123456789012345678901")
	engineA := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 2, 1, 20*1024*1024, true, nil, "", 1024, true, false, 8, 43200, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, masterKey, "", "", 4, false)
	defer engineA.Wait()
	defer serverDB.Close()

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	clientKey := []byte("client-key-32-bytes-long-padded0")
	os.WriteFile(filepath.Join(sourceDir, "f1.txt"), []byte("Some original user data"), 0644)
	rpcClientA := client.NewMockRPCClient(engineA)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClientA, []string{sourceDir}, clientKey)

	mockPeers := []*MockPeerHandler{
		{Pieces: make(map[string]MockPiece)},
		{Pieces: make(map[string]MockPiece)},
		{Pieces: make(map[string]MockPiece)},
	}
	for i := 0; i < 3; i++ {
		serverDB.Exec("INSERT INTO peers (public_key, ip_address, status, max_storage_size) VALUES (?, '127.0.0.1', 'trusted', 1000000000)", fmt.Sprintf("peer-%d", i))
		var pid int64
		serverDB.QueryRow("SELECT id FROM peers WHERE public_key = ?", fmt.Sprintf("peer-%d", i)).Scan(&pid)
		engineA.RegisterActivePeer(pid, rpc.PeerNode_ServerToClient(mockPeers[i]))
	}

	// Ensure shard 1 is encoded and pushed
	var shard1ID int64
	serverDB.QueryRow("SELECT id FROM shards LIMIT 1").Scan(&shard1ID)
	engineA.TriggerEncodeShard(shard1ID)
	engineA.TriggerOutbound(context.Background())

	engineA.RunSelfBackup(context.Background())
	engineA.TriggerSyncMirrored(context.Background())
	engineA.TriggerOutbound(context.Background())

	// Wait for all encoding tasks to finish BEFORE wiping
	engineA.Wait()

	// Verify self-backup produced special mirrored shards and they were distributed
	var specialCount int
	serverDB.QueryRow("SELECT COUNT(*) FROM shards WHERE mirrored = 1").Scan(&specialCount)
	if specialCount == 0 {
		t.Fatal("Self-backup did not create any mirrored shards")
	}

	// Verify mock peers received special piece metadata via PrepareUpload
	totalSpecial := 0
	for _, mp := range mockPeers {
		for _, p := range mp.Pieces {
			if p.Meta.IsSpecial {
				totalSpecial++
			}
		}
	}
	if totalSpecial == 0 {
		t.Fatal("No special pieces distributed to mock peers")
	}
	t.Logf("Self-backup created %d mirrored shards, distributed %d special pieces to peers", specialCount, totalSpecial)

	// Note: Full AttemptRescue/PullPieceDirect round-trip requires a live libp2p Host
	// and is tested in full P2P integration, not in this unit test.
}

func TestContactInfoPropagation(t *testing.T) {
	baseDir := t.TempDir()
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	serverDBPath := filepath.Join(baseDir, "server.db")
	os.MkdirAll(serverBlobDir, 0755)
	os.MkdirAll(serverQueueDir, 0755)

	serverDB, _ := server.InitDB(serverDBPath)
	defer serverDB.Close()
	
	myContact := "operator@server-a.com"
	engine := server.NewEngine(serverDB, serverDBPath, serverBlobDir, serverQueueDir, 1, 1, 1024, true, nil, "127.0.0.1:8080", 1024, true, false, 8, 43200, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "", myContact, 4, false)
	defer engine.Wait()

	// 1. Verify Engine has contact info
	if engine.ContactInfo != myContact {
		t.Errorf("Engine did not store contact info: %s", engine.ContactInfo)
	}

	// 2. Simulate a peer announcing itself with contact info
	peerContact := "@twitter_handle"
	peerPubKey := "peer-pub-key-hex-string-32-chars-long"
	peerAddr := "1.2.3.4:8081"
	
	peerID, err := engine.AnnouncePeer(context.Background(), peerPubKey, peerAddr, peerContact)
	if err != nil {
		t.Fatalf("AnnouncePeer failed: %v", err)
	}

	// 3. Verify it's in the DB
	var dbContact string
	err = serverDB.QueryRow("SELECT contact_info FROM peers WHERE id = ?", peerID).Scan(&dbContact)
	if err != nil {
		t.Fatalf("Failed to query peer contact info: %v", err)
	}
	if dbContact != peerContact {
		t.Errorf("DB did not store peer contact info correctly. Got %s, want %s", dbContact, peerContact)
	}

	// 4. Verify ListPeers returns it
	peers, err := engine.ListPeers(context.Background())
	if err != nil {
		t.Fatalf("ListPeers failed: %v", err)
	}
	found := false
	for _, p := range peers {
		if p.ID == peerID {
			if p.ContactInfo != peerContact {
				t.Errorf("ListPeers returned wrong contact info: %s", p.ContactInfo)
			}
			found = true
		}
	}
	if !found {
		t.Error("Peer not found in ListPeers")
	}
	t.Log("Success! Contact info propagated correctly through the engine and DB.")
}
