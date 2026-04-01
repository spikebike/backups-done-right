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

func TestEndToEndBackup(t *testing.T) {
	// 1. Setup Isolated Temporary Directories
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	clientSpoolDir := filepath.Join(baseDir, "spool")
	clientUploadDir := filepath.Join(baseDir, "upload")

	// Ensure directories exist
	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir, clientSpoolDir, clientUploadDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create test dir: %v", err)
		}
	}

	// 2. Create Dummy Files in Source
	dummyFile1 := filepath.Join(sourceDir, "file1.txt")
	if err := os.WriteFile(dummyFile1, []byte("Hello, World!"), 0644); err != nil {
		t.Fatalf("Failed to write dummy file: %v", err)
	}

	// 3. Setup Server
	serverDBPath := filepath.Join(baseDir, "server.db")
	serverDB, err := server.InitDB(serverDBPath)
	if err != nil {
		t.Fatalf("Failed to init server DB: %v", err)
	}
	defer serverDB.Close()

	// Use smaller sizes for fast tests
	pieceSize := int64(1 * 1024 * 1024) // 1MB for fast shards
	shardSize := int64(10) * pieceSize
	keepLocal := true

	engine := server.NewEngine(
		serverDB, serverBlobDir, serverQueueDir,
		10, 4, shardSize, keepLocal, nil, "", 1024,
		false, false, 8, 43200, 0.5, 720, 1440, 24, 4,
		-1, -1, -1, nil, "",
	)

	// 4. Setup Client
	clientDBPath := filepath.Join(baseDir, "client.db")
	clientDB, err := db.InitClientDB(clientDBPath)
	if err != nil {
		t.Fatalf("Failed to init client DB: %v", err)
	}
	defer clientDB.Close()

	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)

	// dummy backup record
	resChan := make(chan db.DBResult)
	dbJobChan <- db.DBJob{
		Query:      "INSERT INTO backups (start_time, status) VALUES (CURRENT_TIMESTAMP, 'running')",
		ResultChan: resChan,
	}
	backupID := (<-resChan).ID

	// Pipelines
	jobChan := make(chan client.FileJob, 100)
	uploadChan := make(chan client.UploadJob, 100)

	crawler := client.NewCrawler(clientDB, dbJobChan, []string{sourceDir}, jobChan, false)
	key := []byte("01234567890123456789012345678901")
	cryptoPool := client.NewCryptoPool(clientDB, dbJobChan, key, clientSpoolDir, clientUploadDir, 4, uploadChan, false)
	
	rpcClient := client.NewMockRPCClient(engine)
	uploader := client.NewUploader(clientDB, uploadChan, rpcClient, clientUploadDir, 10, false, false, backupID)

	// 5. Run and Wait
	uploader.Start()
	cryptoPool.Start(jobChan)
	crawler.Start(backupID)

	cryptoPool.Wait()
	close(uploadChan)
	uploader.Wait()

	// Wait for DB writer to finish its queues
	close(dbJobChan)

	// 6. Assertions!
	// Now query the server database or filesystem to prove the file was backed up and sharded
	var blobCount int
	err = serverDB.QueryRow("SELECT COUNT(*) FROM blobs").Scan(&blobCount)
	if err != nil {
		t.Fatalf("Failed to query server DB: %v", err)
	}

	if blobCount == 0 {
		t.Errorf("Expected at least 1 blob in server database, got 0")
	} else {
		t.Logf("Success! Found %d blobs in the server DB.", blobCount)
	}

	// 7. Restore Phase
	restoredDir := filepath.Join(baseDir, "restored")
	if err := os.MkdirAll(restoredDir, 0755); err != nil {
		t.Fatalf("Failed to create restored dir: %v", err)
	}

	restorer := client.NewRestorer(clientDB, rpcClient, key, restoredDir, false)

	// Restore the file using context.Background()
	// Pass 0 for targetBackupID to get the latest, and sourceDir as baseTargetPath
	err = restorer.RestoreFile(context.Background(), dummyFile1, 0, sourceDir)
	if err != nil {
		t.Fatalf("Failed to restore file: %v", err)
	}

	// 8. Verification Phase
	// The restorer will strip baseTargetPath (sourceDir) from dummyFile1, 
	// leaving just the relative file name, then place it in restoredDir.
	restoredFilePath := filepath.Join(restoredDir, "file1.txt")
	restoredData, err := os.ReadFile(restoredFilePath)
	if err != nil {
		t.Fatalf("Failed to read restored file: %v", err)
	}

	expectedData := "Hello, World!"
	if string(restoredData) != expectedData {
		t.Errorf("Restored file content mismatch. Expected '%s', got '%s'", expectedData, string(restoredData))
	} else {
		t.Log("Success! Restored file content matches the original exactly.")
	}
}

func TestDeduplication(t *testing.T) {
	// 1. Setup Isolated Temporary Directories
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	clientSpoolDir := filepath.Join(baseDir, "spool")
	clientUploadDir := filepath.Join(baseDir, "upload")

	// Ensure directories exist
	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir, clientSpoolDir, clientUploadDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create test dir: %v", err)
		}
	}

	// 2. Create TWO Files with IDENTICAL Content in Source
	content := []byte("DUPLICATE CONTENT")
	dummyFile1 := filepath.Join(sourceDir, "file1.txt")
	if err := os.WriteFile(dummyFile1, content, 0644); err != nil {
		t.Fatalf("Failed to write dummy file 1: %v", err)
	}

	dummyFile2 := filepath.Join(sourceDir, "file2.txt")
	if err := os.WriteFile(dummyFile2, content, 0644); err != nil {
		t.Fatalf("Failed to write dummy file 2: %v", err)
	}

	// 3. Setup Server
	serverDBPath := filepath.Join(baseDir, "server.db")
	serverDB, err := server.InitDB(serverDBPath)
	if err != nil {
		t.Fatalf("Failed to init server DB: %v", err)
	}
	defer serverDB.Close()

	// Use smaller sizes for fast tests
	pieceSize := int64(1 * 1024 * 1024) // 1MB for fast shards
	shardSize := int64(10) * pieceSize
	keepLocal := true

	engine := server.NewEngine(
		serverDB, serverBlobDir, serverQueueDir,
		10, 4, shardSize, keepLocal, nil, "", 1024,
		false, false, 8, 43200, 0.5, 720, 1440, 24, 4,
		-1, -1, -1, nil, "",
	)

	// 4. Setup Client
	clientDBPath := filepath.Join(baseDir, "client.db")
	clientDB, err := db.InitClientDB(clientDBPath)
	if err != nil {
		t.Fatalf("Failed to init client DB: %v", err)
	}
	defer clientDB.Close()

	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)

	// dummy backup record
	resChan := make(chan db.DBResult)
	dbJobChan <- db.DBJob{
		Query:      "INSERT INTO backups (start_time, status) VALUES (CURRENT_TIMESTAMP, 'running')",
		ResultChan: resChan,
	}
	backupID := (<-resChan).ID

	// Pipelines
	jobChan := make(chan client.FileJob, 100)
	uploadChan := make(chan client.UploadJob, 100)

	crawler := client.NewCrawler(clientDB, dbJobChan, []string{sourceDir}, jobChan, false)
	key := []byte("01234567890123456789012345678901")
	cryptoPool := client.NewCryptoPool(clientDB, dbJobChan, key, clientSpoolDir, clientUploadDir, 4, uploadChan, false)
	
	rpcClient := client.NewMockRPCClient(engine)
	uploader := client.NewUploader(clientDB, uploadChan, rpcClient, clientUploadDir, 10, false, false, backupID)

	// 5. Run and Wait
	uploader.Start()
	cryptoPool.Start(jobChan)
	crawler.Start(backupID)

	cryptoPool.Wait()
	close(uploadChan)
	uploader.Wait()
	close(dbJobChan)

	// 6. Assertions for Deduplication!
	// There should be ONLY ONE blob in the server database, even though two files were backed up.
	var blobCount int
	err = serverDB.QueryRow("SELECT COUNT(*) FROM blobs").Scan(&blobCount)
	if err != nil {
		t.Fatalf("Failed to query server DB: %v", err)
	}

	if blobCount != 1 {
		t.Errorf("Deduplication failure! Expected 1 blob in server database, found %d", blobCount)
	} else {
		t.Log("Success! Deduplication worked: two identical files resulted in only one blob.")
	}

	// 7. Verify Restoration of both files
	restoredDir := filepath.Join(baseDir, "restored")
	restorer := client.NewRestorer(clientDB, rpcClient, key, restoredDir, false)

	for _, originalFile := range []string{dummyFile1, dummyFile2} {
		err = restorer.RestoreFile(context.Background(), originalFile, 0, sourceDir)
		if err != nil {
			t.Fatalf("Failed to restore file %s: %v", originalFile, err)
		}

		relPath, _ := filepath.Rel(sourceDir, originalFile)
		restoredFilePath := filepath.Join(restoredDir, relPath)
		restoredData, err := os.ReadFile(restoredFilePath)
		if err != nil {
			t.Fatalf("Failed to read restored file %s: %v", restoredFilePath, err)
		}

		if string(restoredData) != string(content) {
			t.Errorf("Restored file content mismatch for %s. Expected '%s', got '%s'", originalFile, string(content), string(restoredData))
		}
	}
	t.Log("Success! Both files were correctly restored from the single deduplicated blob.")
}

func TestLargeFile(t *testing.T) {
	// 1. Setup Isolated Temporary Directories
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	clientSpoolDir := filepath.Join(baseDir, "spool")
	clientUploadDir := filepath.Join(baseDir, "upload")

	// Ensure directories exist
	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir, clientSpoolDir, clientUploadDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create test dir: %v", err)
		}
	}

	// 2. Create a LARGE File in Source (10MB)
	// Since chunk size is 4MB (4194304 bytes), 10MB will create 3 chunks (4MB, 4MB, 2MB)
	chunkSize := 4 * 1024 * 1024
	fileSize := 10 * 1024 * 1024
	largeContent := make([]byte, fileSize)
	// Fill with a non-repeating pattern so chunks don't deduplicate
	for i := range largeContent {
		largeContent[i] = byte((i ^ (i >> 8) ^ (i >> 16)) % 256)
	}

	largeFileName := "large_file.bin"
	largeFilePath := filepath.Join(sourceDir, largeFileName)
	if err := os.WriteFile(largeFilePath, largeContent, 0644); err != nil {
		t.Fatalf("Failed to write large dummy file: %v", err)
	}

	// 3. Setup Server
	serverDBPath := filepath.Join(baseDir, "server.db")
	serverDB, err := server.InitDB(serverDBPath)
	if err != nil {
		t.Fatalf("Failed to init server DB: %v", err)
	}
	defer serverDB.Close()

	// Use a large shard size (100MB) so the entire 10MB file fits in one shard, 
	// ensuring each chunk produces exactly one blob entry.
	pieceSize := int64(10 * 1024 * 1024) 
	shardSize := int64(10) * pieceSize
	keepLocal := true

	engine := server.NewEngine(
		serverDB, serverBlobDir, serverQueueDir,
		10, 4, shardSize, keepLocal, nil, "", 1024,
		false, false, 8, 43200, 0.5, 720, 1440, 24, 4,
		-1, -1, -1, nil, "",
	)

	// 4. Setup Client
	clientDBPath := filepath.Join(baseDir, "client.db")
	clientDB, err := db.InitClientDB(clientDBPath)
	if err != nil {
		t.Fatalf("Failed to init client DB: %v", err)
	}
	defer clientDB.Close()

	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)

	// dummy backup record
	resChan := make(chan db.DBResult)
	dbJobChan <- db.DBJob{
		Query:      "INSERT INTO backups (start_time, status) VALUES (CURRENT_TIMESTAMP, 'running')",
		ResultChan: resChan,
	}
	backupID := (<-resChan).ID

	// Pipelines
	jobChan := make(chan client.FileJob, 100)
	uploadChan := make(chan client.UploadJob, 100)

	crawler := client.NewCrawler(clientDB, dbJobChan, []string{sourceDir}, jobChan, false)
	key := []byte("01234567890123456789012345678901")
	cryptoPool := client.NewCryptoPool(clientDB, dbJobChan, key, clientSpoolDir, clientUploadDir, 4, uploadChan, false)
	
	rpcClient := client.NewMockRPCClient(engine)
	uploader := client.NewUploader(clientDB, uploadChan, rpcClient, clientUploadDir, 10, false, false, backupID)

	// 5. Run and Wait
	uploader.Start()
	cryptoPool.Start(jobChan)
	crawler.Start(backupID)

	cryptoPool.Wait()
	close(uploadChan)
	uploader.Wait()
	close(dbJobChan)

	// 6. Assertions for Chunking!
	// 10MB / 4MB chunks = 3 blobs should be in the server database.
	var blobCount int
	err = serverDB.QueryRow("SELECT COUNT(*) FROM blobs").Scan(&blobCount)
	if err != nil {
		t.Fatalf("Failed to query server DB: %v", err)
	}

	expectedChunks := (fileSize + chunkSize - 1) / chunkSize
	if blobCount != expectedChunks {
		t.Errorf("Chunking failure! Expected %d blobs (10MB / 4MB), found %d", expectedChunks, blobCount)
	} else {
		t.Logf("Success! Large file was correctly sliced into %d blobs.", blobCount)
	}

	// 7. Restore and Verify
	restoredDir := filepath.Join(baseDir, "restored")
	restorer := client.NewRestorer(clientDB, rpcClient, key, restoredDir, false)

	err = restorer.RestoreFile(context.Background(), largeFilePath, 0, sourceDir)
	if err != nil {
		t.Fatalf("Failed to restore large file: %v", err)
	}

	restoredFilePath := filepath.Join(restoredDir, largeFileName)
	restoredData, err := os.ReadFile(restoredFilePath)
	if err != nil {
		t.Fatalf("Failed to read restored large file: %v", err)
	}

	if len(restoredData) != fileSize {
		t.Errorf("Restored file size mismatch. Expected %d, got %d", fileSize, len(restoredData))
	}

	// Byte-by-byte comparison
	for i := range largeContent {
		if restoredData[i] != largeContent[i] {
			t.Fatalf("Restored file corruption at offset %d! Data does not match original.", i)
		}
	}
	t.Log("Success! Large file was correctly reassembled and verified byte-for-byte.")
}

func TestIncrementalBackup(t *testing.T) {
	// 1. Setup Isolated Temporary Directories
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	clientSpoolDir := filepath.Join(baseDir, "spool")
	clientUploadDir := filepath.Join(baseDir, "upload")

	// Ensure directories exist
	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir, clientSpoolDir, clientUploadDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create test dir: %v", err)
		}
	}

	// 2. Setup Server
	serverDBPath := filepath.Join(baseDir, "server.db")
	serverDB, err := server.InitDB(serverDBPath)
	if err != nil {
		t.Fatalf("Failed to init server DB: %v", err)
	}
	defer serverDB.Close()

	engine := server.NewEngine(
		serverDB, serverBlobDir, serverQueueDir,
		10, 4, int64(100*1024*1024), true, nil, "", 1024,
		false, false, 8, 43200, 0.5, 720, 1440, 24, 4,
		-1, -1, -1, nil, "",
	)
	rpcClient := client.NewMockRPCClient(engine)

	// 3. Setup Client
	clientDBPath := filepath.Join(baseDir, "client.db")
	clientDB, err := db.InitClientDB(clientDBPath)
	if err != nil {
		t.Fatalf("Failed to init client DB: %v", err)
	}
	defer clientDB.Close()

	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan) // Ensure it stops after test ends

	key := []byte("01234567890123456789012345678901")

	// --- RUN 1: Initial Backup ---
	t.Log("--- RUN 1: Initial Backup ---")
	file1Path := filepath.Join(sourceDir, "file1.txt")
	content1 := "Initial Content"
	if err := os.WriteFile(file1Path, []byte(content1), 0644); err != nil {
		t.Fatalf("Failed to write file1: %v", err)
	}

	backup1ID := runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key, clientSpoolDir, clientUploadDir)

	var vCount int
	err = clientDB.QueryRow("SELECT COUNT(*) FROM file_versions").Scan(&vCount)
	if err != nil {
		t.Fatalf("Failed to query file_versions: %v", err)
	}
	if vCount != 1 {
		t.Errorf("Expected 1 file_version after run 1, found %d", vCount)
	}

	// --- RUN 2: No Changes ---
	t.Log("--- RUN 2: No Changes (Incremental Skip) ---")
	backup2ID := runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key, clientSpoolDir, clientUploadDir)

	if backup2ID == backup1ID {
		t.Fatal("Backup ID did not increment for run 2")
	}

	err = clientDB.QueryRow("SELECT COUNT(*) FROM file_versions").Scan(&vCount)
	if err != nil {
		t.Fatalf("Failed to query file_versions: %v", err)
	}
	if vCount != 1 {
		t.Errorf("Incremental backup failure! Expected STILL 1 file_version (it should have been skipped), found %d", vCount)
	} else {
		t.Log("Success! Incremental skip verified (file mtime/size unchanged).")
	}

	// --- RUN 3: Modified Content ---
	t.Log("--- RUN 3: Modified Content (New Version) ---")
	
	// Ensure mtime will definitely be greater than previous version
	time.Sleep(1100 * time.Millisecond) 

	content2 := "Updated Content"
	if err := os.WriteFile(file1Path, []byte(content2), 0644); err != nil {
		t.Fatalf("Failed to write updated file1: %v", err)
	}
	
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key, clientSpoolDir, clientUploadDir)

	err = clientDB.QueryRow("SELECT COUNT(*) FROM file_versions").Scan(&vCount)
	if err != nil {
		t.Fatalf("Failed to query file_versions: %v", err)
	}
	if vCount != 2 {
		t.Errorf("Expected 2 file_versions after run 3 (modified file), found %d", vCount)
	} else {
		t.Log("Success! New version created for modified file.")
	}

	// --- VERIFY RESTORE OF BOTH VERSIONS ---
	restoredDir1 := filepath.Join(baseDir, "restored1")
	restorer1 := client.NewRestorer(clientDB, rpcClient, key, restoredDir1, false)
	if err := restorer1.RestoreFile(context.Background(), file1Path, backup1ID, sourceDir); err != nil {
		t.Fatalf("Failed to restore version 1: %v", err)
	}
	restored1Data, _ := os.ReadFile(filepath.Join(restoredDir1, "file1.txt"))
	if string(restored1Data) != content1 {
		t.Errorf("Version 1 mismatch! Expected '%s', got '%s'", content1, string(restored1Data))
	}

	restoredDir3 := filepath.Join(baseDir, "restored3")
	restorer3 := client.NewRestorer(clientDB, rpcClient, key, restoredDir3, false)
	if err := restorer3.RestoreFile(context.Background(), file1Path, 0, sourceDir); err != nil { // Latest
		t.Fatalf("Failed to restore version 3: %v", err)
	}
	restored3Data, _ := os.ReadFile(filepath.Join(restoredDir3, "file1.txt"))
	if string(restored3Data) != content2 {
		t.Errorf("Version 3 mismatch! Expected '%s', got '%s'", content2, string(restored3Data))
	}
}

func TestFileDeletion(t *testing.T) {
	// 1. Setup Isolated Temporary Directories
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	clientSpoolDir := filepath.Join(baseDir, "spool")
	clientUploadDir := filepath.Join(baseDir, "upload")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir, clientSpoolDir, clientUploadDir} {
		os.MkdirAll(dir, 0755)
	}

	// 2. Setup Server & Client
	serverDB, _ := server.InitDB(filepath.Join(baseDir, "server.db"))
	defer serverDB.Close()
	engine := server.NewEngine(serverDB, serverBlobDir, serverQueueDir, 10, 4, 100*1024*1024, true, nil, "", 1024, false, false, 8, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "")
	rpcClient := client.NewMockRPCClient(engine)

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")

	// --- RUN 1: Backup file1.txt ---
	file1Path := filepath.Join(sourceDir, "file1.txt")
	os.WriteFile(file1Path, []byte("Content 1"), 0644)
	backup1ID := runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key, clientSpoolDir, clientUploadDir)

	// Verify not deleted
	var deleted int
	clientDB.QueryRow("SELECT deleted FROM files WHERE filename = 'file1.txt'").Scan(&deleted)
	if deleted != 0 {
		t.Errorf("Expected file1.txt to be NOT deleted, got %d", deleted)
	}

	// --- RUN 2: Delete file1.txt and Backup ---
	os.Remove(file1Path)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key, clientSpoolDir, clientUploadDir)

	// Verify marked as deleted in manifest
	clientDB.QueryRow("SELECT deleted FROM files WHERE filename = 'file1.txt'").Scan(&deleted)
	if deleted != 1 {
		t.Errorf("Expected file1.txt to be marked as deleted (1), got %d", deleted)
	} else {
		t.Log("Success! File correctly marked as deleted in local manifest.")
	}

	// --- VERIFY RESTORE BEHAVIOR ---
	restoredDir := filepath.Join(baseDir, "restored")
	restorer := client.NewRestorer(clientDB, rpcClient, key, restoredDir, false)

	// 1. Restoring "latest" should FAIL (or skip) because it's deleted
	err := restorer.Restore(context.Background(), file1Path, 0)
	if err == nil {
		t.Errorf("Expected error when restoring a deleted file as 'latest', but got nil")
	} else {
		t.Logf("Success! 'Latest' restore correctly failed for deleted file: %v", err)
	}

	// 2. Restoring Backup 1 should SUCCEED
	err = restorer.Restore(context.Background(), file1Path, backup1ID)
	if err != nil {
		t.Fatalf("Failed to restore deleted file from Backup 1: %v", err)
	}
	
	restoredFile := filepath.Join(restoredDir, "file1.txt")
	if _, err := os.Stat(restoredFile); os.IsNotExist(err) {
		t.Errorf("Restored file missing: %s", restoredFile)
	} else {
		t.Log("Success! File correctly recovered from historical backup ID.")
	}
}

func TestServerGC(t *testing.T) {
	// 1. Setup Isolated Temporary Directories
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	clientSpoolDir := filepath.Join(baseDir, "spool")
	clientUploadDir := filepath.Join(baseDir, "upload")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir, clientSpoolDir, clientUploadDir} {
		os.MkdirAll(dir, 0755)
	}

	// 2. Setup Server with tiny ShardSize to force sealing
	serverDB, _ := server.InitDB(filepath.Join(baseDir, "server.db"))
	defer serverDB.Close()
	
	shardSize := int64(10) // Tiny shard size
	engine := server.NewEngine(serverDB, serverBlobDir, serverQueueDir, 10, 4, shardSize, true, nil, "", 1024, false, false, 8, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "")
	// Override KeepDeletedMinutes for testing
	engine.KeepDeletedMinutes = 0 
	
	rpcClient := client.NewMockRPCClient(engine)

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")

	// --- RUN 1: Backup file1.txt (more than 10 bytes to seal shard) ---
	file1Path := filepath.Join(sourceDir, "file1.txt")
	content := "This is more than 10 bytes"
	os.WriteFile(file1Path, []byte(content), 0644)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key, clientSpoolDir, clientUploadDir)

	// Verify shard exists and is sealed
	var shardID int64
	var status string
	err := serverDB.QueryRow("SELECT id, status FROM shards ORDER BY id ASC LIMIT 1").Scan(&shardID, &status)
	if err != nil {
		t.Fatalf("Failed to find shard: %v", err)
	}
	if status != "sealed" {
		t.Errorf("Expected shard to be sealed, got %s", status)
	}

	// Get blob hash
	var blobHash string
	serverDB.QueryRow("SELECT hash FROM blobs LIMIT 1").Scan(&blobHash)

	// --- RUN 2: Delete blob on server ---
	err = engine.DeleteBlobs(context.Background(), []string{blobHash})
	if err != nil {
		t.Fatalf("Failed to delete blob: %v", err)
	}

	// Manually age the deleted_at timestamp
	_, err = serverDB.Exec("UPDATE blobs SET deleted_at = datetime('now', '-1 hour') WHERE hash = ?", blobHash)
	if err != nil {
		t.Fatalf("Failed to age deleted_at: %v", err)
	}

	// --- RUN 3: Trigger GC ---
	t.Log("Triggering GC...")
	engine.TriggerGC(context.Background())

	// Verify shard is deleted from DB
	var exists int
	serverDB.QueryRow("SELECT COUNT(*) FROM shards WHERE id = ?", shardID).Scan(&exists)
	if exists != 0 {
		t.Errorf("Expected shard %d to be deleted from DB, but it still exists", shardID)
	}

	// Verify shard file is gone
	shardPath := filepath.Join(serverBlobDir, fmt.Sprintf("shard_%d.dat", shardID))
	if _, err := os.Stat(shardPath); !os.IsNotExist(err) {
		t.Errorf("Expected shard file %s to be deleted from disk", shardPath)
	} else {
		t.Log("Success! Shard and blob correctly garbage collected.")
	}
}

// MockPeerHandler implements the PeerNode Cap'n Proto interface for testing.
type MockPeerHandler struct {
	Pieces map[string][]byte
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

func (h *MockPeerHandler) UploadShards(ctx context.Context, call rpc.PeerNode_uploadShards) error {
	args := call.Args()
	shards, _ := args.Shards()
	for i := 0; i < shards.Len(); i++ {
		s := shards.At(i)
		hashBytes, _ := s.Checksum()
		dataBytes, _ := s.Data()
		
		// Make safe copies
		hash := hex.EncodeToString(hashBytes)
		data := make([]byte, len(dataBytes))
		copy(data, dataBytes)
		
		h.Pieces[hash] = data
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

	data, ok := h.Pieces[hashHex]
	if !ok {
		return fmt.Errorf("piece not found: %s", hashHex)
	}

	if int(offset)+32 > len(data) {
		return fmt.Errorf("offset %d + 32 exceeds data length %d", offset, len(data))
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}
	return res.SetData(data[offset : offset+32])
}
func (h *MockPeerHandler) ReleasePiece(ctx context.Context, call rpc.PeerNode_releasePiece) error {
	res, _ := call.AllocResults()
	res.SetSuccess(true)
	return nil
}
func (h *MockPeerHandler) DownloadPiece(ctx context.Context, call rpc.PeerNode_downloadPiece) error {
	args := call.Args()
	hashBytes, _ := args.ShardChecksum()
	hashHex := hex.EncodeToString(hashBytes)

	data, ok := h.Pieces[hashHex]
	if !ok {
		return fmt.Errorf("piece not found: %s", hashHex)
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}
	return res.SetData(data)
}
func (h *MockPeerHandler) ListSpecialPieces(ctx context.Context, call rpc.PeerNode_listSpecialPieces) error {
	res, _ := call.AllocResults()
	res.NewShards(0)
	return nil
}
func (h *MockPeerHandler) Announce(ctx context.Context, call rpc.PeerNode_announce) error {
	res, _ := call.AllocResults()
	res.SetSuccess(true)
	return nil
}

func TestOutboundWorkerFlow(t *testing.T) {
	// 1. Setup Isolated Temporary Directories
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	clientSpoolDir := filepath.Join(baseDir, "spool")
	clientUploadDir := filepath.Join(baseDir, "upload")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir, clientSpoolDir, clientUploadDir} {
		os.MkdirAll(dir, 0755)
	}

	// 2. Setup Server & Mock Peer
	serverDB, _ := server.InitDB(filepath.Join(baseDir, "server.db"))
	defer serverDB.Close()

	// Use tiny shards so they seal immediately
	engine := server.NewEngine(serverDB, serverBlobDir, serverQueueDir, 1, 1, 10, true, nil, "", 1024, true, false, 8, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "")
	rpcClient := client.NewMockRPCClient(engine)

	// Register two mock peers
	mockHandler1 := &MockPeerHandler{Pieces: make(map[string][]byte)}
	mockHandler2 := &MockPeerHandler{Pieces: make(map[string][]byte)}
	
	serverDB.Exec("INSERT INTO peers (public_key, ip_address, status, max_storage_size) VALUES (?, ?, 'trusted', 100)", "peer-1", "127.0.0.1")
	serverDB.Exec("INSERT INTO peers (public_key, ip_address, status, max_storage_size) VALUES (?, ?, 'trusted', 100)", "peer-2", "127.0.0.2")
	
	var peerID1, peerID2 int64
	serverDB.QueryRow("SELECT id FROM peers WHERE public_key = 'peer-1'").Scan(&peerID1)
	serverDB.QueryRow("SELECT id FROM peers WHERE public_key = 'peer-2'").Scan(&peerID2)
	
	engine.RegisterActivePeer(peerID1, rpc.PeerNode_ServerToClient(mockHandler1))
	engine.RegisterActivePeer(peerID2, rpc.PeerNode_ServerToClient(mockHandler2))

	// 3. Backup a file to create a queued piece
	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")
	os.WriteFile(filepath.Join(sourceDir, "file1.txt"), []byte("Chunk 1 data longer than 10 bytes"), 0644)
	
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key, clientSpoolDir, clientUploadDir)

	// Manually trigger encoding to put pieces in queue
	var shardID int64
	serverDB.QueryRow("SELECT id FROM shards LIMIT 1").Scan(&shardID)
	engine.TriggerEncodeShard(shardID)

	// Verify pieces are in the queue dir
	entries, _ := os.ReadDir(serverQueueDir)
	if len(entries) == 0 {
		t.Fatal("Queue directory is empty, no pieces to upload")
	}
	t.Logf("Found %d pieces in queue before worker run", len(entries))

	// --- 4. Trigger Outbound Worker ---
	t.Log("Triggering OutboundWorker...")
	engine.TriggerOutbound(context.Background())

	// --- 5. Verify Results ---
	// 5a. Queue directory should now be empty
	entries, _ = os.ReadDir(serverQueueDir)
	if len(entries) != 0 {
		t.Errorf("Expected queue directory to be empty, but found %d files", len(entries))
	}

	// 5b. Peers should now have the pieces
	totalPiecesReceived := len(mockHandler1.Pieces) + len(mockHandler2.Pieces)
	if totalPiecesReceived < 2 {
		t.Errorf("Mock peers received only %d pieces, expected 2", totalPiecesReceived)
	} else {
		t.Logf("Success! Mock peers received %d pieces from the worker.", totalPiecesReceived)
	}

	// 5c. Database should mark pieces as uploaded
	var uploadedCount int
	serverDB.QueryRow("SELECT COUNT(*) FROM outbound_pieces WHERE status = 'uploaded'").Scan(&uploadedCount)
	if uploadedCount == 0 {
		t.Error("Database does not show any uploaded pieces")
	}
	}

	func TestChallengeWorkerFlow(t *testing.T) {
	// 1. Setup
	baseDir := t.TempDir()
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	os.MkdirAll(serverBlobDir, 0755)
	os.MkdirAll(serverQueueDir, 0755)

	serverDB, _ := server.InitDB(filepath.Join(baseDir, "server.db"))
	defer serverDB.Close()

	engine := server.NewEngine(serverDB, serverBlobDir, serverQueueDir, 1, 1, 1024, true, nil, "", 1024, true, false, 8, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "")
	engine.ChallengesPerPiece = 1 // Simplified for test

	// Register a mock peer
	mockHandler := &MockPeerHandler{Pieces: make(map[string][]byte)}
	serverDB.Exec("INSERT INTO peers (public_key, ip_address, status, max_storage_size) VALUES (?, ?, 'trusted', 100)", "peer-challenge-test", "127.0.0.1")
	var peerID int64
	serverDB.QueryRow("SELECT id FROM peers WHERE public_key = 'peer-challenge-test'").Scan(&peerID)
	engine.RegisterActivePeer(peerID, rpc.PeerNode_ServerToClient(mockHandler))

	// 2. Manually create an "uploaded" piece in DB and mock peer
	shardID := int64(1)
	pieceData := make([]byte, 1024)
	for i := range pieceData {
		pieceData[i] = byte(i % 256)
	}
	pieceHash := hex.EncodeToString(crypto.Hash(pieceData))
	mockHandler.Pieces[pieceHash] = pieceData

	serverDB.Exec("INSERT INTO shards (id, status, size) VALUES (?, 'sealed', 1024)", shardID, 1024)
	serverDB.Exec("INSERT INTO outbound_pieces (shard_id, piece_index, peer_id, status) VALUES (?, 0, ?, 'uploaded')", shardID, peerID)

	// Pre-seed a challenge
	offset := int64(100)
	expected := pieceData[offset : offset+32]
	serverDB.Exec("INSERT INTO piece_challenges (shard_id, piece_index, peer_id, piece_hash, offset, expected_data) VALUES (?, 0, ?, ?, ?, ?)", 
		shardID, peerID, pieceHash, offset, expected)

	// --- 3. Trigger Challenge Worker ---
	t.Log("Triggering ChallengeWorker...")
	engine.TriggerChallenge(context.Background())

	// --- 4. Verify Results ---
	var status string
	err := serverDB.QueryRow("SELECT status FROM challenge_results WHERE peer_id = ? AND shard_id = ?", peerID, shardID).Scan(&status)
	if err != nil {
		t.Fatalf("No challenge result found in DB: %v", err)
	}

	if status != "pass" {
		t.Errorf("Expected challenge status 'pass', got '%s'", status)
	} else {
		t.Log("Success! Challenge passed verification.")
	}

	// Verify challenge was consumed
	var count int
	serverDB.QueryRow("SELECT COUNT(*) FROM piece_challenges").Scan(&count)
	if count != 0 {
		t.Error("Challenge was not consumed (deleted) from piece_challenges table")
	}
	}

	func TestRepairWorkerFlow(t *testing.T) {
	// 1. Setup Isolated Temporary Directories
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	clientSpoolDir := filepath.Join(baseDir, "spool")
	clientUploadDir := filepath.Join(baseDir, "upload")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir, clientSpoolDir, clientUploadDir} {
		os.MkdirAll(dir, 0755)
	}

	// 2. Setup Server with DataShards=2, ParityShards=1
	serverDB, _ := server.InitDB(filepath.Join(baseDir, "server.db"))
	defer serverDB.Close()

	shardSize := int64(1 * 1024 * 1024)
	engine := server.NewEngine(serverDB, serverBlobDir, serverQueueDir, 2, 1, shardSize, true, nil, "", 1024, true, false, 8, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "")
	rpcClient := client.NewMockRPCClient(engine)

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")

	// --- STEP 1: Create a healthy shard with 3 pieces ---
	os.WriteFile(filepath.Join(sourceDir, "file1.txt"), []byte("Redundancy test data"), 0644)
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key, clientSpoolDir, clientUploadDir)

	var shardID int64
	serverDB.QueryRow("SELECT id FROM shards LIMIT 1").Scan(&shardID)
	// Manually seal and encode
	serverDB.Exec("UPDATE shards SET status = 'sealed', size = ?, total_pieces = 3 WHERE id = ?", shardSize, shardID)
	engine.TriggerEncodeShard(shardID)

	// Mock 3 peers and mark pieces as uploaded
	for i := 0; i < 3; i++ {
		serverDB.Exec("INSERT INTO peers (public_key, ip_address, status) VALUES (?, ?, 'trusted')", fmt.Sprintf("repair-peer-%d", i), "127.0.0.1")
		var pid int64
		serverDB.QueryRow("SELECT id FROM peers WHERE public_key = ?", fmt.Sprintf("repair-peer-%d", i)).Scan(&pid)
		serverDB.Exec("INSERT INTO outbound_pieces (shard_id, piece_index, peer_id, status) VALUES (?, ?, ?, 'uploaded')", shardID, i, pid)
	}

	// Clean queue dir after initial "upload"
	entries, _ := os.ReadDir(serverQueueDir)
	for _, e := range entries {
		os.Remove(filepath.Join(serverQueueDir, e.Name()))
	}

	// --- STEP 2: Simulate Failure (Mark one piece as lost) ---
	t.Log("Simulating piece loss...")
	serverDB.Exec("UPDATE outbound_pieces SET status = 'lost' WHERE piece_index = 2")

	// Verify redundancy is now 2/3
	var healthy int
	serverDB.QueryRow("SELECT COUNT(*) FROM outbound_pieces WHERE shard_id = ? AND status = 'uploaded'", shardID).Scan(&healthy)
	if healthy != 2 {
		t.Fatalf("Expected 2 healthy pieces, got %d", healthy)
	}

	// --- STEP 3: Trigger Repair Worker ---
	t.Log("Triggering RepairWorker...")
	engine.TriggerRepair(context.Background())

	// --- STEP 4: Verify Results ---
	// Repair worker should have enqueued the missing piece back into serverQueueDir
	entries, _ = os.ReadDir(serverQueueDir)
	found := false
	for _, e := range entries {
		if strings.Contains(e.Name(), fmt.Sprintf("shard_%d_piece_2", shardID)) {
			found = true
			break
		}
	}

	if !found {
		t.Error("RepairWorker failed to re-enqueue the lost piece (piece 2)")
	} else {
		t.Log("Success! RepairWorker detected loss and re-encoded the missing piece.")
	}
}

func TestReedSolomonIntegration(t *testing.T) {
	// 1. Setup Isolated Temporary Directories
	baseDir := t.TempDir()
	sourceDir := filepath.Join(baseDir, "source")
	serverBlobDir := filepath.Join(baseDir, "server_blobs")
	serverQueueDir := filepath.Join(baseDir, "server_queue")
	clientSpoolDir := filepath.Join(baseDir, "spool")
	clientUploadDir := filepath.Join(baseDir, "upload")

	for _, dir := range []string{sourceDir, serverBlobDir, serverQueueDir, clientSpoolDir, clientUploadDir} {
		os.MkdirAll(dir, 0755)
	}

	// 2. Setup Server with DataShards=2, ParityShards=1
	serverDB, _ := server.InitDB(filepath.Join(baseDir, "server.db"))
	defer serverDB.Close()

	shardSize := int64(1 * 1024 * 1024) // 1MB shards for fast testing
	engine := server.NewEngine(serverDB, serverBlobDir, serverQueueDir, 2, 1, shardSize, true, nil, "", 1024, true, false, 8, 43200, 0.5, 720, 1440, 24, 4, -1, -1, -1, nil, "")
	
	rpcClient := client.NewMockRPCClient(engine)

	clientDB, _ := db.InitClientDB(filepath.Join(baseDir, "client.db"))
	defer clientDB.Close()
	dbJobChan := make(chan db.DBJob, 100)
	go db.StartDBWriter(clientDB, dbJobChan)
	defer close(dbJobChan)

	key := []byte("01234567890123456789012345678901")

	// --- STEP 1: Backup a file to create a shard ---
	file1Path := filepath.Join(sourceDir, "file1.txt")
	content := make([]byte, 512*1024) // 512KB file
	for i := range content {
		content[i] = byte(i % 256)
	}
	os.WriteFile(file1Path, content, 0644)
	
	_ = runBackupCycle(t, clientDB, dbJobChan, rpcClient, []string{sourceDir}, key, clientSpoolDir, clientUploadDir)

	// Force sealing and encoding of the shard
	var shardID int64
	serverDB.QueryRow("SELECT id FROM shards ORDER BY id ASC LIMIT 1").Scan(&shardID)
	
	// Seal it manually to trigger encoding
	_, err := serverDB.Exec("UPDATE shards SET status = 'sealed', size = ?, total_pieces = 3 WHERE id = ?", shardSize, shardID)
	if err != nil {
		t.Fatalf("Failed to seal shard: %v", err)
	}

	t.Logf("Triggering erasure coding for shard %d...", shardID)
	engine.TriggerEncodeShard(shardID)

	// --- STEP 2: Intercept the pieces and mock peers ---
	peerHandlers := []*MockPeerHandler{
		{Pieces: make(map[string][]byte)},
		{Pieces: make(map[string][]byte)},
		{Pieces: make(map[string][]byte)},
	}

	for i := 0; i < 3; i++ {
		piecePath := filepath.Join(serverQueueDir, fmt.Sprintf("shard_%d_piece_%d", shardID, i))
		data, err := os.ReadFile(piecePath)
		if err != nil {
			t.Fatalf("Failed to read piece %d: %v", i, err)
		}
		
		hash := hex.EncodeToString(crypto.Hash(data))
		peerHandlers[i].Pieces[hash] = data

		// Register peer in DB
		res, _ := serverDB.Exec("INSERT INTO peers (public_key, ip_address, status) VALUES (?, ?, 'trusted')", fmt.Sprintf("peer-%d", i), "127.0.0.1")
		peerID, _ := res.LastInsertId()

		// Register active peer client
		node := rpc.PeerNode_ServerToClient(peerHandlers[i])
		engine.RegisterActivePeer(peerID, node)

		// Link piece to peer in DB
		_, err = serverDB.Exec("INSERT INTO outbound_pieces (shard_id, piece_index, peer_id, status) VALUES (?, ?, ?, 'uploaded')", shardID, i, peerID)
		if err != nil {
			t.Fatalf("Failed to link piece to peer: %v", err)
		}
		_, err = serverDB.Exec("INSERT INTO piece_challenges (shard_id, piece_index, peer_id, piece_hash, offset, expected_data) VALUES (?, ?, ?, ?, ?, ?)", shardID, i, peerID, hash, 0, []byte{0})
		if err != nil {
			t.Fatalf("Failed to add piece challenge: %v", err)
		}
	}

	// --- STEP 3: Delete local shard and "break" one peer ---
	shardPath := filepath.Join(serverBlobDir, fmt.Sprintf("shard_%d.dat", shardID))
	os.Remove(shardPath)
	if _, err := os.Stat(shardPath); !os.IsNotExist(err) {
		t.Fatal("Failed to delete local shard file")
	}

	// Break Peer 2 (Piece 2 - Parity)
	// We'll just remove it from the engine's active peers and the DB for this shard
	serverDB.Exec("DELETE FROM outbound_pieces WHERE shard_id = ? AND piece_index = 2", shardID)

	// --- STEP 4: Trigger Reconstruction ---
	t.Log("Triggering reconstruction from remaining 2 pieces...")
	err = engine.EnsureShardLocal(context.Background(), shardID)
	if err != nil {
		t.Fatalf("Reconstruction failed: %v", err)
	}

	// --- STEP 5: Verify results ---
	if _, err := os.Stat(shardPath); os.IsNotExist(err) {
		t.Fatal("Reconstructed shard file is still missing!")
	}

	// Verify content by restoring the file
	restoredDir := filepath.Join(baseDir, "restored")
	restorer := client.NewRestorer(clientDB, rpcClient, key, restoredDir, false)
	err = restorer.RestoreFile(context.Background(), file1Path, 0, sourceDir)
	if err != nil {
		t.Fatalf("Restore after reconstruction failed: %v", err)
	}

	restoredData, _ := os.ReadFile(filepath.Join(restoredDir, "file1.txt"))
	if string(restoredData) != string(content) {
		t.Error("Restored data mismatch after RS reconstruction!")
	} else {
		t.Log("Success! Shard was perfectly reconstructed from 2 out of 3 pieces.")
	}
}

// runBackupCycle is a helper that starts a full backup cycle and waits for it to complete.
func runBackupCycle(t *testing.T, clientDB *sql.DB, dbJobChan chan db.DBJob, rpcClient *client.MockRPCClient, backupDirs []string, key []byte, spoolDir, uploadDir string) int64 {
	// Create backup record
	resChan := make(chan db.DBResult)
	dbJobChan <- db.DBJob{
		Query:      "INSERT INTO backups (start_time, status) VALUES (CURRENT_TIMESTAMP, 'running')",
		ResultChan: resChan,
	}
	backupID := (<-resChan).ID

	// Pipelines
	jobChan := make(chan client.FileJob, 100)
	uploadChan := make(chan client.UploadJob, 100)

	crawler := client.NewCrawler(clientDB, dbJobChan, backupDirs, jobChan, false)
	cryptoPool := client.NewCryptoPool(clientDB, dbJobChan, key, spoolDir, uploadDir, 2, uploadChan, false)
	uploader := client.NewUploader(clientDB, uploadChan, rpcClient, uploadDir, 10, false, false, backupID)

	// Start
	uploader.Start()
	cryptoPool.Start(jobChan)
	crawler.Start(backupID)

	// Wait
	cryptoPool.Wait()
	close(uploadChan)
	uploader.Wait()

	// Update backup status to complete
	dbJobChan <- db.DBJob{
		Query:      "UPDATE backups SET end_time = CURRENT_TIMESTAMP, status = 'complete' WHERE id = ?",
		Args:       []interface{}{backupID},
		ResultChan: resChan,
	}
	<-resChan

	return backupID
}
