package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"p2p-backup/internal/client"
	"p2p-backup/internal/config"
	"p2p-backup/internal/db"
	"p2p-backup/internal/server"
)

type verbosity int

func (v *verbosity) String() string {
	return fmt.Sprintf("%d", *v)
}

func (v *verbosity) Set(s string) error {
	if s == "true" || s == "" {
		*v++
	} else {
		fmt.Sscanf(s, "%d", v)
	}
	return nil
}

func (v *verbosity) IsBoolFlag() bool {
	return true
}

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "client.yaml", "Path to the client configuration file")
	var v verbosity
	flag.Var(&v, "v", "Enable verbose logging (use -v -v for extra verbosity)")
	dbPath := flag.String("db", "backup.db", "Path to the local SQLite database")
	// Server engine flags for local testing
	serverKeepLocalCopy := flag.Bool("server-keep-local", true, "Keep local copy of data on server")
	serverUntrustedLimitMB := flag.Int("server-untrusted-limit-mb", 1024, "Maximum untrusted data limit in MB")
	serverChallengesPerPiece := flag.Int("server-challenges-per-piece", 8, "Number of challenges to generate per piece")
	flag.Parse()

	verbose := v > 0
	extraVerbose := v > 1

	// Load configuration
	cfg, err := config.LoadClientConfig(*configPath)
	if err != nil {
		log.Printf("Warning: Failed to load config file %s: %v. Using defaults.", *configPath, err)
		cfg = &config.ClientConfig{} // Use empty config, defaults will be applied
	}

	// Override flag with config if provided
	if cfg.Storage.SQLitePath != "" {
		*dbPath = cfg.Storage.SQLitePath
	}

	// The remaining arguments are the directories to back up
	dirs := flag.Args()
	if len(dirs) == 0 {
		fmt.Println("Usage: localtest [options] <dir1> <dir2> ...")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Ensure all provided directories exist
	var backupDirs []string
	for _, dir := range dirs {
		absPath, err := filepath.Abs(dir)
		if err != nil {
			log.Fatalf("Failed to resolve absolute path for %s: %v", dir, err)
		}

		info, err := os.Stat(absPath)
		if err != nil {
			log.Fatalf("Failed to access directory %s: %v", absPath, err)
		}
		if !info.IsDir() {
			log.Fatalf("Path %s is not a directory", absPath)
		}
		backupDirs = append(backupDirs, absPath)
	}

	// --- SETUP SERVER ENGINE ---
	serverDBPath := "server_state.db"
	serverBlobDir := "server_blobs"

	if err := os.MkdirAll(serverBlobDir, 0755); err != nil {
		log.Fatalf("Failed to create server blob dir: %v", err)
	}

	serverDB, err := server.InitDB(serverDBPath)
	if err != nil {
		log.Fatalf("Failed to initialize server DB: %v", err)
	}
	defer serverDB.Close()

	pieceSize := int64(256 * 1024 * 1024) // 256MB per distributed piece
	shardSize := int64(10) * pieceSize    // 10 data shards
	serverQueueDir := "server_queue"
	engine := server.NewEngine(
		serverDB,
		"", // No server config path for local testing
		serverDBPath,
		serverBlobDir,
		serverQueueDir,
		10,
		4,
		shardSize,
		*serverKeepLocalCopy,
		nil,
		"", // No listenAddress for localtest
		*serverUntrustedLimitMB,
		verbose,
		extraVerbose,
		*serverChallengesPerPiece,
		60*24*30, // Default 30 days for keepDeletedMinutes
		60*24*30, // Default 30 days for keepMetadataMinutes
		0.5,      // Default 0.5 for wasteThreshold
		720,      // Default 12 hours for gcIntervalMinutes
		1440,     // Default 24 hours for selfBackupIntervalMinutes
		24,       // Default 24 hours for peerEvictionHours
		4,        // Default 4 pieces buffer for basePieceBuffer
		-1,       // Default no global storage limit
		-1,       // Default no upload limit
		-1,       // Default no download limit
		nil,      // No master key for localtest
		"",       // No admin key for localtest
		"",       // No contact info for localtest
		4,        // Default 4 concurrent streams
		false,    // Not standalone mode
		false,    // Adoption disabled
		0,        // Adoption period minutes
		0,        // Adoption challenge pieces
	)
	// ---------------------------

	// --- SETUP CLIENT ---
	database, err := db.InitClientDB(*dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize client database: %v", err)
	}
	// Do NOT defer database.Close() here, close it after pipeline finishes

	// Set up channels for the pipeline
	jobChan := make(chan client.FileJob, 1000)
	uploadChan := make(chan client.UploadJob, 1000)
	dbJobChan := make(chan db.DBJob, 1000)

	db.StartDBWriter(database, dbJobChan)

	crawler := client.NewCrawler(database, dbJobChan, backupDirs, jobChan, 4, verbose)

	key := []byte("01234567890123456789012345678901")
	archiveChan := make(chan client.FileArchive, 1000)
	cryptoPool := client.NewCryptoPool(key, 4, uploadChan, archiveChan, false, true, nil, 10, 1)

	// Connect the client to the local server engine
	rpcClient := client.NewMockRPCClient(engine)

	batchSize := cfg.Pipeline.BatchUploadSize
	if batchSize <= 0 {
		batchSize = 100
	}

	// Create dummy backup record for localtest
	resChan := make(chan db.DBResult)
	dbJobChan <- db.DBJob{
		Query:      "INSERT INTO backups (start_time, status) VALUES (CURRENT_TIMESTAMP, 'running')",
		ResultChan: resChan,
	}
	res := <-resChan
	if res.Err != nil {
		log.Fatalf("Failed to create dummy backup record: %v", res.Err)
	}
	backupID := res.ID

	uploader := client.NewUploader((chan<- db.DBJob)(dbJobChan), uploadChan, rpcClient, 2, batchSize, false, verbose, backupID, nil)
	stateManager := client.NewStateManager(database, dbJobChan, archiveChan, verbose)

	if verbose {
		log.Println("Pipeline components initialized. Starting threads...")
	}

	go stateManager.Start()
	uploader.Start()
	cryptoPool.Start(jobChan)
	crawler.Start(backupID)

	// Wait for CryptoPool to finish processing all jobs
	cryptoPool.Wait()

	if verbose {
		log.Println("Crypto workers finished. Closing upload channel...")
	}

	// Close uploadChan to signal Uploader that no more uploads are coming
	close(uploadChan)

	// Wait for Uploader to finish uploading all files
	uploader.Wait()

	database.Close()

	log.Println("Backup completed successfully!")
}
