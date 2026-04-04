package main
import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/libp2p/go-libp2p"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"p2p-backup/internal/client"
	"p2p-backup/internal/config"
	"p2p-backup/internal/crypto"
	"p2p-backup/internal/db"
	"p2p-backup/internal/rpc"
)

const Version = "0.0.9"

type verbosity int

func (v *verbosity) String() string {
	return fmt.Sprintf("%d", *v)
}

func (v *verbosity) Set(s string) error {
	if s == "true" || s == "" {
		*v++
	} else {
		i, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		*v = verbosity(i)
	}
	return nil
}

func (v *verbosity) IsBoolFlag() bool {
	return true
}

func main() {
	// Parse command-line flags
	configPath := flag.String("config", "client.yaml", "Path to the client configuration file")
	fakeUpload := flag.Bool("fake-upload", false, "Simulate upload process without connecting to server")
	var v verbosity
	flag.Var(&v, "v", "Enable verbose logging (use -v -v for extra verbosity)")
	dbPath := flag.String("db", "backup.db", "Path to the local SQLite database")
	flag.Parse()

	verbose := v > 0
	extraVerbose := v > 1

	log.Printf("Starting P2P Backup Client v%s", Version)

	// Load configuration
	cfg, err := config.LoadClientConfig(*configPath)
	if err != nil {
		log.Printf("Warning: Failed to load config file %s: %v. Using defaults.", *configPath, err)
		cfg = &config.ClientConfig{} // Use empty config, defaults will be applied
	}

	// Override flag with config if provided
	actualDBPath := *dbPath
	if cfg.Storage.SQLitePath != "" {
		actualDBPath = cfg.Storage.SQLitePath
	}

	// The remaining arguments are the directories to back up or subcommands
	args := flag.Args()
	if len(args) == 0 {
		fmt.Println("Usage: client [options] <command> [args...]")
		fmt.Println("Commands:")
		fmt.Println("  backup <dir1> [dir2] ...   Backup specified directories")
		fmt.Println("  restore [-b backup_id] <file or dir> [destination_dir] Restore specified file/directory to a destination (default: newest backup, to 'restored_files' dir)")
		fmt.Println("  history <file>             List all backed-up versions of a file")
		fmt.Println("  status                     Show server status")
		fmt.Println("  prune                      Find and delete orphaned blobs on the server")
		fmt.Println("  reset                      Force a full re-scan and re-offer of all files")
		fmt.Println("  addpeer <address:port>     Discover and add a new dynamic remote peer via TLS Handshake")
		fmt.Println("  updatepeer <id> <status>   Modify a peer's trusted status explicitly (e.g. 'trusted', 'blocked')")
		fmt.Println("  listpeers                  Show all actively tracked remote nodes and their metrics")
		fmt.Println("  addclient <key> <status> [--max=GB] Manually authorize a client by Public Key")
		fmt.Println("  updateclient <id> <status> [--max=GB] Update an existing client's status/quota")
		fmt.Println("  listclients                Show all known clients and their storage usage")
		fmt.Println("  keygen                     Generate a new 24-word recovery mnemonic and public key")
		fmt.Println("  identity                   Show public key derived from mnemonic in config")
		flag.PrintDefaults()
		os.Exit(1)
	}

	command := args[0]
	isStatusCmd := (command == "status")
	isRestoreCmd := (command == "restore")
	isHistoryCmd := (command == "history")
	isBackupCmd := (command == "backup")
	isPruneCmd := (command == "prune")
	isResetCmd := (command == "reset")
	isAddPeerCmd := (command == "addpeer")
	isUpdatePeerCmd := (command == "updatepeer")
	isListPeersCmd := (command == "listpeers")
	isAddClientCmd := (command == "addclient")
	isUpdateClientCmd := (command == "updateclient")
	isListClientsCmd := (command == "listclients")
	isKeygenCmd := (command == "keygen")
	isIdentityCmd := (command == "identity")

	if isIdentityCmd {
		if cfg.Mnemonic == "" {
			log.Fatal("Identity command requires a mnemonic in the config file")
		}
		id, err := crypto.DeriveIdentity(cfg.Mnemonic, false)
		if err != nil {
			log.Fatalf("Failed to derive identity: %v", err)
		}
		leaf, _ := x509.ParseCertificate(id.TLSCert.Certificate[0])
		pubKeyBytes, _ := x509.MarshalPKIXPublicKey(leaf.PublicKey)
		fmt.Printf("Client Public Key (Hex): %x\n", pubKeyBytes)
		return
	}

	if isKeygenCmd {
		m, err := crypto.GenerateMnemonic()
		if err != nil {
			log.Fatalf("Failed to generate mnemonic: %v", err)
		}
		id, err := crypto.DeriveIdentity(m, false)
		if err != nil {
			log.Fatalf("Failed to derive identity: %v", err)
		}
		
		// Extract Public Key Hex for sharing
		leaf, err := x509.ParseCertificate(id.TLSCert.Certificate[0])
		if err != nil {
			log.Fatalf("Failed to parse generated cert: %v", err)
		}
		pubKeyBytes, _ := x509.MarshalPKIXPublicKey(leaf.PublicKey)
		
		fmt.Println("=== NEW CLIENT IDENTITY GENERATED ===")
		fmt.Printf("Mnemonic: %s\n\n", m)
		fmt.Printf("Public Key (Hex): %x\n", pubKeyBytes)
		fmt.Println("======================================")
		fmt.Println("IMPORTANT: Write down the mnemonic. It is the ONLY way to recover your client.")
		return
	}

	if !isStatusCmd && !isRestoreCmd && !isHistoryCmd && !isBackupCmd && !isPruneCmd && !isResetCmd && !isAddPeerCmd && !isUpdatePeerCmd && !isListPeersCmd && !isUpdateClientCmd && !isListClientsCmd && !isKeygenCmd && !isIdentityCmd && !isAddClientCmd {
		log.Fatalf("Unknown command: %s. Expected keygen, identity, status, backup, restore, history, prune, reset, addpeer, updatepeer, listpeers, addclient, updateclient, or listclients.", command)
	}

	var backupSources []config.BackupSource
	isBackupListCmd := false
	if isBackupCmd {
		if len(args) > 1 && (args[1] == "--list" || args[1] == "-list") {
			isBackupListCmd = true
		} else if len(args) >= 2 {
			// Explicit directories provided on CLI
			for _, dir := range args[1:] {
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
				backupSources = append(backupSources, config.BackupSource{Path: absPath})
			}
		} else {
			// Use configured directories
			if len(cfg.BackupDirectories) == 0 {
				log.Fatalf("Usage: client backup [--list] <dir1> [dir2] ... (or configure backup_directories in %s)", *configPath)
			}
			// Normalize paths from config
			for _, src := range cfg.BackupDirectories {
				absPath, err := filepath.Abs(src.Path)
				if err != nil {
					log.Printf("Warning: Failed to resolve absolute path for configured dir %s: %v", src.Path, err)
					continue
				}
				
				// Normalize excludes
				var absExcludes []string
				for _, ex := range src.Excludes {
					exPath, err := filepath.Abs(ex)
					if err == nil {
						absExcludes = append(absExcludes, exPath)
					} else {
						absExcludes = append(absExcludes, ex) // Fallback to raw pattern
					}
				}
				
				backupSources = append(backupSources, config.BackupSource{
					Path:     absPath,
					Excludes: absExcludes,
				})
			}
		}
	}

	if verbose {
		log.Printf("Verbose logging: ENABLED")
		log.Printf("Fake upload: %v", *fakeUpload)
		log.Printf("Database path: %s", actualDBPath)
		if isBackupCmd && !isBackupListCmd {
			var paths []string
			for _, s := range backupSources {
				paths = append(paths, s.Path)
			}
			log.Printf("Backup directories: %s", strings.Join(paths, ", "))
		}
	}

	// Initialize the local SQLite database
	database, err := db.InitClientDB(actualDBPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	if isBackupListCmd {
		rows, err := database.Query("SELECT id, start_time, end_time, status, offered_files, uploaded_files FROM backups ORDER BY id DESC")
		if err != nil {
			log.Fatalf("Failed to query backups: %v", err)
		}
		defer rows.Close()

		fmt.Println("=== Backup History ===")
		fmt.Printf("%-5s | %-20s | %-20s | %-10s | %-10s | %-10s\n", "ID", "Start Time", "End Time", "Status", "Offered", "Uploaded")
		fmt.Println(strings.Repeat("-", 85))
		
		var count int
		for rows.Next() {
			var id int
			var startTime string
			var endTime sql.NullString
			var status string
			var offered int
			var uploaded int
			if err := rows.Scan(&id, &startTime, &endTime, &status, &offered, &uploaded); err != nil {
				log.Printf("Error scanning row: %v", err)
				continue
			}
			
			endStr := "Running/Incomplete"
			if endTime.Valid {
				endStr = endTime.String
			}
			
			fmt.Printf("%-5d | %-20s | %-20s | %-10s | %-10d | %-10d\n", id, startTime, endStr, status, offered, uploaded)
			count++
		}
		if count == 0 {
			fmt.Println("No backups found.")
		}
		fmt.Println("======================")
		
		database.Close()
		return
	}

	if isResetCmd {
		log.Println("Resetting local deduplication cache and sync states...")
		
		_, err = database.Exec(`DELETE FROM local_blobs`)
		if err != nil {
			log.Fatalf("Failed to clear local_blobs: %v", err)
		}
		
		_, err = database.Exec(`UPDATE directories SET last_seen = '1970-01-01 00:00:00'`)
		if err != nil {
			log.Fatalf("Failed to reset directories last_seen: %v", err)
		}
		
		_, err = database.Exec(`UPDATE file_versions SET mtime = 0`)
		if err != nil {
			log.Fatalf("Failed to reset files mtime: %v", err)
		}
		
		log.Println("Reset complete. The next 'backup' command will perform a full rescan and re-offer of all active files to the server.")
		database.Close()
		return
	}
	// Do NOT defer database.Close() here, close it after pipeline finishes

	// 1. Identity Derivation (Mnemonic Mandatory)
	if cfg.Mnemonic == "" {
		log.Fatal("Mnemonic is mandatory in client.yaml (currently empty). Run 'client keygen' to generate one.")
	}

	id, err := crypto.DeriveIdentity(cfg.Mnemonic, false)
	if err != nil {
		log.Fatalf("Failed to derive identity from mnemonic: %v", err)
	}
	key := id.MasterKey
	
	// Convert to libp2p key
	edPrivKey := id.TLSCert.PrivateKey.(ed25519.PrivateKey)
	p2pPrivKey, err := crypto.Libp2pKeyFromEd25519(edPrivKey)
	if err != nil {
		log.Fatalf("Failed to convert client identity to libp2p key: %v", err)
	}

	var rpcClient client.RPCClient
	if *fakeUpload {
		rpcClient = client.NewMockRPCClient(nil)
	} else {
		serverAddr := cfg.Server.Address
		if serverAddr == "" {
			serverAddr = "127.0.0.1:8080"
		}
		
		if cfg.Server.ExpectedServerKey == "" {
			log.Fatal("expected_server_key is mandatory in the 'server:' section of client.yaml. Run 'server identity' on your server to get it.")
		}

		rm, rmErr := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits))
		if rmErr != nil {
			log.Fatalf("Failed to initialize resource manager: %v", rmErr)
		}

		// Initialize libp2p host for the client (using random ephemeral ports since it's just dialling out)
		p2pHost, err := libp2p.New(
			libp2p.Identity(p2pPrivKey),
			libp2p.NoListenAddrs, // Client doesn't need to listen for incoming connections
			libp2p.ResourceManager(rm),
		)
		if err != nil {
			log.Fatalf("Failed to start libp2p client host: %v", err)
		}

		ctx := context.Background()
		rpcClient, err = client.NewCapnpRPCClient(ctx, p2pHost, serverAddr, cfg.Server.ExpectedServerKey, verbose, extraVerbose)
		if err != nil {
			log.Fatalf("Failed to connect to server at %s: %v", serverAddr, err)
		}
		defer rpcClient.Close()
	}

	if isAddPeerCmd {
		if len(args) < 2 {
			log.Fatalf("Usage: client addpeer <address:port>")
		}
		
		ctx := context.Background()
		targetAddress := args[1]
		log.Printf("Adding peer at %s...", targetAddress)
		
		err := rpcClient.AddPeer(ctx, targetAddress)
		if err != nil {
			log.Fatalf("Failed to add peer: %v", err)
		}
		log.Println("Peer added successfully.")
		
		database.Close()
		return
	}

	if isUpdatePeerCmd {
		updateCmd := flag.NewFlagSet("updatepeer", flag.ExitOnError)
		maxGB := updateCmd.Uint64("max", 0, "Set maximum storage quota in GB")
		updateCmd.Parse(args[1:])

		parsedArgs := updateCmd.Args()
		if len(parsedArgs) < 2 {
			log.Fatalf("Usage: client updatepeer <id> <status> [--max=GB]")
		}

		ctx := context.Background()
		peerID, err := strconv.ParseInt(parsedArgs[0], 10, 64)
		if err != nil {
			log.Fatalf("Invalid peer ID: %v", err)
		}
		targetStatus := parsedArgs[1]

		log.Printf("Updating peer %d to status '%s' (Max: %d GB)...", peerID, targetStatus, *maxGB)

		err = rpcClient.UpdatePeer(ctx, peerID, targetStatus, *maxGB)
		if err != nil {
			log.Fatalf("Failed to update peer: %v", err)
		}
		log.Println("Peer updated successfully.")

		database.Close()
		return
	}
	if isListPeersCmd {
		ctx := context.Background()
		peers, err := rpcClient.ListPeers(ctx)
		if err != nil {
			log.Fatalf("Failed to list peers: %v", err)
		}

		fmt.Println("\n=== Active P2P Swarm Nodes ===")
		fmt.Printf("%-3s | %-20s | %-10s | %-10s | %-10s | %-7s | %-7s | %-7s | %-7s | %-20s | %s\n", "ID", "IP Address", "Status", "Inbound MB", "Outbound MB", "Total", "Curr", "Uptime", "Pass%", "Contact", "Last Seen")
		fmt.Println(strings.Repeat("-", 170))
		for _, p := range peers {
			var uptimePct, passPct float64
			if p.ChallengesMade > 0 {
				uptimePct = float64(p.ConnectionsOk) / float64(p.ChallengesMade) * 100
			}
			if p.IntegrityAttempts > 0 {
				passPct = float64(p.ChallengesPassed) / float64(p.IntegrityAttempts) * 100
			}
			fmt.Printf("%-3d | %-20s | %-10s | %10.2f | %10.2f | %-7d | %-7d | %5.1f%% | %5.1f%% | %-20s | %s\n",
				p.ID, p.IPAddress, p.Status, 
				float64(p.CurrentStorageSize)/(1024*1024), 
				float64(p.OutboundStorageSize)/(1024*1024),
				p.TotalShards, p.CurrentShards,
				uptimePct, passPct, p.ContactInfo, p.LastSeen)
		}
		fmt.Println("==============================")
		
		database.Close()
		return
	}

	if isListClientsCmd {
		ctx := context.Background()
		clients, err := rpcClient.ListClients(ctx)
		if err != nil {
			log.Fatalf("Failed to list clients: %v", err)
		}

		fmt.Println("\n=== Known Backup Clients ===")
		fmt.Printf("%-3s | %-66s | %-10s | %-10s | %-10s | %s\n", "ID", "Public Key", "Status", "Used MB", "Max GB", "Last Seen")
		fmt.Println(strings.Repeat("-", 125))
		for _, c := range clients {
			fmt.Printf("%-3d | %-66s | %-10s | %-10d | %-10d | %s\n",
				c.ID, c.PublicKey, c.Status, c.CurrentStorageSize/(1024*1024),
				c.MaxStorageSize/(1024*1024*1024), c.LastSeen)
		}
		fmt.Println("============================")
		
		database.Close()
		return
	}

	if isUpdateClientCmd {
		updateCmd := flag.NewFlagSet("updateclient", flag.ExitOnError)
		maxGB := updateCmd.Uint64("max", 0, "Set maximum storage quota in GB")
		updateCmd.Parse(args[1:])

		parsedArgs := updateCmd.Args()
		if len(parsedArgs) < 2 {
			log.Fatalf("Usage: client updateclient <id> <status> [--max=GB]")
		}
		
		ctx := context.Background()
		clientID, err := strconv.ParseUint(parsedArgs[0], 10, 64)
		if err != nil {
			log.Fatalf("Invalid client ID: %v", err)
		}
		targetStatus := parsedArgs[1]
		
		log.Printf("Updating client %d to status '%s' (Max: %d GB)...", clientID, targetStatus, *maxGB)
		
		err = rpcClient.UpdateClient(ctx, clientID, targetStatus, *maxGB)
		if err != nil {
			log.Fatalf("Failed to update client: %v", err)
		}
		log.Println("Client updated successfully.")
		
		database.Close()
		return
	}

	if isAddClientCmd {
		addCmd := flag.NewFlagSet("addclient", flag.ExitOnError)
		maxGB := addCmd.Uint64("max", 0, "Set maximum storage quota in GB")
		addCmd.Parse(args[1:])

		parsedArgs := addCmd.Args()
		if len(parsedArgs) < 2 {
			log.Fatalf("Usage: client addclient <public_key> <status> [--max=GB]")
		}
		
		ctx := context.Background()
		clientPK := parsedArgs[0]
		targetStatus := parsedArgs[1]
		
		log.Printf("Adding client %s... with status '%s' (Max: %d GB)...", clientPK[:16], targetStatus, *maxGB)
		
		err = rpcClient.AddClient(ctx, clientPK, targetStatus, *maxGB)
		if err != nil {
			log.Fatalf("Failed to add client: %v", err)
		}
		log.Println("Client added successfully.")
		
		database.Close()
		return
	}

	if isStatusCmd {
		ctx := context.Background()
		status, err := rpcClient.GetStatus(ctx)
		if err != nil {
			log.Fatalf("Failed to get server status: %v", err)
		}

		fmt.Println("\n=== Server Status & P2P Swarm Metrics ===")
		fmt.Printf("Uptime:                        %d seconds\n", status.UptimeSeconds)
		fmt.Printf("Total Local Shards:            %d\n", status.TotalShards)
		fmt.Printf("Fully Replicated Shards:       %d\n", status.FullyReplicatedShards)
		fmt.Printf("Partially Replicated Shards:   %d\n", status.PartiallyReplicatedShards)
		fmt.Printf("Hosted Peer Shards:            %d\n", status.HostedPeerShards)
		fmt.Printf("Queued Data:                   %.2f MB\n", float64(status.QueuedBytes)/(1024*1024))
		fmt.Println("=========================================")
		
		database.Close()
		return
	}

	if isRestoreCmd {
		restoreCmd := flag.NewFlagSet("restore", flag.ExitOnError)
		backupIDPtr := restoreCmd.Int64("b", 0, "Specify a backup ID to restore from (defaults to newest)")
		restoreCmd.Parse(args[1:])

		parsedArgs := restoreCmd.Args()
		if len(parsedArgs) < 1 {
			log.Fatalf("Usage: client restore [-b backup_id] <file or dir> [destination_dir]")
		}
		
		target := parsedArgs[0]
		targetBackupID := *backupIDPtr
		
		restoreDir := "restored_files"
		if len(parsedArgs) >= 2 {
			restoreDir = parsedArgs[1]
		}
		if err := os.MkdirAll(restoreDir, 0755); err != nil {
			log.Fatalf("Failed to create restore directory: %v", err)
		}

		restorer := client.NewRestorer(database, rpcClient, key, restoreDir, verbose)
		
		ctx := context.Background()
		absTarget, err := filepath.Abs(target)
		if err != nil {
			log.Fatalf("Failed to resolve absolute path for %s: %v", target, err)
		}
		
		err = restorer.Restore(ctx, absTarget, targetBackupID)
		if err != nil {
			log.Printf("Restore failed for %s: %v", target, err)
		}
		
		database.Close()
		return
	}

	if isHistoryCmd {
		if len(args) < 2 {
			log.Fatalf("Usage: client history <file>")
		}
		
		restorer := client.NewRestorer(database, rpcClient, key, "", verbose)
		
		ctx := context.Background()
		for _, target := range args[1:] {
			absTarget, err := filepath.Abs(target)
			if err != nil {
				log.Printf("Failed to resolve absolute path for %s: %v", target, err)
				continue
			}
			err = restorer.PrintHistory(ctx, absTarget)
			if err != nil {
				log.Printf("History lookup failed for %s: %v", target, err)
			}
		}
		
		database.Close()
		return
	}

	if isPruneCmd {
		ctx := context.Background()
		runPrune(ctx, database, rpcClient)
		database.Close()
		return
	}

	if isBackupCmd {
		// Setup upload pipeline
		// Set up channels for the pipeline
		// 1. Crawler -> CryptoPool (Files to be encrypted/hashed or marked deleted)
		jobChan := make(chan client.FileJob, 1000)
		
		// 2. CryptoPool -> Uploader (Encrypted files ready for upload)
		maxMemMB := cfg.Pipeline.MaxPipelineMemMB
		if maxMemMB <= 0 {
			maxMemMB = 400 // Default to 400MB
		}
		blockSize := cfg.Crypto.BlockSizeBytes
		if blockSize <= 0 {
			blockSize = 4 * 1024 * 1024 // Default 4MB
		}
		uploadQueueSize := (maxMemMB * 1024 * 1024) / blockSize
		if uploadQueueSize <= 0 {
			uploadQueueSize = 1
		}
		uploadChan := make(chan client.UploadJob, uploadQueueSize)
		
		// 3. DBWriter channel
		dbJobChan := make(chan db.DBJob, 1000)
		db.StartDBWriter(database, dbJobChan)

		// Create a new backup record
		resChan := make(chan db.DBResult)
		dbJobChan <- db.DBJob{
			Query:      "INSERT INTO backups (start_time, status) VALUES (CURRENT_TIMESTAMP, 'running')",
			ResultChan: resChan,
		}
		res := <-resChan
		if res.Err != nil {
			log.Fatalf("Failed to create backup record: %v", res.Err)
		}
		backupID := res.ID

		// Initialize pipeline components
		var backupPaths []string
		for _, s := range backupSources {
			backupPaths = append(backupPaths, s.Path)
		}
		scanThreads := cfg.Pipeline.ScanThreads
		if scanThreads <= 0 {
			scanThreads = 4
		}
		crawler := client.NewCrawler(database, dbJobChan, backupPaths, jobChan, scanThreads, verbose)
		for _, s := range backupSources {
			if len(s.Excludes) > 0 {
				crawler.SetExcludes(s.Path, s.Excludes)
			}
		}
		
		archiveChan := make(chan client.FileArchive, 1000)

		stats := client.NewBackupStats()

		cryptoThreads := cfg.Pipeline.CryptoThreads
		if cryptoThreads <= 0 {
			cryptoThreads = 4
		}
		cryptoPool := client.NewCryptoPool(key, cryptoThreads, uploadChan, archiveChan, verbose, stats)

		batchSize := cfg.Pipeline.BatchUploadSize
		if batchSize <= 0 {
			batchSize = 100 // Default
		}
		
		uploaderThreads := cfg.Pipeline.UploadThreads
		if uploaderThreads <= 0 {
			uploaderThreads = 2
		}
		
		uploader := client.NewUploader(dbJobChan, uploadChan, rpcClient, uploaderThreads, batchSize, *fakeUpload, verbose, backupID, stats)
		stateManager := client.NewStateManager(database, dbJobChan, archiveChan, verbose)

		if verbose {
			log.Println("Pipeline components initialized. Starting threads...")
		}

		go stateManager.Start()

		// Start the Uploader (runs in background, consumes from uploadChan)
		uploader.Start()

		// Start the CryptoPool (runs in background, consumes from jobChan, produces to uploadChan)
		cryptoPool.Start(jobChan)

		// Start the Crawler (runs in foreground, produces to jobChan)
		// This will block until the crawler finishes walking all directories
		crawler.Start(backupID)

		// Wait for CryptoPool to finish processing all jobs
		cryptoPool.Wait()

		if verbose {
			log.Println("Crypto workers finished. Closing upload channel...")
		}

		// Close uploadChan to signal Uploader that no more uploads are coming
		close(uploadChan)
		close(archiveChan)

		// Wait for Uploader to finish uploading all files
		uploader.Wait()
		
		stateManager.Wait()

		// Print performance summary
		stats.PrintSummary()
		
		if verbose {
			log.Println("Backup process completed successfully.")
		} else {
			fmt.Println("Backup completed.")
		}

		// Close the database so we can safely read it for upload
		database.Close()

		if !*fakeUpload {
			// Backup both the SQLite database and the configuration file as "Special" blobs
			metadataFiles := []string{actualDBPath, *configPath}
			err = uploadMetadata(context.Background(), metadataFiles, key, rpcClient, verbose)
			if err != nil {
				log.Printf("Warning: Failed to upload metadata backup: %v", err)
			}
		}

		return // Exit early since we already closed the DB
	}

	database.Close()
}

func uploadMetadata(ctx context.Context, filePaths []string, key []byte, rpcClient client.RPCClient, verbose bool) error {
	var metaToOffer []rpc.BlobMeta
	var blobsToUpload []rpc.LocalBlobData

	for _, path := range filePaths {
		if verbose {
			log.Printf("Encrypting metadata: %s...", filepath.Base(path))
		}

		data, err := os.ReadFile(path)
		if err != nil {
			log.Printf("Warning: failed to read %s: %v", path, err)
			continue
		}

		ciphertext, err := crypto.Encrypt(key, data)
		if err != nil {
			return fmt.Errorf("encrypt %s: %w", path, err)
		}

		encHash := crypto.Hash(ciphertext)
		encHashHex := hex.EncodeToString(encHash)

		metaToOffer = append(metaToOffer, rpc.BlobMeta{
			Hash:    encHashHex,
			Size:    int64(len(ciphertext)),
			Special: true,
		})
		
		blobsToUpload = append(blobsToUpload, rpc.LocalBlobData{
			Hash:      encHashHex,
			Data:      ciphertext,
			IsSpecial: true,
		})
	}

	if len(metaToOffer) == 0 {
		return nil
	}

	// 1. Offer all metadata blobs
	needed, err := rpcClient.OfferBlobs(ctx, metaToOffer)
	if err != nil {
		return fmt.Errorf("offer metadata: %w", err)
	}

	// 2. Filter blobs to only those the server needs
	if len(needed) > 0 {
		var finalUpload []rpc.LocalBlobData
		for _, idx := range needed {
			finalUpload = append(finalUpload, blobsToUpload[idx])
		}

		err = rpcClient.UploadBlobs(ctx, finalUpload)
		if err != nil {
			return fmt.Errorf("upload metadata: %w", err)
		}
		if verbose {
			log.Printf("Successfully uploaded %d metadata files.", len(finalUpload))
		}
	} else if verbose {
		log.Println("Metadata already up to date on server.")
	}

	return nil
}

func runPrune(ctx context.Context, database *sql.DB, rpcClient client.RPCClient) {
	log.Println("Starting garbage collection (prune)...")
	
	// 1. Ask server for all hashes it holds for us
	log.Println("Fetching blob list from server...")
	serverHashes, err := rpcClient.ListAllBlobs(ctx)
	if err != nil {
		log.Fatalf("Failed to fetch server blobs: %v", err)
	}
	
	// 2. Query our local database for active hashes across all versions
	log.Println("Querying local database for active chunks...")
	rows, err := database.QueryContext(ctx, `
		SELECT DISTINCT hash_encrypted FROM file_blobs
	`)
	if err != nil {
		log.Fatalf("Failed to query active local blobs: %v", err)
	}
	
	activeHashes := make(map[string]bool)
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			log.Fatalf("Failed to scan hash: %v", err)
		}
		activeHashes[hash] = true
	}
	rows.Close()

	// 3. Find orphans
	var orphanHashes []string
	for _, serverHash := range serverHashes {
		if !activeHashes[serverHash] {
			orphanHashes = append(orphanHashes, serverHash)
		}
	}

	if len(orphanHashes) == 0 {
		log.Println("Server is perfectly synchronized. No orphaned blobs to prune.")
		return
	}

	log.Printf("Found %d orphaned blobs on the server. Sending delete command...", len(orphanHashes))
	
	// 4. Send delete command
	err = rpcClient.DeleteBlobs(ctx, orphanHashes)
	if err != nil {
		log.Fatalf("Failed to delete orphaned blobs: %v", err)
	}

	log.Println("Prune complete. Orphaned blobs marked for deletion on server.")
}
