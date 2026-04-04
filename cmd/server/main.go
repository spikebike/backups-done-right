package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	"p2p-backup/internal/config"
	"p2p-backup/internal/crypto"
	internalrpc "p2p-backup/internal/rpc"
	"p2p-backup/internal/server"

	capnp "capnproto.org/go/capnp/v3"
	capnprpc "capnproto.org/go/capnp/v3/rpc"
	"capnproto.org/go/capnp/v3/rpc/transport"
	capnpserver "capnproto.org/go/capnp/v3/server"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

type verbosity int

func (v *verbosity) String() string {
	return fmt.Sprintf("%d", *v)
}

func (v *verbosity) Set(s string) error {
	if s == "true" || s == "" {
		*v++
	} else {
		// handle -v=1 etc
		fmt.Sscanf(s, "%d", v)
	}
	return nil
}

func (v *verbosity) IsBoolFlag() bool {
	return true
}

func main() {
	configPath := flag.String("config", "server.yaml", "Path to the server configuration file")
	rescue := flag.Bool("rescue", false, "Attempt to recover the server database from peers")
	var v verbosity
	flag.Var(&v, "v", "Enable verbose logging (use -v -v for extra verbosity)")
	flag.Parse()

	// Handle subcommands
	if len(flag.Args()) > 0 {
		command := flag.Arg(0)
		switch command {
		case "keygen", "genkey":
			m, err := crypto.GenerateMnemonic()
			if err != nil {
				log.Fatalf("Failed to generate mnemonic: %v", err)
			}
			id, err := crypto.DeriveIdentity(m, true)
			if err != nil {
				log.Fatalf("Failed to derive identity: %v", err)
			}

			// Extract Public Key Hex for sharing
			leaf, err := x509.ParseCertificate(id.TLSCert.Certificate[0])
			if err != nil {
				log.Fatalf("Failed to parse generated cert: %v", err)
			}
			pubKeyBytes, _ := x509.MarshalPKIXPublicKey(leaf.PublicKey)

			fmt.Println("=== NEW SERVER IDENTITY GENERATED ===")
			fmt.Printf("Mnemonic: %s\n\n", m)
			fmt.Printf("Public Key (Hex): %x\n", pubKeyBytes)
			fmt.Println("======================================")
			fmt.Println("IMPORTANT: Write down the mnemonic. It is the ONLY way to recover your server.")
			return

		case "identity":
			cfg, err := config.LoadServerConfig(*configPath)
			if err != nil {
				log.Fatalf("Failed to load config: %v", err)
			}
			if cfg.Mnemonic == "" {
				log.Fatal("Identity command requires a mnemonic in the config file")
			}
			id, err := crypto.DeriveIdentity(cfg.Mnemonic, true)
			if err != nil {
				log.Fatalf("Failed to derive identity: %v", err)
			}
			leaf, _ := x509.ParseCertificate(id.TLSCert.Certificate[0])
			pubKeyBytes, _ := x509.MarshalPKIXPublicKey(leaf.PublicKey)
			fmt.Printf("Server Public Key (Hex): %x\n", pubKeyBytes)
			return

		case "help":
			fmt.Println("Usage: server [options] [command]")
			fmt.Println("\nCommands:")
			fmt.Println("  keygen      Generate a new 24-word recovery mnemonic and public key")
			fmt.Println("  identity    Show public key derived from mnemonic in config")
			fmt.Println("  help        Show this help message")
			fmt.Println("\nOptions:")
			flag.PrintDefaults()
			return
		}
	}

	verbose := v > 0
	extraVerbose := v > 1

	cfg, err := config.LoadServerConfig(*configPath)
	if err != nil {
		log.Printf("Warning: Failed to load config file %s: %v. Using defaults.", *configPath, err)
		cfg = &config.ServerConfig{}
		cfg.Storage.SQLitePath = "server_state.db"
		cfg.Storage.BlobStoreDir = "server_blobs"
	}

	// 1. Identity Derivation (Mnemonic Mandatory)
	if cfg.Mnemonic == "" {
		log.Fatal("Mnemonic is mandatory in server.yaml. Run 'server keygen' to generate one.")
	}

	if verbose {
		log.Println("Deriving identity and master key from mnemonic...")
	}
	id, err := crypto.DeriveIdentity(cfg.Mnemonic, true)
	if err != nil {
		log.Fatalf("Failed to derive identity from mnemonic: %v", err)
	}
	masterKey := id.MasterKey
	serverCert := &id.TLSCert

	if *rescue {
		runRescue(cfg, masterKey, *serverCert, verbose)
		return
	}

	queueDir := cfg.Storage.QueueDir
	if queueDir == "" {
		queueDir = "server_queue"
	}

	// Ensure directories exist
	dirs := []string{
		filepath.Dir(cfg.Storage.SQLitePath),
		cfg.Storage.BlobStoreDir,
		queueDir,
	}
	for _, dir := range dirs {
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				log.Fatalf("Failed to create directory %s: %v", dir, err)
			}
		}
	}

	// Initialize Database
	db, err := server.InitDB(cfg.Storage.SQLitePath)
	if err != nil {
		log.Fatalf("Failed to initialize server database: %v", err)
	}
	defer db.Close()

	// Initialize Engine
	dataShards := cfg.ErasureCoding.DataShardsN
	if dataShards <= 0 {
		dataShards = 10
	}
	parityShards := cfg.ErasureCoding.ParityShardsK
	if parityShards <= 0 {
		parityShards = 4
	}

	pieceSizeMB := cfg.ErasureCoding.TargetPieceSizeMB
	if pieceSizeMB <= 0 {
		pieceSizeMB = 256
	}

	pieceSize := int64(pieceSizeMB) * 1024 * 1024 // e.g., 256MB per distributed piece
	shardSize := int64(dataShards) * pieceSize

	challengesPerPiece := cfg.ErasureCoding.ChallengesPerPiece
	if challengesPerPiece <= 0 {
		challengesPerPiece = 8
	}

	keepDeletedMinutes := cfg.Storage.KeepDeletedMinutes
	if keepDeletedMinutes <= 0 {
		keepDeletedMinutes = 60 * 24 * 30 // 30 days default
	}

	keepMetadataMinutes := cfg.Storage.KeepMetadataMinutes
	if keepMetadataMinutes <= 0 {
		keepMetadataMinutes = 60 * 24 * 7 // 7 days default
	}

	wasteThreshold := cfg.Storage.WasteThreshold
	if wasteThreshold <= 0 {
		wasteThreshold = 0.5
	}

	gcIntervalMinutes := cfg.Storage.GCIntervalMinutes
	if gcIntervalMinutes <= 0 {
		gcIntervalMinutes = 720 // 12 hours default
	}

	// Convert Ed25519 identity to Libp2p Key
	_, err = crypto.Libp2pKeyFromEd25519(id.MasterKey) // Assuming MasterKey or a specific field holds the ed25519.PrivateKey. Actually DeriveIdentity returns an Identity struct. Let's look at crypto.go in a second. Wait, DerivIdentity returns id which has MasterKey and TLSCert. The TLS cert has the private key inside it.
	// We will get the raw ed25519.PrivateKey from the parsed crypto.go. Wait, better to construct it from the mnemonic in main.go or crypto.go.
	// I'll leave the initialization to use standard libp2p.New().

	// Parse listen port
	listenAddr := cfg.Network.ListenAddress
	if listenAddr == "" {
		listenAddr = "0.0.0.0:8080"
	}

	hostAddr, portStr, err := net.SplitHostPort(listenAddr)
	if err != nil {
		log.Fatalf("Invalid listen address %s: %v", listenAddr, err)
	}

	quicAddr := fmt.Sprintf("/ip4/%s/udp/%s/quic-v1", hostAddr, portStr)
	tcpAddr := fmt.Sprintf("/ip4/%s/tcp/%s", hostAddr, portStr)

	// Since we need to pass a private key to libp2p, let's extract it from the id.TLSCert
	edPrivKey := id.TLSCert.PrivateKey.(ed25519.PrivateKey)
	p2pPrivKey, err := crypto.Libp2pKeyFromEd25519(edPrivKey)
	if err != nil {
		log.Fatalf("Failed to convert identity to libp2p key: %v", err)
	}

	rm, rmErr := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits))
	if rmErr != nil {
		log.Fatalf("Failed to initialize resource manager: %v", rmErr)
	}

	p2pOpts := []libp2p.Option{
		libp2p.ListenAddrStrings(quicAddr, tcpAddr),
		libp2p.Identity(p2pPrivKey),
		// NAT Traversal
		libp2p.EnableRelay(), // Enable acting as a limited v2 relay for others (Circuit Relay v2)
		libp2p.EnableAutoRelayWithStaticRelays([]peer.AddrInfo{}), // DCUtR: Automatically use relays to coordinate hole punches
		libp2p.EnableHolePunching(),                               // Execute UDP hole punching
		libp2p.ResourceManager(rm),
	}

	if cfg.Network.EnableUPnP {
		p2pOpts = append(p2pOpts, libp2p.NATPortMap())
	}

	p2pHost, err := libp2p.New(p2pOpts...)
	if err != nil {
		log.Fatalf("Failed to start libp2p host: %v", err)
	}

	fmt.Printf("Server listening on %s (libp2p QUIC/TCP)...\n", listenAddr)
	fmt.Printf("Peer ID: %s\n", p2pHost.ID().String())
	for _, addr := range p2pHost.Addrs() {
		fmt.Printf("  - %s/p2p/%s\n", addr.String(), p2pHost.ID().String())
	}

	// 2. Identity: Extract our own public key for the RPC handler
	myPubKeyHex, err := crypto.PubKeyHexFromPeerID(p2pHost.ID())
	if err != nil {
		log.Fatalf("Failed to extract own public key: %v", err)
	}

	engine := server.NewEngine(
		db,
		cfg.Storage.SQLitePath,
		cfg.Storage.BlobStoreDir,
		queueDir,
		dataShards,
		parityShards,
		shardSize,
		cfg.Storage.KeepLocalCopy,
		p2pHost,
		listenAddr,
		cfg.Storage.UntrustedPeerUploadLimitMB,
		verbose,
		extraVerbose,
		challengesPerPiece,
		keepDeletedMinutes,
		keepMetadataMinutes,
		wasteThreshold,
		gcIntervalMinutes,
		cfg.Storage.SelfBackupIntervalMinutes,
		cfg.Storage.PeerEvictionHours,
		cfg.Storage.BasePieceBuffer,
		cfg.Storage.MaxStorageGB,
		cfg.Network.MaxUploadKBPS,
		cfg.Network.MaxDownloadKBPS,
		id.MasterKey,
		cfg.AdminPublicKey,
		cfg.ContactInfo,
		cfg.Network.MaxConcurrentStreams,
		cfg.Network.StandaloneMode,
		)

	if verbose {
		log.Printf("Server engine initialized with identity: %s...", myPubKeyHex[:16])
		log.Printf("Blob storage at: %s", cfg.Storage.BlobStoreDir)
		log.Printf("GC Config: keep_deleted_minutes=%d, waste_threshold=%.2f, gc_interval_minutes=%d", keepDeletedMinutes, wasteThreshold, gcIntervalMinutes)
		if cfg.Network.StandaloneMode {
			log.Println("Running in STANDALONE MODE — no peers, no erasure coding, no challenges")
		}
	}

	peerHandler := server.NewRPCHandler(engine, myPubKeyHex)
	peerServerClient := internalrpc.PeerNode_ServerToClient(peerHandler)
	engine.LocalPeerNode = peerServerClient

	// Start background workers
	ctx := context.Background()
	if !cfg.Network.StandaloneMode {
		go engine.StartOutboundWorker(ctx)
		go engine.StartChallengeWorker(ctx)
		go engine.StartSelfBackupWorker(ctx)
		go engine.StartRepairWorker(ctx)
	}
	go engine.StartGCWorker(ctx)

	if cfg.Discovery.Enabled {
		go engine.StartDiscoveryWorker(ctx, cfg.Discovery.Mode)
	}

	p2pHost.SetStreamHandler("/bdr/rpc/1.0.0", func(s network.Stream) {
		// Apply bandwidth throttling
		throttledStream := engine.NewThrottledStream(context.Background(), s)
		defer throttledStream.Close()

		peerID := s.Conn().RemotePeer()
		peerAddr := s.Conn().RemoteMultiaddr().String()

		pubKeyHex, err := crypto.PubKeyHexFromPeerID(peerID)
		if err != nil {
			log.Printf("Failed to extract pubkey from peer %s: %v", peerID, err)
			return
		}

		// 1. Identify if it's a known client
		status, quota, current, _ := engine.AuthorizeClient(context.Background(), pubKeyHex)

		// 2. Create combined handler that serves both roles
		// Identification is lazy: peer registration happens on Announce or OfferShards
		combinedHandler := server.NewRPCHandler(engine, pubKeyHex)

		// Combine methods from both interfaces
		methods := internalrpc.BackupServer_Methods(nil, combinedHandler)
		methods = internalrpc.PeerNode_Methods(methods, combinedHandler)

		// Create a single bootstrap client that implements both
		bootstrapClient := capnp.NewClient(capnpserver.New(methods, combinedHandler, nil))

		if verbose {
			log.Printf("Identified Connection: %s... from %s (client_status=%s, quota=%d/%d MB)",
				pubKeyHex[:16], peerAddr, status, current/(1024*1024), quota/(1024*1024))
		}

		if extraVerbose {
			log.Printf("Public Key: %s", pubKeyHex)
		}

		// Create a custom decoder to allow up to 512MB messages
		decoder := capnp.NewDecoder(throttledStream)
		decoder.MaxMessageSize = 512 * 1024 * 1024 // 512MB limit for large erasure chunks
		encoder := capnp.NewEncoder(throttledStream)

		codec := &customCodec{
			decoder: decoder,
			encoder: encoder,
			closer:  throttledStream.Close,
		}

		rpcConn := capnprpc.NewConn(transport.New(codec), &capnprpc.Options{
			BootstrapClient: bootstrapClient,
		})

		defer rpcConn.Close()
		<-rpcConn.Done()
	})

	p2pHost.SetStreamHandler("/bdr/data/1.0.0", func(s network.Stream) {
		engine.HandleDataStream(s)
	})

	// Keep main thread alive
	select {}
}

// customCodec wraps a standard Cap'n Proto encoder/decoder pair to allow overriding MaxMessageSize.
type customCodec struct {
	decoder *capnp.Decoder
	encoder *capnp.Encoder
	closer  func() error
}

func (c *customCodec) Encode(m *capnp.Message) error {
	return c.encoder.Encode(m)
}
func (c *customCodec) Decode() (*capnp.Message, error) {
	msg, err := c.decoder.Decode()
	if err != nil {
		return nil, err
	}
	msg.TraverseLimit = 512 * 1024 * 1024 // 512MB to match MaxMessageSize
	return msg, nil
}
func (c *customCodec) Close() error {
	return c.closer()
}

func runRescue(cfg *config.ServerConfig, masterKey []byte, myCert tls.Certificate, verbose bool) {
	log.Fatal("RESCUE: Disaster recovery not yet ported to out-of-band streaming.")
}
