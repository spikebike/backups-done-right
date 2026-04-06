package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type BackupSource struct {
	Path     string   `yaml:"path"`
	Excludes []string `yaml:"excludes"`
}

// ClientConfig represents the configuration for the trusted client node.
type ClientConfig struct {
	Mnemonic string `yaml:"mnemonic"` // 24-word recovery phrase
	Server   struct {
		Address           string `yaml:"address"`
		ExpectedServerKey string `yaml:"expected_server_key"` // Hex string of the server's public key
	} `yaml:"server"`

	Crypto struct {
		Password          string `yaml:"password"`
		BlockSizeBytes    int    `yaml:"block_size_bytes"`
		EnableCompression *bool  `yaml:"enable_compression"` // Defaults to true if omitted
	} `yaml:"crypto"`

	Pipeline struct {
		ScanThreads      int `yaml:"scan_threads"`
		CryptoThreads    int `yaml:"crypto_threads"`
		UploadThreads    int `yaml:"upload_threads"`
		MaxPipelineMemMB int `yaml:"max_pipeline_mem_mb"`
		BatchUploadSize  int `yaml:"batch_upload_size"`
	} `yaml:"pipeline"`

	Storage struct {
		SQLitePath string `yaml:"sqlite_path"`
		SpoolDir   string `yaml:"spool_dir"`
		UploadDir  string `yaml:"upload_dir"`
	} `yaml:"storage"`

	BackupDirectories []BackupSource `yaml:"backup_directories"`
}

// PeerConfig represents a remote peer in the server configuration.
type PeerConfig struct {
	Name           string `yaml:"name"`
	Address        string `yaml:"address"`
	Port           int    `yaml:"port"`
	TLSPublicKey   string `yaml:"tls_public_key"`
	StorageLimitGB int    `yaml:"storage_limit_gb"`
}

// ServerConfig represents the configuration for the zero-knowledge server node.
type ServerConfig struct {
	Mnemonic       string `yaml:"mnemonic"`         // 24-word recovery phrase
	AdminPublicKey string `yaml:"admin_public_key"` // Authorized client for management
	ContactInfo    string `yaml:"contact_info"`     // Optional contact info (email, social, etc.)
	Network        struct {
		ListenAddress        string `yaml:"listen_address"`
		EnableUPnP           bool   `yaml:"enable_upnp"`
		StandaloneMode       bool   `yaml:"standalone_mode"` // If true, no peers, no erasure coding, no challenges
		MaxUploadKBPS        int    `yaml:"max_upload_kbps"`
		MaxDownloadKBPS      int    `yaml:"max_download_kbps"`
		MaxConcurrentStreams int    `yaml:"max_concurrent_streams"`
	} `yaml:"network"`

	ErasureCoding struct {
		DataShardsN        int `yaml:"data_shards_n"`
		ParityShardsK      int `yaml:"parity_shards_k"`
		TargetPieceSizeMB  int `yaml:"target_piece_size_mb"`
		ChallengesPerPiece int `yaml:"challenges_per_piece"`
	} `yaml:"erasure_coding"`

	Storage struct {
		SQLitePath                 string  `yaml:"sqlite_path"`
		SpoolDir                   string  `yaml:"spool_dir"`
		QueueDir                   string  `yaml:"queue_dir"`
		BlobStoreDir               string  `yaml:"blob_store_dir"`
		MasterPassword             string  `yaml:"master_password"`              // Used to encrypt server's own database backups
		KeepLocalCopy              bool    `yaml:"keep_local_copy"`              // If false, deletes local 256MB shards after P2P sync
		KeepDeletedMinutes         int     `yaml:"keep_deleted_minutes"`         // Minutes to keep blobs after client deletes them
		KeepMetadataMinutes        int     `yaml:"keep_metadata_minutes"`        // Minutes to keep server rescue bundles after update
		WasteThreshold             float64 `yaml:"waste_threshold"`              // Percentage (0.0 to 1.0) of deleted bytes to trigger GC
		MaxStorageGB               int     `yaml:"max_storage_gb"`               // Maximum total disk space to use for all shards
		GCIntervalMinutes          int     `yaml:"gc_interval_minutes"`          // How often to run the GC worker
		SelfBackupIntervalMinutes  int     `yaml:"self_backup_interval_minutes"` // How often to backup the server's own database
		PeerEvictionHours          int     `yaml:"peer_eviction_hours"`          // Hours before a peer is considered dead and its data migrated
		UntrustedPeerUploadLimitMB int     `yaml:"untrusted_peer_upload_limit_mb"`
		BasePieceBuffer            int     `yaml:"base_piece_buffer"`
	} `yaml:"storage"`

	Discovery struct {
		Enabled bool `yaml:"enabled"`
	} `yaml:"discovery"`

	AllowedClients []string     `yaml:"allowed_clients"`
	Peers          []PeerConfig `yaml:"peers"`
}

// LoadClientConfig reads and parses the client YAML configuration file.
func LoadClientConfig(path string) (*ClientConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read client config file: %w", err)
	}

	var cfg ClientConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal client config: %w", err)
	}

	return &cfg, nil
}

// LoadServerConfig reads and parses the server YAML configuration file.
func LoadServerConfig(path string) (*ServerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read server config file: %w", err)
	}

	var cfg ServerConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal server config: %w", err)
	}

	return &cfg, nil
}
