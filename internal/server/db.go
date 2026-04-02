package server

import (
	"database/sql"
	"fmt"
	"log"

	_ "modernc.org/sqlite"
)

// InitDB initializes the server-side SQLite database.
func InitDB(dbPath string) (*sql.DB, error) {
	// Enable WAL mode and busy timeout for better concurrency
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Performance optimization for WAL mode
	_, _ = db.Exec("PRAGMA synchronous = NORMAL")

	// Create tables if they don't exist
	// We track blobs to avoid duplicate uploads (deduplication).
	query := `
	CREATE TABLE IF NOT EXISTS shards (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		status TEXT NOT NULL DEFAULT 'open', -- 'open' or 'sealed'
		size INTEGER NOT NULL DEFAULT 0,
		mirrored BOOLEAN NOT NULL DEFAULT 0,
		hash TEXT NOT NULL DEFAULT '',
		sequence INTEGER NOT NULL DEFAULT 0,
		total_pieces INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS blobs (
		hash TEXT PRIMARY KEY,
		size INTEGER NOT NULL,
		special BOOLEAN NOT NULL DEFAULT 0,
		ref_count INTEGER NOT NULL DEFAULT 1,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		deleted_at DATETIME
	);

	CREATE TABLE IF NOT EXISTS blob_locations (
		blob_hash TEXT NOT NULL,
		shard_id INTEGER NOT NULL,
		offset INTEGER NOT NULL,
		length INTEGER NOT NULL,
		sequence INTEGER NOT NULL,
		FOREIGN KEY(blob_hash) REFERENCES blobs(hash),
		FOREIGN KEY(shard_id) REFERENCES shards(id)
	);

	CREATE INDEX IF NOT EXISTS idx_blob_locations_hash ON blob_locations(blob_hash);

	CREATE TABLE IF NOT EXISTS peers (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		ip_address TEXT NOT NULL,
		public_key TEXT UNIQUE NOT NULL,
		status TEXT NOT NULL DEFAULT 'untrusted',
		first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
		last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
		max_storage_size INTEGER NOT NULL DEFAULT 0,
		current_storage_size INTEGER NOT NULL DEFAULT 0,
		outbound_storage_size INTEGER NOT NULL DEFAULT 0,
		contact_info TEXT NOT NULL DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS clients (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		public_key TEXT UNIQUE NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		max_storage_size INTEGER NOT NULL DEFAULT 0, -- In bytes
		current_storage_size INTEGER NOT NULL DEFAULT 0,
		last_seen DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS peer_blobs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		peer_id INTEGER NOT NULL,
		checksum TEXT NOT NULL,
		datestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(peer_id) REFERENCES peers(id),
		UNIQUE(peer_id, checksum)
	);

	CREATE TABLE IF NOT EXISTS hosted_shards (
		hash TEXT PRIMARY KEY,
		size INTEGER NOT NULL,
		peer_id INTEGER NOT NULL DEFAULT 0,
		is_special INTEGER NOT NULL DEFAULT 0,
		piece_index INTEGER NOT NULL DEFAULT 0,
		parent_shard_hash TEXT NOT NULL DEFAULT '',
		sequence INTEGER NOT NULL DEFAULT 0,
		total_pieces INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_hosted_shards_special ON hosted_shards(peer_id, is_special);

	CREATE TABLE IF NOT EXISTS outbound_pieces (
		shard_id INTEGER NOT NULL,
		piece_index INTEGER NOT NULL,
		peer_id INTEGER NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(shard_id) REFERENCES shards(id),
		FOREIGN KEY(peer_id) REFERENCES peers(id),
		UNIQUE(shard_id, piece_index, peer_id)
	);

	CREATE TABLE IF NOT EXISTS piece_challenges (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		shard_id INTEGER NOT NULL,
		piece_index INTEGER NOT NULL,
		peer_id INTEGER NOT NULL,
		piece_hash TEXT NOT NULL,
		offset INTEGER NOT NULL,
		expected_data BLOB NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(shard_id) REFERENCES shards(id),
		FOREIGN KEY(peer_id) REFERENCES peers(id)
	);

	CREATE TABLE IF NOT EXISTS challenge_results (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		peer_id INTEGER NOT NULL,
		shard_id INTEGER NOT NULL,
		piece_index INTEGER NOT NULL,
		status TEXT NOT NULL,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(peer_id) REFERENCES peers(id),
		FOREIGN KEY(shard_id) REFERENCES shards(id)
	);
	`

	if _, err := db.Exec(query); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	// Ensure columns exist for legacy databases.
	_, _ = db.Exec("ALTER TABLE peers ADD COLUMN status TEXT NOT NULL DEFAULT 'untrusted'")
	_, _ = db.Exec("ALTER TABLE peers ADD COLUMN outbound_storage_size INTEGER NOT NULL DEFAULT 0")
	_, _ = db.Exec("ALTER TABLE peers ADD COLUMN first_seen DATETIME DEFAULT CURRENT_TIMESTAMP")
	_, _ = db.Exec("ALTER TABLE piece_challenges ADD COLUMN piece_hash TEXT NOT NULL DEFAULT ''")
	_, _ = db.Exec("ALTER TABLE peers ADD COLUMN contact_info TEXT NOT NULL DEFAULT ''")

	log.Println("Server database initialized successfully")
	return db, nil
}
