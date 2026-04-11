package db

import (
	"database/sql"
	"fmt"

	// Using the pure-Go SQLite driver (no CGO required)
	_ "modernc.org/sqlite"
)

const clientSchema = `
-- Enable foreign key constraints
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

-- Use Write-Ahead Logging for better concurrent read/write performance
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS backups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time DATETIME NOT NULL,
    end_time DATETIME,
    status TEXT NOT NULL,
    offered_files INTEGER DEFAULT 0,
    uploaded_files INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS directory_backups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    backup_id INTEGER NOT NULL,
    dir_id INTEGER NOT NULL,
    completed_at DATETIME NOT NULL,
    FOREIGN KEY(backup_id) REFERENCES backups(id),
    FOREIGN KEY(dir_id) REFERENCES directories(id)
);

CREATE TABLE IF NOT EXISTS directories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    full_path TEXT UNIQUE NOT NULL,
    first_seen DATETIME NOT NULL,
    last_seen DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dir_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
    deleted BOOLEAN DEFAULT 0,
    FOREIGN KEY(dir_id) REFERENCES directories(id),
    UNIQUE(dir_id, filename)
);

CREATE TABLE IF NOT EXISTS file_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL,
    backup_id INTEGER NOT NULL,
    mtime INTEGER NOT NULL,
    size INTEGER NOT NULL,
    hash_plain TEXT NOT NULL,
    uid INTEGER NOT NULL DEFAULT 0,
    gid INTEGER NOT NULL DEFAULT 0,
    mode INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,
    FOREIGN KEY(backup_id) REFERENCES backups(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS local_blobs (
    hash_plain TEXT PRIMARY KEY,
    hash_encrypted TEXT UNIQUE NOT NULL,
    size INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS file_blobs (
    version_id INTEGER NOT NULL,
    hash_encrypted TEXT NOT NULL,
    size INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    FOREIGN KEY(version_id) REFERENCES file_versions(id) ON DELETE CASCADE,
    PRIMARY KEY(version_id, sequence)
);


-- Indexes for fast lookups during directory crawls and syncs
CREATE INDEX IF NOT EXISTS idx_directories_path ON directories(full_path);
CREATE INDEX IF NOT EXISTS idx_file_versions_file_id ON file_versions(file_id);
CREATE INDEX IF NOT EXISTS idx_file_blobs_hash_encrypted ON file_blobs(hash_encrypted);
CREATE INDEX IF NOT EXISTS idx_local_blobs_hash_encrypted ON local_blobs(hash_encrypted);
`

// InitClientDB opens the SQLite database, configures pragmas, and initializes the schema.
func InitClientDB(dbPath string) (*sql.DB, error) {
	// Add pragmas to DSN so they apply to ALL connection pool connections, not just the first one.
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Verify the connection is valid
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Execute the schema creation and pragma setup
	if _, err := db.Exec(clientSchema); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Migrations for existing databases
	_, _ = db.Exec("ALTER TABLE file_versions ADD COLUMN uid INTEGER NOT NULL DEFAULT 0")
	_, _ = db.Exec("ALTER TABLE file_versions ADD COLUMN gid INTEGER NOT NULL DEFAULT 0")
	_, _ = db.Exec("ALTER TABLE file_versions ADD COLUMN mode INTEGER NOT NULL DEFAULT 0")

	return db, nil
}
