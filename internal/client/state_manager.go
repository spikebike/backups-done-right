package client

import (
	"database/sql"
	"log"
	"sync"

	"p2p-backup/internal/db"
)

// StateManager processes finalized FileArchives and commits them to the database.
type StateManager struct {
	DB              *sql.DB
	DBJobChan       chan<- db.DBJob
	ArchiveChan     <-chan FileArchive
	Verbose         bool
	wg              sync.WaitGroup

	dirCache        map[string]int64 // fullPath -> dir_id
	dirBackupsCache map[int64]bool   // dir_id -> true (already logged this run)
}

// NewStateManager creates a StateManager instance.
func NewStateManager(database *sql.DB, dbJobChan chan<- db.DBJob, archiveChan <-chan FileArchive, verbose bool) *StateManager {
	return &StateManager{
		DB:              database,
		DBJobChan:       dbJobChan,
		ArchiveChan:     archiveChan,
		Verbose:         verbose,
		dirCache:        make(map[string]int64),
		dirBackupsCache: make(map[int64]bool),
	}
}

// Start listens to the archive channel and processes files.
func (s *StateManager) Start() {
	s.wg.Add(1)
	defer s.wg.Done()
	
	for archive := range s.ArchiveChan {
		s.processArchive(archive)
	}
	if s.Verbose {
		log.Println("StateManager completed. Archive channel closed.")
	}
}

// Wait blocks until StateManager completes processing.
func (s *StateManager) Wait() {
	s.wg.Wait()
}

// getDirID securely fetches or creates the dir_id.
func (s *StateManager) getDirID(dirPath string) int64 {
	if dirID, ok := s.dirCache[dirPath]; ok {
		return dirID
	}

	var dirID int64
	// First fetch (pure fast read)
	err := s.DB.QueryRow("SELECT id FROM directories WHERE full_path = ?", dirPath).Scan(&dirID)
	if err == nil {
		s.dirCache[dirPath] = dirID
		return dirID
	}

	// Not found, insert via DBJobChan to sync perfectly
	resChan := make(chan db.DBResult)
	s.DBJobChan <- db.DBJob{
		Query: `
			INSERT INTO directories (full_path, first_seen, last_seen)
			VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT(full_path) DO UPDATE SET last_seen = CURRENT_TIMESTAMP
			RETURNING id;
		`,
		Args:       []interface{}{dirPath},
		ResultChan: resChan,
		Scan: func(row *sql.Row) error {
			return row.Scan(&dirID)
		},
	}
	res := <-resChan
	if res.Err != nil {
		log.Printf("StateManager error registering dir %s: %v", dirPath, res.Err)
		return 0
	}

	s.dirCache[dirPath] = dirID
	return dirID
}

// processArchive handles a completely finished file bundle.
func (s *StateManager) processArchive(archive FileArchive) {
	dirID := s.getDirID(archive.DirPath)
	if dirID == 0 {
		return
	}

	if archive.Deleted {
		s.DBJobChan <- db.DBJob{
			Query: `
				UPDATE files SET deleted = 1 
				WHERE dir_id = ? AND filename = ?
			`,
			Args: []interface{}{dirID, archive.FileName},
			ResultChan: make(chan db.DBResult, 1),
		}
		if s.Verbose {
			log.Printf("StateManager marked file as deleted: %s/%s", archive.DirPath, archive.FileName)
		}
		return
	}

	// 1. Insert/Update File
	resChan := make(chan db.DBResult)
	var fileID int64
	s.DBJobChan <- db.DBJob{
		Query: `
			INSERT INTO files (dir_id, filename, first_seen, last_seen, deleted)
			VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0)
			ON CONFLICT(dir_id, filename) DO UPDATE SET
				last_seen = CURRENT_TIMESTAMP,
				deleted = 0
			RETURNING id
		`,
		Args:       []interface{}{dirID, archive.FileName},
		ResultChan: resChan,
		Scan: func(row *sql.Row) error {
			return row.Scan(&fileID)
		},
	}
	res := <-resChan
	if res.Err != nil || fileID == 0 {
		log.Printf("StateManager error inserting file %s: %v", archive.FileName, res.Err)
		return
	}

	// 2. Insert Version
	var versionID int64
	s.DBJobChan <- db.DBJob{
		Query: `
			INSERT INTO file_versions (file_id, backup_id, mtime, size, hash_plain, uid, gid, mode)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			RETURNING id
		`,
		Args:       []interface{}{fileID, archive.BackupID, archive.Mtime, archive.Size, archive.PlainHash, archive.UID, archive.GID, archive.Mode},
		ResultChan: resChan,
		Scan: func(row *sql.Row) error {
			return row.Scan(&versionID)
		},
	}
	res = <-resChan
	if res.Err != nil || versionID == 0 {
		log.Printf("StateManager error inserting version: %v", res.Err)
		return
	}

	// 3. Insert Chunks
	for _, chunk := range archive.Chunks {
		// Log local deduplication blob mapping
		s.DBJobChan <- db.DBJob{
			Query:      "INSERT OR IGNORE INTO local_blobs (hash_plain, hash_encrypted, size) VALUES (?, ?, ?)",
			Args:       []interface{}{chunk.PlainHash, chunk.EncHash, chunk.Size},
			ResultChan: make(chan db.DBResult, 1), // Send-and-forget
		}

		// Link chunk to file
		s.DBJobChan <- db.DBJob{
			Query:      "INSERT INTO file_blobs (version_id, hash_encrypted, size, sequence) VALUES (?, ?, ?, ?)",
			Args:       []interface{}{versionID, chunk.EncHash, chunk.Size, chunk.Sequence},
			ResultChan: make(chan db.DBResult, 1), // Send-and-forget
		}
	}

	// 4. Update directory tracking exactly once per backup ID run
	if !s.dirBackupsCache[dirID] {
		s.DBJobChan <- db.DBJob{
			Query:      "INSERT INTO directory_backups (backup_id, dir_id, completed_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
			Args:       []interface{}{archive.BackupID, dirID},
			ResultChan: make(chan db.DBResult, 1), // Send-and-forget
		}
		s.dirBackupsCache[dirID] = true
	}

	if s.Verbose {
		log.Printf("StateManager fully archived %s/%s", archive.DirPath, archive.FileName)
	}
}
