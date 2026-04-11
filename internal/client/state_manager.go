package client

import (
	"database/sql"
	"log"
	"sync"

	"p2p-backup/internal/db"
)

// StateManager processes finalized FileArchives and commits them to the database.
type StateManager struct {
	DB          *sql.DB
	DBJobChan   chan<- db.DBJob
	ArchiveChan <-chan FileArchive
	Verbose     bool
	wg          sync.WaitGroup

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

	// First fetch (pure fast read)
	dirID, err := fetchDirID(s.DB, dirPath)
	if err == nil {
		s.dirCache[dirPath] = dirID
		return dirID
	}

	// Not found, insert via DBJobChan to sync perfectly
	dirID, err = insertDirID(s.DBJobChan, dirPath)
	if err != nil {
		log.Printf("StateManager error registering dir %s: %v", dirPath, err)
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
		markFileDeleted(s.DBJobChan, dirID, archive.FileName)
		if s.Verbose {
			log.Printf("StateManager marked file as deleted: %s/%s", archive.DirPath, archive.FileName)
		}
		return
	}

	// 1. Insert/Update File
	fileID, err := insertFile(s.DBJobChan, dirID, archive.FileName)
	if err != nil || fileID == 0 {
		log.Printf("StateManager error inserting file %s: %v", archive.FileName, err)
		return
	}

	// 2. Insert Version
	versionID, err := insertFileVersion(s.DBJobChan, fileID, archive.BackupID, archive.Mtime, archive.Size, archive.PlainHash, archive.UID, archive.GID, archive.Mode)
	if err != nil || versionID == 0 {
		log.Printf("StateManager error inserting version: %v", err)
		return
	}

	// 3. Insert Chunks
	for _, chunk := range archive.Chunks {
		// Log local deduplication blob mapping
		insertLocalBlob(s.DBJobChan, chunk.PlainHash, chunk.EncHash, chunk.Size)

		// Link chunk to file
		insertFileBlob(s.DBJobChan, versionID, chunk.EncHash, chunk.Size, chunk.Sequence)
	}

	// 4. Update directory tracking exactly once per backup ID run
	if !s.dirBackupsCache[dirID] {
		markDirectoryBackedUp(s.DBJobChan, archive.BackupID, dirID)
		s.dirBackupsCache[dirID] = true
	}

	if s.Verbose {
		log.Printf("StateManager fully archived %s/%s", archive.DirPath, archive.FileName)
	}
}
