package client

import (
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"strings"
	"p2p-backup/internal/db"
)

// FileJob represents a file that needs to be processed (either backed up or marked deleted).
type FileJob struct {
	FileID   int64
	DirID    int64
	BackupID int64
	Path     string
}

// Crawler is responsible for walking the filesystem and finding files to backup.
type Crawler struct {
	DB              *sql.DB
	DBJobChan       chan<- db.DBJob
	BackupDirs      []string
	Excludes        map[string][]string // rootDir -> patterns
	JobChan         chan<- FileJob
	CurrentBackupID int64
	Verbose         bool
}

// NewCrawler creates a new Crawler instance.
func NewCrawler(db *sql.DB, dbJobChan chan<- db.DBJob, backupDirs []string, jobChan chan<- FileJob, verbose bool) *Crawler {
	return &Crawler{
		DB:         db,
		DBJobChan:  dbJobChan,
		BackupDirs: backupDirs,
		Excludes:   make(map[string][]string),
		JobChan:    jobChan,
		Verbose:    verbose,
	}
}

// SetExcludes sets the exclusion patterns for a specific root directory.
func (c *Crawler) SetExcludes(rootDir string, patterns []string) {
	c.Excludes[rootDir] = patterns
}

// isExcluded checks if a path matches any exclusion patterns for its root.
func (c *Crawler) isExcluded(path string) bool {
	for root, patterns := range c.Excludes {
		if strings.HasPrefix(path, root) {
			for _, p := range patterns {
				// Simple prefix or exact match for now
				if path == p || strings.HasPrefix(path, p+string(os.PathSeparator)) {
					return true
				}
			}
		}
	}
	return false
}

// Start begins the crawling process across all configured directories.
func (c *Crawler) Start(backupID int64) {
	if c.Verbose {
		log.Println("Crawler thread 1 of 1 launched")
	}

	c.CurrentBackupID = backupID
	log.Printf("Started backup job #%d", c.CurrentBackupID)

	for _, dir := range c.BackupDirs {
		if c.isExcluded(dir) {
			if c.Verbose {
				log.Printf("Skipping excluded root directory: %s", dir)
			}
			continue
		}
		log.Printf("Syncing directory: %s", dir)
		c.syncDirectory(dir)
	}
	
	// Mark backup complete
	c.DBJobChan <- db.DBJob{
		Query:      "UPDATE backups SET end_time = CURRENT_TIMESTAMP, status = 'complete' WHERE id = ?",
		Args:       []interface{}{c.CurrentBackupID},
		ResultChan: make(chan db.DBResult, 1),
	}
	
	// Close the channel to signal workers that scanning is complete
	close(c.JobChan)
	log.Printf("Backup job #%d complete. Job channel closed.", c.CurrentBackupID)
}

// syncDirectory compares the filesystem state with the database state for a single directory.
func (c *Crawler) syncDirectory(dirPath string) {
	// 1. Get or create directory ID
	var dirID int64
	
	defer func() {
		if dirID > 0 {
			c.DBJobChan <- db.DBJob{
				Query:      "INSERT INTO directory_backups (backup_id, dir_id, completed_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
				Args:       []interface{}{c.CurrentBackupID, dirID},
				ResultChan: make(chan db.DBResult, 1),
			}
		}
	}()

	resChan := make(chan db.DBResult)
	c.DBJobChan <- db.DBJob{
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
		log.Printf("DB error registering dir %s: %v", dirPath, res.Err)
		return
	}

	// 2. Fetch DB state for this directory
	query := `
		SELECT f.id, f.filename, v.mtime 
		FROM files f 
		JOIN file_versions v ON v.file_id = f.id 
		WHERE f.dir_id = ? AND f.deleted = 0 
		  AND v.id = (SELECT MAX(id) FROM file_versions WHERE file_id = f.id)
	`
	rows, err := c.DB.Query(query, dirID)
	if err != nil {
		log.Printf("DB query error for %s: %v", dirPath, err)
		return
	}
	defer rows.Close()

	type dbFile struct {
		id    int64
		mtime int64
	}
	dbFiles := make(map[string]dbFile) // filename -> dbFile
	for rows.Next() {
		var id int64
		var name string
		var mtime int64
		if err := rows.Scan(&id, &name, &mtime); err == nil {
			dbFiles[name] = dbFile{id, mtime}
		}
	}

	// 3. Read filesystem state
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		log.Printf("Failed to read dir %s: %v", dirPath, err)
		return
	}

	fsFiles := make(map[string]os.DirEntry)
	for _, entry := range entries {
		fullPath := filepath.Join(dirPath, entry.Name())
		if c.isExcluded(fullPath) {
			if c.Verbose {
				log.Printf("Skipping excluded path: %s", fullPath)
			}
			continue
		}

		if entry.IsDir() {
			// Recurse into subdirectory
			c.syncDirectory(fullPath)
		} else {
			fsFiles[entry.Name()] = entry
		}
	}

	// 4. Compare FS vs DB (Find new or modified files)
	for name, entry := range fsFiles {
		info, err := entry.Info()
		if err != nil {
			continue // Skip files we can't stat
		}
		
		fullPath := filepath.Join(dirPath, name)
		dbF, exists := dbFiles[name]
		fsMtime := info.ModTime().Unix()
		
		if !exists || fsMtime > dbF.mtime {
			// File is new or has been modified since last backup
			if c.Verbose {
				log.Printf("Crawler sending to JobChan: %s", fullPath)
			}
			c.JobChan <- FileJob{Path: fullPath, DirID: dirID, BackupID: c.CurrentBackupID}
		}
	}

	// 5. Compare DB vs FS (Find deleted files)
	for name, dbF := range dbFiles {
		if _, exists := fsFiles[name]; !exists {
			fullPath := filepath.Join(dirPath, name)
			
			// Mark as deleted in local DB
			c.DBJobChan <- db.DBJob{
				Query:      "UPDATE files SET deleted = 1 WHERE id = ?",
				Args:       []interface{}{dbF.id},
				ResultChan: make(chan db.DBResult, 1),
			}
			if c.Verbose {
				log.Printf("Marked file as deleted in local DB: %s", fullPath)
			}
		}
	}
}
