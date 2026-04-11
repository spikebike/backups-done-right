package client

import (
	"database/sql"
	"log"
	"os"
	"p2p-backup/internal/db"
	"path/filepath"
	"strings"
	"sync"
)

// FileJob represents a file that needs to be processed (either backed up or marked deleted).
type FileJob struct {
	DirPath  string
	FileName string
	BackupID int64
	Deleted  bool
}

// Crawler is responsible for walking the filesystem and finding files to backup.
type Crawler struct {
	DB              *sql.DB
	DBJobChan       chan<- db.DBJob
	BackupDirs      []string
	Excludes        map[string][]string // rootDir -> patterns
	ScanThreads     int
	JobChan         chan<- FileJob
	CurrentBackupID int64
	Verbose         bool
}

// NewCrawler creates a new Crawler instance.
func NewCrawler(db *sql.DB, dbJobChan chan<- db.DBJob, backupDirs []string, jobChan chan<- FileJob, scanThreads int, verbose bool) *Crawler {
	if scanThreads <= 0 {
		scanThreads = 4
	}
	return &Crawler{
		DB:          db,
		DBJobChan:   dbJobChan,
		BackupDirs:  backupDirs,
		Excludes:    make(map[string][]string),
		ScanThreads: scanThreads,
		JobChan:     jobChan,
		Verbose:     verbose,
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
		log.Printf("Crawler launching %d parallel scanner threads", c.ScanThreads)
	}

	c.CurrentBackupID = backupID
	log.Printf("Started backup job #%d", c.CurrentBackupID)

	dirQueue := make(chan string, 10000)
	var wg sync.WaitGroup

	// Spawn scanner workers
	for i := 0; i < c.ScanThreads; i++ {
		go c.crawlerWorker(dirQueue, &wg)
	}

	for _, dir := range c.BackupDirs {
		if c.isExcluded(dir) {
			if c.Verbose {
				log.Printf("Skipping excluded root directory: %s", dir)
			}
			continue
		}
		log.Printf("Syncing directory root: %s", dir)
		wg.Add(1)
		dirQueue <- dir
	}

	wg.Wait()
	close(dirQueue)

	// Close the channel to signal workers that scanning is complete
	close(c.JobChan)
	log.Printf("Crawler backup indexing complete. Job channel closed.")
}

func (c *Crawler) crawlerWorker(dirQueue chan string, wg *sync.WaitGroup) {
	for dirPath := range dirQueue {
		subDirs := c.syncDirectory(dirPath)
		for _, sd := range subDirs {
			wg.Add(1)
			go func(d string) {
				dirQueue <- d
			}(sd)
		}
		wg.Done()
	}
}

// syncDirectory compares the filesystem state with the database state for a single directory.
// Returns a slice of found subdirectories to be enqueued.
func (c *Crawler) syncDirectory(dirPath string) []string {
	var subDirs []string

	// 1. Fetch DB state for this directory by joining on full_path (100% read-only)
	dbFiles, err := getDirectoryState(c.DB, dirPath)
	if err != nil {
		log.Printf("DB query error for %s: %v", dirPath, err)
		return nil
	}

	// 2. Read filesystem state
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		log.Printf("Failed to read dir %s: %v", dirPath, err)
		return nil
	}

	fsFiles := make(map[string]os.DirEntry)
	for _, entry := range entries {
		fullPath := filepath.Join(dirPath, entry.Name())
		if c.isExcluded(fullPath) {
			continue
		}

		if entry.IsDir() {
			subDirs = append(subDirs, fullPath)
		} else {
			fsFiles[entry.Name()] = entry
		}
	}

	// 3. Compare FS vs DB (Find new or modified files)
	for name, entry := range fsFiles {
		info, err := entry.Info()
		if err != nil {
			continue // Skip files we can't stat
		}

		dbMtime, exists := dbFiles[name]
		fsMtime := info.ModTime().Unix()

		if !exists || fsMtime > dbMtime {
			// File is new or has been modified since last backup
			c.JobChan <- FileJob{
				DirPath:  dirPath,
				FileName: name,
				BackupID: c.CurrentBackupID,
				Deleted:  false,
			}
		}
	}

	// 4. Compare DB vs FS (Find deleted files)
	for name := range dbFiles {
		if _, exists := fsFiles[name]; !exists {
			// Mark as deleted in state manager later
			c.JobChan <- FileJob{
				DirPath:  dirPath,
				FileName: name,
				BackupID: c.CurrentBackupID,
				Deleted:  true,
			}
		}
	}

	return subDirs
}
