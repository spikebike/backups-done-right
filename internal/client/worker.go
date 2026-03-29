package client

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/klauspost/compress/zstd"
	"lukechampine.com/blake3"
	"p2p-backup/internal/crypto"
	"p2p-backup/internal/db"
)

// UploadJob represents a blob that is ready to be uploaded.
type UploadJob struct {
	FileID    int64
	Hash      string // Blob hash
	Size      int64  // Blob size
	IsSpecial bool
}

// CryptoPool manages a pool of workers that encrypt files and update the local database.
type CryptoPool struct {
	DB          *sql.DB
	DBJobChan   chan<- db.DBJob
	Key         []byte
	SpoolDir    string
	UploadDir   string
	NumWorkers  int
	UploadChan  chan<- UploadJob
	Verbose     bool
	wg          sync.WaitGroup
	zstdEncoder *zstd.Encoder
}

// NewCryptoPool initializes a new CryptoPool.
func NewCryptoPool(db *sql.DB, dbJobChan chan<- db.DBJob, key []byte, spoolDir, uploadDir string, numWorkers int, uploadChan chan<- UploadJob, verbose bool) *CryptoPool {
	encoder, _ := zstd.NewWriter(nil)
	return &CryptoPool{
		DB:          db,
		DBJobChan:   dbJobChan,
		Key:         key,
		SpoolDir:    spoolDir,
		UploadDir:   uploadDir,
		NumWorkers:  numWorkers,
		UploadChan:  uploadChan,
		Verbose:     verbose,
		zstdEncoder: encoder,
	}
}

// Start launches the worker goroutines.
func (p *CryptoPool) Start(jobChan <-chan FileJob) {
	// Ensure the directories exist
	if err := os.MkdirAll(p.SpoolDir, 0700); err != nil {
		log.Fatalf("Failed to create spool directory: %v", err)
	}
	if err := os.MkdirAll(p.UploadDir, 0700); err != nil {
		log.Fatalf("Failed to create upload directory: %v", err)
	}

	// Launch workers
	for i := 0; i < p.NumWorkers; i++ {
		p.wg.Add(1)
		if p.Verbose {
			log.Printf("Checksum/Encryption thread %d of %d launched", i+1, p.NumWorkers)
		}
		go p.worker(jobChan, &p.wg, i+1)
	}
}

// Wait blocks until all workers finish processing jobs.
func (p *CryptoPool) Wait() {
	p.wg.Wait()
	log.Println("Crypto pool finished processing all jobs.")
}

func (p *CryptoPool) worker(jobChan <-chan FileJob, wg *sync.WaitGroup, workerID int) {
	defer wg.Done()
	
	for job := range jobChan {
		err := p.processFile(job)
		if err != nil {
			log.Printf("[Worker %d] Error processing %s: %v", workerID, job.Path, err)
		} else {
			if p.Verbose {
				log.Printf("[Worker %d] Successfully processed %s", workerID, job.Path)
			}
		}
	}
}

func (p *CryptoPool) processFile(job FileJob) error {
	// 1. Read file stats (Lstat to avoid following symlinks)
	info, err := os.Lstat(job.Path)
	if err != nil {
		return err
	}

	// Extract UID, GID, and permissions
	var fileUID, fileGID uint32
	var fileMode os.FileMode
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		fileUID = stat.Uid
		fileGID = stat.Gid
	}
	fileMode = info.Mode().Perm()
	
	// 2. Open file for streaming
	inFile, err := os.Open(job.Path)
	if err != nil {
		return err
	}
	defer inFile.Close()

	// 3. Register file in DB first to get FileID
	fileName := filepath.Base(job.Path)

	// We'll compute the full file hash as we read it
	fullPlainHasher := blake3.New(32, nil)

	var fileID int64
	// INSERT or UPDATE and get ID
	resChan := make(chan db.DBResult)
	p.DBJobChan <- db.DBJob{
		Query: `
		INSERT INTO files (dir_id, filename, first_seen, last_seen, deleted)
		VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0)
		ON CONFLICT(dir_id, filename) DO UPDATE SET
			last_seen = CURRENT_TIMESTAMP,
			deleted = 0
		RETURNING id
	`,
		Args:       []interface{}{job.DirID, fileName},
		ResultChan: resChan,
		Scan: func(row *sql.Row) error {
			return row.Scan(&fileID)
		},
	}
	res := <-resChan
	if res.Err != nil {
		return fmt.Errorf("failed to upsert file: %w", res.Err)
	}

	// Create a new version for this backup
	var versionID int64
	p.DBJobChan <- db.DBJob{
		Query: `
		INSERT INTO file_versions (file_id, backup_id, mtime, size, hash_plain, uid, gid, mode)
		VALUES (?, ?, ?, ?, '', ?, ?, ?)
		RETURNING id
	`,
		Args:       []interface{}{fileID, job.BackupID, info.ModTime().Unix(), info.Size(), fileUID, fileGID, uint32(fileMode)},
		ResultChan: resChan,
		Scan: func(row *sql.Row) error {
			return row.Scan(&versionID)
		},
	}
	res = <-resChan
	if res.Err != nil {
		return fmt.Errorf("failed to insert file version: %w", res.Err)
	}

	// 4. Process in 4MB blocks
	buf := make([]byte, 4*1024*1024) // 4MB blocks
	sequence := 0

	for {
		n, err := inFile.Read(buf)
		if n > 0 {
			rawChunk := buf[:n]
			
			// Compress with zstd
			chunk := p.zstdEncoder.EncodeAll(rawChunk, nil)
			
			// Hash plaintext chunk (now compressed)
			plainHash := crypto.Hash(chunk)
			plainHashHex := hex.EncodeToString(plainHash)
			
			fullPlainHasher.Write(rawChunk) // Full file hash uses raw data

			// Check local deduplication
			var encHashHex string
			resChan := make(chan db.DBResult)
			p.DBJobChan <- db.DBJob{
				Query:      "SELECT hash_encrypted FROM local_blobs WHERE hash_plain = ?",
				Args:       []interface{}{plainHashHex},
				ResultChan: resChan,
				Scan: func(row *sql.Row) error {
					return row.Scan(&encHashHex)
				},
			}
			res := <-resChan
			
			ciphertextSize := len(chunk) + crypto.NonceSizeX + 16 // Poly1305 tag is 16 bytes

			if res.Err == sql.ErrNoRows {
				// Encrypt the chunk (uses Convergent Encryption with BLAKE3 hash of compressed chunk as nonce)
				ciphertext, encErr := crypto.Encrypt(p.Key, chunk)
				if encErr != nil {
					return fmt.Errorf("encryption error: %w", encErr)
				}

				// Hash ciphertext chunk
				encHash := crypto.Hash(ciphertext)
				encHashHex = hex.EncodeToString(encHash)
				
				// Record in local_blobs
				p.DBJobChan <- db.DBJob{
					Query:      "INSERT INTO local_blobs (hash_plain, hash_encrypted, size) VALUES (?, ?, ?)",
					Args:       []interface{}{plainHashHex, encHashHex, ciphertextSize},
					ResultChan: make(chan db.DBResult, 1),
				}

				// Write to spool and then upload
				tmpPath := filepath.Join(p.SpoolDir, "enc-"+encHashHex)
				if writeErr := os.WriteFile(tmpPath, ciphertext, 0600); writeErr != nil {
					return fmt.Errorf("spool write error: %w", writeErr)
				}
				
				uploadPath := filepath.Join(p.UploadDir, encHashHex)
				if renameErr := os.Rename(tmpPath, uploadPath); renameErr != nil {
					return fmt.Errorf("spool rename error: %w", renameErr)
				}

				// Queue for upload
				p.UploadChan <- UploadJob{
					FileID:    fileID,
					Hash:      encHashHex,
					Size:      int64(ciphertextSize),
					IsSpecial: false,
				}
			} else if res.Err != nil {
				return fmt.Errorf("local dedup check error: %w", res.Err)
			}

			// Link blob to file directly by hash (always done, whether new or deduplicated)
			p.DBJobChan <- db.DBJob{
				Query:      "INSERT INTO file_blobs (version_id, hash_encrypted, size, sequence) VALUES (?, ?, ?, ?)",
				Args:       []interface{}{versionID, encHashHex, ciphertextSize, sequence},
				ResultChan: make(chan db.DBResult, 1),
			}

			sequence++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("file read error: %w", err)
		}
	}

	// 5. Finalize full file hashes and update the files table
	fullPlainHashHex := hex.EncodeToString(fullPlainHasher.Sum(nil))

	p.DBJobChan <- db.DBJob{
		Query:      "UPDATE file_versions SET hash_plain = ? WHERE id = ?",
		Args:       []interface{}{fullPlainHashHex, versionID},
		ResultChan: make(chan db.DBResult, 1),
	}

	return nil
}
