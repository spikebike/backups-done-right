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
	"time"

	"github.com/klauspost/compress/zstd"
	"lukechampine.com/blake3"
	"p2p-backup/internal/crypto"
	"p2p-backup/internal/db"
)

const (
	// defaultBlockSize is the default 4MB chunk size for file splitting.
	defaultBlockSize = 4 * 1024 * 1024
	// cipherOverhead is the nonce (24 bytes) + Poly1305 tag (16 bytes).
	cipherOverhead = crypto.NonceSizeX + 16
)

// UploadJob represents a blob that is ready to be uploaded.
type UploadJob struct {
	FileID    int64
	Hash      string // Blob hash
	Size      int64  // Blob size
	Data      []byte // Encrypted data in memory
	IsSpecial bool
	Release   func() // Returns the Data buffer to the pool (nil if not pooled)
}

// CryptoPool manages a pool of workers that encrypt files and update the local database.
type CryptoPool struct {
	DB             *sql.DB
	DBJobChan      chan<- db.DBJob
	Key            []byte
	NumWorkers     int
	UploadChan     chan<- UploadJob
	Verbose        bool
	wg             sync.WaitGroup
	sessionUploads sync.Map   // Track hashes already queued in this session (plainHashHex -> encHashHex)
	cipherBufs     chan []byte // Pre-allocated free-list of reusable ciphertext buffers
	rawChunkPool   sync.Pool  // Reusable raw chunk read buffers
}

// NewCryptoPool initializes a new CryptoPool.
func NewCryptoPool(db *sql.DB, dbJobChan chan<- db.DBJob, key []byte, numWorkers int, uploadChan chan<- UploadJob, verbose bool) *CryptoPool {
	// Pre-allocate cipher buffers: enough to fill the upload channel plus
	// one per crypto worker so encryption never stalls waiting for a free buffer
	// while the channel still has capacity.
	poolSize := cap(uploadChan) + numWorkers
	if poolSize <= 0 {
		poolSize = 100
	}
	cipherBufs := make(chan []byte, poolSize)
	for i := 0; i < poolSize; i++ {
		cipherBufs <- make([]byte, 0, defaultBlockSize+cipherOverhead+1024)
	}
	if verbose {
		log.Printf("Pre-allocated %d cipher buffers (%.0f MB)",
			poolSize, float64(poolSize*(defaultBlockSize+cipherOverhead+1024))/(1024*1024))
	}

	return &CryptoPool{
		DB:          db,
		DBJobChan:   dbJobChan,
		Key:         key,
		NumWorkers:  numWorkers,
		UploadChan:  uploadChan,
		Verbose:     verbose,
		cipherBufs:  cipherBufs,
		rawChunkPool: sync.Pool{
			New: func() any {
				return make([]byte, defaultBlockSize)
			},
		},
	}
}

// Start launches the worker goroutines.
func (p *CryptoPool) Start(jobChan <-chan FileJob) {
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
	
	zstdEncoder, _ := zstd.NewWriter(nil)
	defer zstdEncoder.Close()

	// Worker-local compression buffer, reused across all chunks/files.
	compressBuf := make([]byte, 0, defaultBlockSize)

	for job := range jobChan {
		err := p.processFile(job, zstdEncoder, &compressBuf)
		if err != nil {
			log.Printf("[Worker %d] Error processing %s: %v", workerID, job.Path, err)
		} else {
			if p.Verbose {
				log.Printf("[Worker %d] Successfully processed %s", workerID, job.Path)
			}
		}
	}
}

func (p *CryptoPool) processFile(job FileJob, zstdEncoder *zstd.Encoder, compressBuf *[]byte) error {
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
	chunkBuf := p.rawChunkPool.Get().([]byte)
	defer p.rawChunkPool.Put(chunkBuf)
	sequence := 0

	for {
		n, err := inFile.Read(chunkBuf)
		if n > 0 {
			rawChunk := chunkBuf[:n]
			
			// Compress with zstd (reuses worker-local buffer)
			*compressBuf = zstdEncoder.EncodeAll(rawChunk, (*compressBuf)[:0])
			chunk := *compressBuf
			
			// Hash plaintext chunk (now compressed)
			plainHash := crypto.Hash(chunk)
			plainHashHex := hex.EncodeToString(plainHash)
			
			fullPlainHasher.Write(rawChunk) // Full file hash uses raw data

			// Check session deduplication — fast in-memory cache to prevent concurrent processing of the same chunk
			var encHashHex string
			ciphertextSize := len(chunk) + cipherOverhead

			if _, loaded := p.sessionUploads.LoadOrStore(plainHashHex, ""); loaded {
				// Another worker is processing this hash — wait for it to finish encrypting
				start := time.Now()
				for time.Since(start) < 5*time.Second { // Longer timeout shouldn't be needed, but safe
					if val, ok := p.sessionUploads.Load(plainHashHex); ok {
						if s := val.(string); s != "" {
							encHashHex = s
							break
						}
					}
					time.Sleep(10 * time.Millisecond)
				}
				if encHashHex == "" {
					return fmt.Errorf("timeout waiting for concurrent worker to process hash %s", plainHashHex)
				}
			} else {
				// We own this hash for this session — encrypt it
				cipherBuf := <-p.cipherBufs
				ciphertext, encErr := crypto.EncryptTo(p.Key, chunk, cipherBuf)
				if encErr != nil {
					p.cipherBufs <- cipherBuf[:0]
					p.sessionUploads.Delete(plainHashHex)
					return fmt.Errorf("encryption error: %w", encErr)
				}

				// Hash ciphertext chunk
				encHash := crypto.Hash(ciphertext)
				encHashHex = hex.EncodeToString(encHash)
				
				// Update session cache with actual encHashHex so waiting workers can proceed
				p.sessionUploads.Store(plainHashHex, encHashHex)

				// Record in local_blobs DB (Asynchronous - non blocking)
				p.DBJobChan <- db.DBJob{
					Query:      "INSERT OR IGNORE INTO local_blobs (hash_plain, hash_encrypted, size) VALUES (?, ?, ?)",
					Args:       []interface{}{plainHashHex, encHashHex, ciphertextSize},
					ResultChan: make(chan db.DBResult, 1),
				}

				// Queue for upload (In-Memory) — uploader calls Release to return buffer
				bufs := p.cipherBufs // capture for closure
				p.UploadChan <- UploadJob{
					FileID:    fileID,
					Hash:      encHashHex,
					Size:      int64(ciphertextSize),
					Data:      ciphertext,
					IsSpecial: false,
					Release: func() {
						bufs <- cipherBuf[:0]
					},
				}
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
