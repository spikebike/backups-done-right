package client

import (
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
)

const (
	// defaultBlockSize is the default 4MB chunk size for file splitting.
	defaultBlockSize = 4 * 1024 * 1024
	// cipherOverhead is the nonce (24 bytes) + Poly1305 tag (16 bytes).
	cipherOverhead = crypto.NonceSizeX + 16
)

// UploadJob represents a blob that is ready to be uploaded.
type UploadJob struct {
	Hash      string // Blob hash
	Size      int64  // Blob size
	Data      []byte // Encrypted data in memory
	IsSpecial bool
	Release   func() // Returns the Data buffer to the pool (nil if not pooled)
}

// FileArchive represents a completed file ready to be saved to local state.
type FileArchive struct {
	DirPath   string
	FileName  string
	BackupID  int64
	Deleted   bool
	Mtime     int64
	Size      int64
	UID       uint32
	GID       uint32
	Mode      uint32
	PlainHash string
	Chunks    []ChunkArchive
}

type ChunkArchive struct {
	Sequence  int
	PlainHash string
	EncHash   string
	Size      int
}

type CryptoPool struct {
	Key            []byte
	NumWorkers     int
	UploadChan     chan<- UploadJob
	ArchiveChan    chan<- FileArchive
	Verbose        bool
	Compress       bool
	Stats          *BackupStats
	wg             sync.WaitGroup
	sessionUploads sync.Map    // Track hashes already queued in this session (plainHashHex -> encHashHex)
	cipherBufs     chan []byte // Pre-allocated free-list of reusable ciphertext buffers
	rawChunkPool   sync.Pool   // Reusable raw chunk read buffers
}

func NewCryptoPool(key []byte, numWorkers int, uploadChan chan<- UploadJob, archiveChan chan<- FileArchive, verbose bool, compress bool, stats *BackupStats) *CryptoPool {
	poolSize := cap(uploadChan) + numWorkers
	if poolSize <= 0 {
		poolSize = 100
	}
	cipherBufs := make(chan []byte, poolSize)
	for i := 0; i < poolSize; i++ {
		cipherBufs <- make([]byte, 0, defaultBlockSize+cipherOverhead+1024)
	}

	return &CryptoPool{
		Key:          key,
		NumWorkers:   numWorkers,
		UploadChan:   uploadChan,
		ArchiveChan:  archiveChan,
		Verbose:      verbose,
		Compress:     compress,
		Stats:        stats,
		cipherBufs:   cipherBufs,
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
			log.Printf("[Worker %d] Error processing %s/%s: %v", workerID, job.DirPath, job.FileName, err)
		} else {
			if p.Verbose {
				log.Printf("[Worker %d] Successfully processed %s/%s", workerID, job.DirPath, job.FileName)
			}
		}
	}
}

func (p *CryptoPool) processFile(job FileJob, zstdEncoder *zstd.Encoder, compressBuf *[]byte) error {
	fullPath := filepath.Join(job.DirPath, job.FileName)

	if job.Deleted {
		p.ArchiveChan <- FileArchive{
			DirPath:  job.DirPath,
			FileName: job.FileName,
			BackupID: job.BackupID,
			Deleted:  true,
		}
		return nil
	}

	// 1. Read file stats (Lstat to avoid following symlinks)
	info, err := os.Lstat(fullPath)
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
	inFile, err := os.Open(fullPath)
	if err != nil {
		return err
	}
	defer inFile.Close()

	// We'll compute the full file hash as we read it
	fullPlainHasher := blake3.New(32, nil)
	
	archive := FileArchive{
		DirPath:  job.DirPath,
		FileName: job.FileName,
		BackupID: job.BackupID,
		Deleted:  false,
		Mtime:    info.ModTime().Unix(),
		Size:     info.Size(),
		UID:      fileUID,
		GID:      fileGID,
		Mode:     uint32(fileMode),
		Chunks:   []ChunkArchive{},
	}

	// 4. Process in 4MB blocks
	chunkBuf := p.rawChunkPool.Get().([]byte)
	defer p.rawChunkPool.Put(chunkBuf)
	sequence := 0

	for {
		n, err := inFile.Read(chunkBuf)
		if n > 0 {
			rawChunk := chunkBuf[:n]
			
			// Compress with zstd (reuses worker-local buffer) if enabled
			var chunk []byte
			if p.Compress {
				*compressBuf = zstdEncoder.EncodeAll(rawChunk, (*compressBuf)[:0])
				chunk = *compressBuf
			} else {
				chunk = rawChunk
			}
			
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

				// Queue for upload (In-Memory) — uploader calls Release to return buffer
				bufs := p.cipherBufs // capture for closure
				p.UploadChan <- UploadJob{
					Hash:      encHashHex,
					Size:      int64(ciphertextSize),
					Data:      ciphertext,
					IsSpecial: false,
					Release: func() {
						bufs <- cipherBuf[:0]
					},
				}
			}

			archive.Chunks = append(archive.Chunks, ChunkArchive{
				Sequence:  sequence,
				PlainHash: plainHashHex,
				EncHash:   encHashHex,
				Size:      ciphertextSize,
			})

			sequence++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("file read error: %w", err)
		}
	}

	// 5. Finalize full file metadata and send to StateManager
	archive.PlainHash = hex.EncodeToString(fullPlainHasher.Sum(nil))
	p.ArchiveChan <- archive

	if p.Stats != nil {
		p.Stats.AddFileRead(info.Size())
	}

	return nil
}
