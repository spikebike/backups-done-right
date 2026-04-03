package client

import (
	"context"
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"p2p-backup/internal/rpc"
)

// Uploader manages the syncing of encrypted files and deletions to the server.
type Uploader struct {
	DB              *sql.DB
	UploadChan      <-chan UploadJob
	RPCClient       RPCClient
	UploadDir       string
	BatchUploadSize int
	FakeUpload      bool
	Verbose         bool
	CurrentBackupID int64
	wg              sync.WaitGroup
}

// NewUploader creates a new Uploader instance.
func NewUploader(db *sql.DB, uploadChan <-chan UploadJob, rpcClient RPCClient, uploadDir string, batchSize int, fakeUpload, verbose bool, currentBackupID int64) *Uploader {
	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}
	return &Uploader{
		DB:              db,
		UploadChan:      uploadChan,
		RPCClient:       rpcClient,
		UploadDir:       uploadDir,
		BatchUploadSize: batchSize,
		FakeUpload:      fakeUpload,
		Verbose:         verbose,
		CurrentBackupID: currentBackupID,
	}
}

// Start begins listening for upload jobs.
func (u *Uploader) Start() {
	if u.Verbose {
		log.Println("Uploader thread 1 of 1 launched")
	}

	u.wg.Add(1)

	go func() {
		defer u.wg.Done()

		var batch []UploadJob

		for job := range u.UploadChan {
			batch = append(batch, job)

			if len(batch) >= u.BatchUploadSize {
				u.processBatch(batch)
				batch = nil // Reset batch
			}
		}

		// Process remaining jobs in the final batch
		if len(batch) > 0 {
			u.processBatch(batch)
		}
	}()
}

func (u *Uploader) processBatch(batch []UploadJob) {
	if u.Verbose {
		log.Printf("Uploader processing batch of %d jobs", len(batch))
	}

	var blobsToOffer []rpc.BlobMeta
	var uploadJobs []UploadJob

	for _, job := range batch {
		uploadJobs = append(uploadJobs, job)
		blobsToOffer = append(blobsToOffer, rpc.BlobMeta{
			Hash:    job.Hash,
			Size:    job.Size,
			Special: job.IsSpecial,
		})
	}

	if len(uploadJobs) == 0 {
		return
	}

	// 2. Offer Blobs to Server
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var neededIndices []uint32
	var err error

	if u.FakeUpload {
		if u.Verbose {
			log.Printf("Fake upload: Pretending to offer %d blobs", len(blobsToOffer))
		}
		// In fake upload, pretend server needs all of them
		neededIndices = make([]uint32, len(blobsToOffer))
		for i := range blobsToOffer {
			neededIndices[i] = uint32(i)
		}
	} else {
		neededIndices, err = u.RPCClient.OfferBlobs(ctx, blobsToOffer)
		if err != nil {
			log.Printf("Failed to offer blobs to server: %v", err)
			return // Cannot proceed with this batch
		}
	}

	if u.Verbose {
		log.Printf("Server requested %d out of %d offered blobs", len(neededIndices), len(blobsToOffer))
	}

	// 3. Upload Needed Blobs
	if len(neededIndices) > 0 {
		var blobsToUpload []rpc.LocalBlobData
		for _, idx := range neededIndices {
			job := batch[idx]

			blobPath := filepath.Join(u.UploadDir, job.Hash)
			
			data, err := os.ReadFile(blobPath)
			if err != nil {
				log.Printf("Failed to read blob %s for upload: %v", job.Hash, err)
				continue
			}

			blobsToUpload = append(blobsToUpload, rpc.LocalBlobData{
				Hash: job.Hash,
				Data: data,
			})
		}

		if u.FakeUpload {
			if u.Verbose {
				log.Printf("Fake upload: Pretending to upload %d blobs", len(blobsToUpload))
			}
		} else {
			err = u.RPCClient.UploadBlobs(ctx, blobsToUpload)
			if err != nil {
				log.Printf("Failed to upload blobs to server: %v", err)
				return // Upload failed
			}
		}

		// Update database with upload count
		_, err := u.DB.Exec("UPDATE backups SET uploaded_files = uploaded_files + ? WHERE id = ?", len(blobsToUpload), u.CurrentBackupID)
		if err != nil {
			log.Printf("Failed to update uploaded_files count: %v", err)
		}

		// Clean up uploaded blobs from the upload directory
		for _, blob := range blobsToUpload {
			blobPath := filepath.Join(u.UploadDir, blob.Hash)
			if err := os.Remove(blobPath); err != nil && !os.IsNotExist(err) {
				log.Printf("Failed to remove uploaded blob %s: %v", blobPath, err)
			}
		}
	}

	// Update database with offered count
	_, err = u.DB.Exec("UPDATE backups SET offered_files = offered_files + ? WHERE id = ?", len(blobsToOffer), u.CurrentBackupID)
	if err != nil {
		log.Printf("Failed to update offered_files count: %v", err)
	}
}

// Wait blocks until all upload jobs are processed.
func (u *Uploader) Wait() {
	u.wg.Wait()
	if u.Verbose {
		log.Println("Uploader thread finished")
	}
}
