package client

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"p2p-backup/internal/rpc"
)

// Uploader manages the syncing of encrypted files and deletions to the server.
type Uploader struct {
	DB              *sql.DB
	UploadChan      <-chan UploadJob
	RPCClient       RPCClient
	NumWorkers      int
	BatchUploadSize int
	FakeUpload      bool
	Verbose         bool
	CurrentBackupID int64
	wg              sync.WaitGroup
}

// NewUploader creates a new Uploader instance.
func NewUploader(db *sql.DB, uploadChan <-chan UploadJob, rpcClient RPCClient, numWorkers, batchSize int, fakeUpload, verbose bool, currentBackupID int64) *Uploader {
	if batchSize <= 0 {
		batchSize = 10 // Default batch size
	}
	if numWorkers <= 0 {
		numWorkers = 1
	}
	return &Uploader{
		DB:              db,
		UploadChan:      uploadChan,
		RPCClient:       rpcClient,
		NumWorkers:      numWorkers,
		BatchUploadSize: batchSize,
		FakeUpload:      fakeUpload,
		Verbose:         verbose,
		CurrentBackupID: currentBackupID,
	}
}

// Start begins listening for upload jobs.
func (u *Uploader) Start() {
	if u.Verbose {
		log.Printf("Launching %d uploader threads", u.NumWorkers)
	}

	for i := 0; i < u.NumWorkers; i++ {
		u.wg.Add(1)
		workerID := i + 1
		go u.worker(workerID)
	}
}

func (u *Uploader) worker(workerID int) {
	defer u.wg.Done()

	var batch []UploadJob

	for job := range u.UploadChan {
		batch = append(batch, job)

		if len(batch) >= u.BatchUploadSize {
			u.processBatch(batch, workerID)
			releaseBatch(batch)
			batch = nil // Reset batch
		}
	}

	// Process remaining jobs in the final batch
	if len(batch) > 0 {
		u.processBatch(batch, workerID)
		releaseBatch(batch)
	}
}

// releaseBatch returns all pooled ciphertext buffers to the sync.Pool.
func releaseBatch(batch []UploadJob) {
	for i := range batch {
		if batch[i].Release != nil {
			batch[i].Release()
			batch[i].Release = nil
		}
	}
}

func (u *Uploader) processBatch(batch []UploadJob, workerID int) {
	if u.Verbose {
		log.Printf("[Uploader %d] Processing batch of %d jobs", workerID, len(batch))
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
			log.Printf("[Uploader %d] Fake upload: Pretending to offer %d blobs", workerID, len(blobsToOffer))
		}
		// In fake upload, pretend server needs all of them
		neededIndices = make([]uint32, len(blobsToOffer))
		for i := range blobsToOffer {
			neededIndices[i] = uint32(i)
		}
	} else {
		neededIndices, err = u.RPCClient.OfferBlobs(ctx, blobsToOffer)
		if err != nil {
			log.Printf("[Uploader %d] Failed to offer blobs to server: %v", workerID, err)
			return // Cannot proceed with this batch
		}
	}

	if u.Verbose {
		log.Printf("[Uploader %d] Server requested %d out of %d offered blobs", workerID, len(neededIndices), len(blobsToOffer))
	}

	// 3. Upload Needed Blobs
	if len(neededIndices) > 0 {
		var blobsToUpload []rpc.LocalBlobData
		for _, idx := range neededIndices {
			job := batch[idx]

			blobsToUpload = append(blobsToUpload, rpc.LocalBlobData{
				Hash: job.Hash,
				Data: job.Data,
			})
		}

		if u.FakeUpload {
			if u.Verbose {
				log.Printf("[Uploader %d] Fake upload: Pretending to upload %d blobs", workerID, len(blobsToUpload))
			}
		} else {
			err = u.RPCClient.UploadBlobs(ctx, blobsToUpload)
			if err != nil {
				log.Printf("[Uploader %d] Failed to upload blobs to server: %v", workerID, err)
				return // Upload failed
			}
		}

		// Update database with upload count
		_, err := u.DB.Exec("UPDATE backups SET uploaded_files = uploaded_files + ? WHERE id = ?", len(blobsToUpload), u.CurrentBackupID)
		if err != nil {
			log.Printf("[Uploader %d] Failed to update uploaded_files count: %v", workerID, err)
		}
	}

	// Update database with offered count
	_, err = u.DB.Exec("UPDATE backups SET offered_files = offered_files + ? WHERE id = ?", len(blobsToOffer), u.CurrentBackupID)
	if err != nil {
		log.Printf("[Uploader %d] Failed to update offered_files count: %v", workerID, err)
	}
}

// Wait blocks until all upload jobs are processed.
func (u *Uploader) Wait() {
	u.wg.Wait()
	if u.Verbose {
		log.Println("All uploader threads finished")
	}
}
