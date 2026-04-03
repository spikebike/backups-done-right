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
	DB                 *sql.DB
	UploadChan         <-chan UploadJob
	RPCClient          RPCClient
	NumWorkers         int
	BatchUploadSize    int
	FakeUpload         bool
	Verbose            bool
	CurrentBackupID    int64
	wg                 sync.WaitGroup
	uploadWg           sync.WaitGroup
	uploadPipelineChan chan pendingUpload
}

type pendingUpload struct {
	Blobs []rpc.LocalBlobData
	Jobs  []UploadJob
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
		log.Printf("Launching %d offer pipeline threads and %d upload data pump threads", u.NumWorkers, u.NumWorkers)
	}

	u.uploadPipelineChan = make(chan pendingUpload, u.NumWorkers*4)

	for i := 0; i < u.NumWorkers; i++ {
		u.wg.Add(1)
		go u.offerWorker(i + 1)

		u.uploadWg.Add(1)
		go u.uploadWorker(i + 1)
	}
}

func (u *Uploader) offerWorker(workerID int) {
	defer u.wg.Done()

	var batch []UploadJob

	for job := range u.UploadChan {
		batch = append(batch, job)

		if len(batch) >= u.BatchUploadSize {
			u.processOfferBatch(batch, workerID)
			batch = nil // Reset batch
		}
	}

	// Process remaining jobs in the final batch
	if len(batch) > 0 {
		u.processOfferBatch(batch, workerID)
	}
}

func (u *Uploader) uploadWorker(workerID int) {
	defer u.uploadWg.Done()

	for pending := range u.uploadPipelineChan {
		if len(pending.Blobs) > 0 {
			if u.FakeUpload {
				if u.Verbose {
					log.Printf("[Upload Pump %d] Fake upload: Pretending to stream %d blobs", workerID, len(pending.Blobs))
				}
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // allow longer for stream
				err := u.RPCClient.UploadBlobs(ctx, pending.Blobs)
				cancel()
				
				if err != nil {
					log.Printf("[Upload Pump %d] Failed to upload stream to server: %v", workerID, err)
					// Note: On failure, we might lose DB sync status for this batch. Production versions should retry.
				} else {
					// Update database with upload count
					_, err := u.DB.Exec("UPDATE backups SET uploaded_files = uploaded_files + ? WHERE id = ?", len(pending.Blobs), u.CurrentBackupID)
					if err != nil {
						log.Printf("[Upload Pump %d] Failed to update uploaded_files count: %v", workerID, err)
					}
				}
			}
		}

		// Always release buffers back to pool
		releaseBatch(pending.Jobs)
	}
}

// releaseBatch returns all pooled ciphertext buffers to the free-list
func releaseBatch(batch []UploadJob) {
	for i := range batch {
		if batch[i].Release != nil {
			batch[i].Release()
			batch[i].Release = nil
		}
	}
}

func (u *Uploader) processOfferBatch(batch []UploadJob, workerID int) {
	if u.Verbose {
		log.Printf("[Offer Pipeline %d] Offering batch of %d jobs", workerID, len(batch))
	}

	var blobsToOffer []rpc.BlobMeta
	for _, job := range batch {
		blobsToOffer = append(blobsToOffer, rpc.BlobMeta{
			Hash:    job.Hash,
			Size:    job.Size,
			Special: job.IsSpecial,
		})
	}

	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var neededIndices []uint32
	var err error

	if u.FakeUpload {
		if u.Verbose {
			log.Printf("[Offer Pipeline %d] Fake upload: Pretending to offer %d blobs", workerID, len(blobsToOffer))
		}
		neededIndices = make([]uint32, len(blobsToOffer))
		for i := range blobsToOffer {
			neededIndices[i] = uint32(i)
		}
	} else {
		neededIndices, err = u.RPCClient.OfferBlobs(ctx, blobsToOffer)
		if err != nil {
			log.Printf("[Offer Pipeline %d] Failed to offer blobs to server: %v", workerID, err)
			releaseBatch(batch) // Abort, release all back to pool.
			return
		}
	}

	if u.Verbose {
		log.Printf("[Offer Pipeline %d] Server requested %d out of %d offered blobs", workerID, len(neededIndices), len(blobsToOffer))
	}

	// Figure out which ones are needed vs not needed
	neededMap := make(map[uint32]bool)
	for _, idx := range neededIndices {
		neededMap[idx] = true
	}

	var neededJobs []UploadJob
	var blobsToUpload []rpc.LocalBlobData
	var unneededJobs []UploadJob

	for i, job := range batch {
		if neededMap[uint32(i)] {
			neededJobs = append(neededJobs, job)
			blobsToUpload = append(blobsToUpload, rpc.LocalBlobData{
				Hash: job.Hash,
				Data: job.Data,
			})
		} else {
			unneededJobs = append(unneededJobs, job)
		}
	}

	// Immediately release unneeded jobs since we don't have to upload them
	releaseBatch(unneededJobs)

	// Send to data pump pipeline for upload
	u.uploadPipelineChan <- pendingUpload{
		Blobs: blobsToUpload,
		Jobs:  neededJobs,
	}

	// Update database with offered count
	_, err = u.DB.Exec("UPDATE backups SET offered_files = offered_files + ? WHERE id = ?", len(blobsToOffer), u.CurrentBackupID)
	if err != nil {
		log.Printf("[Offer Pipeline %d] Failed to update offered_files count: %v", workerID, err)
	}
}

// Wait blocks until all upload jobs are processed.
func (u *Uploader) Wait() {
	u.wg.Wait()
	if u.Verbose {
		log.Println("Offer pipeline threads finished. Closing data pump...")
	}
	close(u.uploadPipelineChan)
	u.uploadWg.Wait()
	if u.Verbose {
		log.Println("All data pump threads finished. Uploader complete.")
	}
}
