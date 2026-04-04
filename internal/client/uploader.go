package client

import (
	"context"
	"log"
	"sync"
	"time"

	"p2p-backup/internal/db"
	"p2p-backup/internal/rpc"
)

// Uploader manages the syncing of encrypted files and deletions to the server.
type Uploader struct {
	DBJobChan          chan<- db.DBJob
	UploadChan         <-chan UploadJob
	RPCClient          RPCClient
	NumWorkers         int
	BatchUploadSize    int
	FakeUpload         bool
	Verbose            bool
	CurrentBackupID    int64
	Stats              *BackupStats
	wg                 sync.WaitGroup
	uploadWg           sync.WaitGroup
	uploadPipelineChan chan pendingUpload
}

type pendingUpload struct {
	Blobs []rpc.LocalBlobData
	Jobs  []UploadJob
}

// NewUploader creates a new Uploader instance.
func NewUploader(dbJobChan chan<- db.DBJob, uploadChan <-chan UploadJob, rpcClient RPCClient, numWorkers, batchSize int, fakeUpload, verbose bool, currentBackupID int64, stats *BackupStats) *Uploader {
	if batchSize <= 0 {
		batchSize = 10 // Default batch size
	}
	if numWorkers <= 0 {
		numWorkers = 1
	}
	return &Uploader{
		DBJobChan:       dbJobChan,
		UploadChan:      uploadChan,
		RPCClient:       rpcClient,
		NumWorkers:      numWorkers,
		BatchUploadSize: batchSize,
		FakeUpload:      fakeUpload,
		Verbose:         verbose,
		CurrentBackupID: currentBackupID,
		Stats:           stats,
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
	ticker := time.NewTicker(2 * time.Second) // Flush partial batches every 2 seconds
	defer ticker.Stop()

	for {
		select {
		case job, ok := <-u.UploadChan:
			if !ok {
				if len(batch) > 0 {
					u.processOfferBatch(batch, workerID)
				}
				return
			}
			batch = append(batch, job)
			if len(batch) >= u.BatchUploadSize {
				u.processOfferBatch(batch, workerID)
				batch = nil
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if u.Verbose {
					log.Printf("[Offer Pipeline %d] Periodic flush of %d jobs", workerID, len(batch))
				}
				u.processOfferBatch(batch, workerID)
				batch = nil
			}
		}
	}
}

func (u *Uploader) uploadWorker(workerID int) {
	defer u.uploadWg.Done()

	for pending := range u.uploadPipelineChan {
		if u.FakeUpload {
			if u.Verbose {
				log.Printf("[Upload Pump %d] Fake upload: Pretending to stream %d blobs", workerID, len(pending.Blobs))
			}
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // allow longer for stream

			var blobsMeta []rpc.BlobMeta
			for _, blob := range pending.Blobs {
				blobsMeta = append(blobsMeta, rpc.BlobMeta{
					Hash:    blob.Hash,
					Size:    int64(len(blob.Data)),
					Special: blob.IsSpecial,
				})
			}

			err := u.RPCClient.PrepareUploadClient(ctx, blobsMeta)
			if err != nil {
				log.Printf("[Upload Pump %d] Failed to prepare upload: %v", workerID, err)
			} else {
				for _, blob := range pending.Blobs {
					err = u.RPCClient.PushBlob(ctx, blob.Hash, blob.Data)
					if err != nil {
						log.Printf("[Upload Pump %d] Failed to push blob %s: %v", workerID, blob.Hash, err)
						break
					}
				}
				if err == nil {
					// Track upload stats
					if u.Stats != nil {
						for _, blob := range pending.Blobs {
							u.Stats.AddFileUploaded(int64(len(blob.Data)))
						}
					}
					// Update database with upload count
					u.DBJobChan <- db.DBJob{
						Query:      "UPDATE backups SET uploaded_files = uploaded_files + ? WHERE id = ?",
						Args:       []interface{}{len(pending.Blobs), u.CurrentBackupID},
						ResultChan: make(chan db.DBResult, 1),
					}
				}
			}
			cancel()
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
		if u.Verbose {
			log.Printf("[Offer Pipeline %d] Sending OfferBlobs RPC for %d blobs...", workerID, len(blobsToOffer))
		}
		neededIndices, err = u.RPCClient.OfferBlobs(ctx, blobsToOffer)
		if u.Verbose {
			log.Printf("[Offer Pipeline %d] OfferBlobs RPC returned (err=%v)", workerID, err)
		}
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
	u.DBJobChan <- db.DBJob{
		Query:      "UPDATE backups SET offered_files = offered_files + ? WHERE id = ?",
		Args:       []interface{}{len(blobsToOffer), u.CurrentBackupID},
		ResultChan: make(chan db.DBResult, 1),
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
