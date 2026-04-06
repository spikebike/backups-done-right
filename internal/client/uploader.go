package client

import (
	"bytes"
	"context"
	"encoding/hex"
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
	Items []rpc.ItemData
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
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case job, ok := <-u.UploadChan:
			if !ok {
				// Process remaining jobs in the final batch
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
					log.Printf("[Offer Pipeline %d] Flushing partial batch of %d items due to timeout", workerID, len(batch))
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
		if len(pending.Items) > 0 {
			if u.FakeUpload {
				if u.Verbose {
					log.Printf("[Upload Pump %d] Fake upload: Pretending to stream %d items", workerID, len(pending.Items))
				}
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

				var items []rpc.StreamItem
				for _, b := range pending.Items {
					hashBytes, _ := hex.DecodeString(b.Meta.Hash)
					item := rpc.StreamItem{
						Header: rpc.StreamItemHeader{
							OpCode: rpc.OpCodePush,
							Flags:  rpc.FlagTypeClientBlob,
							Size:   uint64(len(b.Data)),
						},
						Data: bytes.NewReader(b.Data),
					}
					copy(item.Header.Hash[:], hashBytes)
					items = append(items, item)
				}

				err := u.RPCClient.UploadItemsStreamed(ctx, items)
				cancel()

				if err != nil {
					log.Printf("[Upload Pump %d] Failed to upload stream to server: %v", workerID, err)
					// Note: On failure, we might lose DB sync status for this batch. Production versions should retry.
				} else {
					// Track upload stats
					if u.Stats != nil {
						for _, item := range pending.Items {
							u.Stats.AddFileUploaded(int64(len(item.Data)))
						}
					}
					// Update database with upload count
					u.DBJobChan <- db.DBJob{
						Query:      "UPDATE backups SET uploaded_files = uploaded_files + ? WHERE id = ?",
						Args:       []interface{}{len(pending.Items), u.CurrentBackupID},
						ResultChan: make(chan db.DBResult, 1),
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

	var itemsToOffer []rpc.Metadata
	for _, job := range batch {
		itemsToOffer = append(itemsToOffer, rpc.Metadata{
			Hash:      job.Hash,
			Size:      job.Size,
			IsSpecial: job.IsSpecial,
		})
	}

	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var plan rpc.UploadPlan
	var err error

	if u.FakeUpload {
		if u.Verbose {
			log.Printf("[Offer Pipeline %d] Fake upload: Pretending to offer %d items", workerID, len(itemsToOffer))
		}
		for i := range itemsToOffer {
			plan.NeededIndices = append(plan.NeededIndices, int32(i))
		}
	} else {
		plan, err = u.RPCClient.OfferItems(ctx, itemsToOffer)
		if err != nil {
			log.Printf("[Offer Pipeline %d] Failed to prepare upload: %v", workerID, err)
			releaseBatch(batch) // Abort, release all back to pool.
			return
		}
	}
	neededIndices := plan.NeededIndices

	if u.Verbose {
		log.Printf("[Offer Pipeline %d] Server requested %d out of %d offered items", workerID, len(neededIndices), len(itemsToOffer))
	}

	// Figure out which ones are needed vs not needed
	neededMap := make(map[int32]bool)
	for _, idx := range neededIndices {
		neededMap[idx] = true
	}

	var neededJobs []UploadJob
	var itemsToUpload []rpc.ItemData
	var unneededJobs []UploadJob

	for i, job := range batch {
		if neededMap[int32(i)] {
			neededJobs = append(neededJobs, job)
			itemsToUpload = append(itemsToUpload, rpc.ItemData{
				Meta: itemsToOffer[i],
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
		Items: itemsToUpload,
		Jobs:  neededJobs,
	}

	// Update database with offered count
	u.DBJobChan <- db.DBJob{
		Query:      "UPDATE backups SET offered_files = offered_files + ? WHERE id = ?",
		Args:       []interface{}{len(itemsToOffer), u.CurrentBackupID},
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
