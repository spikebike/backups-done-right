package server

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"lukechampine.com/blake3"
	"p2p-backup/internal/crypto"
	"p2p-backup/internal/rpc"
)

const (
	OpCodePushPiece     byte = 0x01
	OpCodePullPiece     byte = 0x02
	OpCodePushBlob      byte = 0x03
	OpCodePullBlob      byte = 0x04
	OpCodePushBlobBatch byte = 0x05
	OpCodePullBlobBatch byte = 0x06
)

type PendingClientBlob struct {
	rpc.BlobMeta
	ClientPubKey string
}

type PendingStreamMeta struct {
	PeerID          int64
	IsSpecial       bool
	PieceIndex      int
	ParentShardHash string
	SequenceNumber  uint64
	TotalPieces     int
	Size            uint64
}

// HandleDataStream processes incoming raw data streams for /bdr/data/1.0.0
func (e *Engine) HandleDataStream(s network.Stream) {
	peerID := s.Conn().RemotePeer()
	defer s.Close()

	// Acquire concurrency semaphore slot
	e.streamSemaphore <- struct{}{}
	defer func() { <-e.streamSemaphore }()

	// 1. Read OpCode (1 byte)
	opBuf := make([]byte, 1)
	if _, err := io.ReadFull(s, opBuf); err != nil {
		log.Printf("DataStream: failed to read OpCode from %s: %v", peerID, err)
		return
	}
	opCode := opBuf[0]

	// 2. Dispatch based on OpCode
	switch opCode {
	case OpCodePushPiece, OpCodePushBlob, OpCodePullPiece, OpCodePullBlob:
		// These operations have a standard 40-byte header: [32 bytes Checksum][8 bytes Size]
		header := make([]byte, 40)
		if _, err := io.ReadFull(s, header); err != nil {
			log.Printf("DataStream: failed to read header from %s: %v", peerID, err)
			return
		}
		checksumHex := hex.EncodeToString(header[0:32])
		size := binary.BigEndian.Uint64(header[32:40])

		if opCode == OpCodePushPiece {
			e.handleIncomingPushPiece(s, checksumHex, size)
		} else if opCode == OpCodePushBlob {
			e.handleIncomingPushBlob(s, checksumHex, size)
		} else if opCode == OpCodePullPiece {
			e.handleIncomingPullPiece(s, checksumHex)
		} else {
			e.handleIncomingPullBlob(s, checksumHex)
		}

	case OpCodePushBlobBatch:
		e.handleIncomingPushBlobBatch(s)
	case OpCodePullBlobBatch:
		e.handleIncomingPullBlobBatch(s)
	default:
		log.Printf("DataStream: unknown OpCode %d from %s", opCode, peerID)
	}
}

func (e *Engine) handleIncomingPullBlobBatch(s network.Stream) {
	// 1. Read Count (uint32)
	countBuf := make([]byte, 4)
	if _, err := io.ReadFull(s, countBuf); err != nil {
		log.Printf("DataStream: failed to read pull batch count: %v", err)
		return
	}
	count := binary.BigEndian.Uint32(countBuf)

	for i := uint32(0); i < count; i++ {
		// 2. Read Checksum for each item: [32 bytes]
		checksumHeader := make([]byte, 32)
		if _, err := io.ReadFull(s, checksumHeader); err != nil {
			log.Printf("DataStream: failed to read pull batch item %d header: %v", i, err)
			break
		}
		checksumHex := hex.EncodeToString(checksumHeader)

		// 3. Fetch blob
		blobs, _, err := e.GetBlobs(context.Background(), []string{checksumHex})
		if err != nil || len(blobs) == 0 {
			log.Printf("DataStream: blob %s not found for pull batch", checksumHex)
			// Write 0 size to indicate failure for this item
			s.Write(make([]byte, 8))
			continue
		}

		// 4. Write response for item: [8 bytes Size][Data]
		blob := blobs[0]
		sizeBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(sizeBuf, uint64(len(blob.Data)))
		if _, err := s.Write(sizeBuf); err != nil {
			break
		}
		if _, err := s.Write(blob.Data); err != nil {
			break
		}
	}
}

func (e *Engine) handleIncomingPushBlobBatch(s network.Stream) {
	peerID := s.Conn().RemotePeer()
	
	// 1. Read Count (uint32)
	countBuf := make([]byte, 4)
	if _, err := io.ReadFull(s, countBuf); err != nil {
		log.Printf("DataStream: failed to read batch count from %s: %v", peerID, err)
		return
	}
	count := binary.BigEndian.Uint32(countBuf)

	if e.Verbose {
		log.Printf("DataStream: receiving batch of %d blobs from %s", count, peerID)
	}

	var ingestedBlobs []rpc.LocalBlobData
	var clientPubKey string

	for i := uint32(0); i < count; i++ {
		// 2. Read Header for each item: [32 bytes Checksum][8 bytes Size]
		header := make([]byte, 40)
		if _, err := io.ReadFull(s, header); err != nil {
			log.Printf("DataStream: failed to read batch item %d/%d header from %s: %v", i+1, count, peerID, err)
			break
		}
		checksumHex := hex.EncodeToString(header[0:32])
		size := binary.BigEndian.Uint64(header[32:40])

		// 3. Look up expected metadata
		metaVal, ok := e.pendingClientStreams.LoadAndDelete(checksumHex)
		if !ok {
			log.Printf("DataStream: item %d/%d hash %s NOT in pending map from %s. Draining %d bytes...", i+1, count, checksumHex, peerID, size)
			// We MUST drain the stream for this item to keep position
			io.CopyN(io.Discard, s, int64(size))
			continue
		}
		meta := metaVal.(PendingClientBlob)
		clientPubKey = meta.ClientPubKey

		// 4. Stream data
		data := make([]byte, size)
		hasher := blake3.New(32, nil)
		tee := io.TeeReader(s, hasher)

		if _, err := io.ReadFull(tee, data); err != nil {
			log.Printf("DataStream: failed to read data for item %d/%d (%s) from %s: %v", i+1, count, checksumHex, peerID, err)
			break
		}

		if hex.EncodeToString(hasher.Sum(nil)) != checksumHex {
			log.Printf("DataStream: checksum mismatch for item %d/%d (%s) from %s", i+1, count, checksumHex, peerID)
			continue
		}

		ingestedBlobs = append(ingestedBlobs, rpc.LocalBlobData{
			Hash:      checksumHex,
			Data:      data,
			IsSpecial: meta.Special,
		})
	}

	// 5. Atomic ingest for the entire batch
	if len(ingestedBlobs) > 0 {
		if err := e.IngestBlobs(context.Background(), clientPubKey, ingestedBlobs, false); err != nil {
			log.Printf("DataStream: failed to ingest batch: %v", err)
		} else if e.Verbose {
			log.Printf("DataStream: successfully received and ingested batch of %d blobs", len(ingestedBlobs))
		}
	}
}

func (e *Engine) handleIncomingPushPiece(s network.Stream, checksumHex string, size uint64) {
	// Look up expected metadata (needs to be set by prepareUpload RPC first)
	metaVal, ok := e.pendingInboundStreams.LoadAndDelete(checksumHex)
	if !ok {
		log.Printf("DataStream: unexpected push for %s (not in pending streams)", checksumHex)
		return
	}
	meta := metaVal.(PendingStreamMeta)

	tmpPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("peer_%s.tmp", checksumHex))
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("DataStream: failed to create tmp file for %s: %v", checksumHex, err)
		return
	}
	defer f.Close()

	hasher := blake3.New(32, nil)
	tee := io.TeeReader(s, hasher)

	// Stream with a 4MB buffer pool to bound memory
	buf := e.StreamBufferPool.Get().([]byte)
	defer e.StreamBufferPool.Put(buf)
	
	written, err := io.CopyBuffer(f, io.LimitReader(tee, int64(size)), buf)
	if err != nil {
		log.Printf("DataStream: failed to stream data for %s: %v", checksumHex, err)
		os.Remove(tmpPath)
		return
	}
	f.Close()

	if uint64(written) != size {
		log.Printf("DataStream: incomplete transfer for %s (got %d, expected %d)", checksumHex, written, size)
		os.Remove(tmpPath)
		return
	}

	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if actualHash != checksumHex {
		log.Printf("DataStream: checksum mismatch for %s (got %s)", checksumHex, actualHash)
		os.Remove(tmpPath)
		return
	}

	// Validation passed. Move to final location and commit to DB.
	finalPath := filepath.Join(e.BlobStoreDir, "peer_"+checksumHex)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		log.Printf("DataStream: failed to finalize file %s: %v", checksumHex, err)
		os.Remove(tmpPath)
		return
	}

	// Update hosted_shards database
	ctx := context.Background()
	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("DataStream: failed to begin transaction: %v", err)
		os.Remove(finalPath)
		return
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, 
		"INSERT OR IGNORE INTO hosted_shards (hash, size, peer_id, is_special, piece_index, parent_shard_hash, sequence, total_pieces) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		checksumHex, size, meta.PeerID, meta.IsSpecial, meta.PieceIndex, meta.ParentShardHash, meta.SequenceNumber, meta.TotalPieces)
	
	if err != nil {
		log.Printf("DataStream: failed to update DB for %s: %v", checksumHex, err)
		os.Remove(finalPath)
		return
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected > 0 {
		_, err = tx.ExecContext(ctx, "UPDATE peers SET current_storage_size = current_storage_size + ?, total_shards = total_shards + 1, current_shards = current_shards + 1 WHERE id = ?", size, meta.PeerID)
		if err != nil {
			log.Printf("DataStream: failed to update peer storage size: %v", err)
			os.Remove(finalPath)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("DataStream: failed to commit transaction: %v", err)
		os.Remove(finalPath)
		return
	}

	if e.Verbose {
		log.Printf("DataStream: successfully received and verified piece %s", checksumHex)
	}
}

func (e *Engine) handleIncomingPullPiece(s network.Stream, checksumHex string) {
	// Send the file over the stream
	filePath := filepath.Join(e.BlobStoreDir, "peer_"+checksumHex)
	f, err := os.Open(filePath)
	if err != nil {
		log.Printf("DataStream: failed to open requested file %s: %v", checksumHex, err)
		return
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return
	}

	// Send an 8-byte size header back
	sizeBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBuf, uint64(stat.Size()))
	if _, err := s.Write(sizeBuf); err != nil {
		log.Printf("DataStream: failed to write size header for pull %s: %v", checksumHex, err)
		return
	}

	buf := e.StreamBufferPool.Get().([]byte)
	defer e.StreamBufferPool.Put(buf)
	
	if _, err := io.CopyBuffer(s, f, buf); err != nil {
		log.Printf("DataStream: failed to send file %s: %v", checksumHex, err)
	} else if e.Verbose {
		log.Printf("DataStream: successfully served piece %s to peer", checksumHex)
	}
}

// PushPiece opens a raw stream to a peer and pushes the file data.
func (e *Engine) PushPiece(ctx context.Context, peerID int64, dataStream io.Reader, size int64, checksumHex string) error {
	// In test/localtest mode (no libp2p host), skip the raw data stream.
	// PrepareUpload already notified the peer via the Cap'n Proto callback.
	if e.Host == nil {
		if e.Verbose {
			log.Printf("PushPiece: skipping raw stream (no libp2p host) for %s to peer %d", checksumHex, peerID)
		}
		return nil
	}

	client, err := e.GetOrDialPeer(ctx, peerID)
	if err != nil {
		return fmt.Errorf("failed to dial peer: %w", err)
	}
	defer client.Close()
	
	var pubKeyHex string
	err = e.DB.QueryRowContext(ctx, "SELECT public_key FROM peers WHERE id = ?", peerID).Scan(&pubKeyHex)
	if err != nil {
		return err
	}
	pid, err := crypto.PeerIDFromPubKeyHex(pubKeyHex)
	if err != nil {
		return err
	}

	stream, err := e.Host.NewStream(ctx, pid, "/bdr/data/1.0.0")
	if err != nil {
		return fmt.Errorf("failed to open data stream: %w", err)
	}
	defer stream.Close()

	// Apply bandwidth throttling
	throttledStream := e.NewThrottledStream(ctx, stream)

	checksumBytes, _ := hex.DecodeString(checksumHex)
	header := make([]byte, 41)
	header[0] = OpCodePushPiece
	copy(header[1:33], checksumBytes)
	binary.BigEndian.PutUint64(header[33:41], uint64(size))

	if _, err := throttledStream.Write(header); err != nil {
		return fmt.Errorf("failed to write stream header: %w", err)
	}

	buf := e.StreamBufferPool.Get().([]byte)
	defer e.StreamBufferPool.Put(buf)

	if _, err := io.CopyBuffer(throttledStream, dataStream, buf); err != nil {
		return fmt.Errorf("failed to stream data: %w", err)
	}

	return nil
}

// PullPiece opens a raw stream to a peer and pulls the file data, returning it.
func (e *Engine) PullPiece(ctx context.Context, peerID int64, checksumHex string) ([]byte, error) {
	if e.Host == nil {
		return nil, fmt.Errorf("raw data stream unavailable (no libp2p host)")
	}

	client, err := e.GetOrDialPeer(ctx, peerID)
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer: %w", err)
	}
	defer client.Close()

	var pubKeyHex string
	err = e.DB.QueryRowContext(ctx, "SELECT public_key FROM peers WHERE id = ?", peerID).Scan(&pubKeyHex)
	if err != nil {
		return nil, err
	}
	pid, err := crypto.PeerIDFromPubKeyHex(pubKeyHex)
	if err != nil {
		return nil, err
	}

	return e.PullPieceDirect(ctx, pid, checksumHex)
}

// PullPieceDirect opens a raw stream to a specific libp2p peer and pulls the data.
func (e *Engine) PullPieceDirect(ctx context.Context, pid peer.ID, checksumHex string) ([]byte, error) {
	stream, err := e.Host.NewStream(ctx, pid, "/bdr/data/1.0.0")
	if err != nil {
		return nil, fmt.Errorf("failed to open data stream: %w", err)
	}
	defer stream.Close()

	// Apply bandwidth throttling
	throttledStream := e.NewThrottledStream(ctx, stream)

	checksumBytes, _ := hex.DecodeString(checksumHex)
	header := make([]byte, 41)
	header[0] = OpCodePullPiece
	copy(header[1:33], checksumBytes)
	// Size is ignored for Pull requests
	
	if _, err := throttledStream.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write stream header: %w", err)
	}

	// Read 8-byte size back
	sizeBuf := make([]byte, 8)
	if _, err := io.ReadFull(throttledStream, sizeBuf); err != nil {
		return nil, fmt.Errorf("failed to read response size header: %w", err)
	}
	size := binary.BigEndian.Uint64(sizeBuf)

	buf := e.StreamBufferPool.Get().([]byte)
	defer e.StreamBufferPool.Put(buf)

	data := make([]byte, size)
	if _, err := io.ReadFull(throttledStream, data); err != nil {
		return nil, fmt.Errorf("failed to read pulled data: %w", err)
	}

	actualHash := hex.EncodeToString(e.Hash(data))
	if actualHash != checksumHex {
		return nil, fmt.Errorf("checksum mismatch: expected %s, got %s", checksumHex, actualHash)
	}

	return data, nil
}

func (e *Engine) handleIncomingPushBlob(s network.Stream, checksumHex string, size uint64) {
	metaVal, ok := e.pendingClientStreams.LoadAndDelete(checksumHex)
	if !ok {
		log.Printf("DataStream: unexpected push blob for %s (not in pending streams)", checksumHex)
		return
	}
	meta := metaVal.(PendingClientBlob)

	if size != uint64(meta.Size) {
		log.Printf("DataStream: size mismatch for blob %s: expected %d, got %d", checksumHex, meta.Size, size)
		return
	}

	data := make([]byte, size)
	hasher := blake3.New(32, nil)
	tee := io.TeeReader(s, hasher)

	written, err := io.ReadFull(tee, data)
	if err != nil {
		log.Printf("DataStream: failed to stream blob %s: %v", checksumHex, err)
		return
	}

	if uint64(written) != size {
		log.Printf("DataStream: incomplete blob transfer for %s (got %d, expected %d)", checksumHex, written, size)
		return
	}

	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if actualHash != checksumHex {
		log.Printf("DataStream: checksum mismatch for blob %s (got %s)", checksumHex, actualHash)
		return
	}

	blobs := []rpc.LocalBlobData{
		{
			Hash:      checksumHex,
			Data:      data,
			IsSpecial: meta.Special,
		},
	}
	
	if err := e.IngestBlobs(context.Background(), meta.ClientPubKey, blobs, false); err != nil {
		log.Printf("DataStream: failed to ingest blob %s: %v", checksumHex, err)
	} else if e.Verbose {
		log.Printf("DataStream: successfully received and ingested blob %s", checksumHex)
	}
}

func (e *Engine) handleIncomingPullBlob(s network.Stream, checksumHex string) {
	blobs, _, err := e.GetBlobs(context.Background(), []string{checksumHex})
	if err != nil || len(blobs) == 0 {
		log.Printf("DataStream: failed to get blob %s for pull: %v", checksumHex, err)
		sizeBuf := make([]byte, 8)
		s.Write(sizeBuf)
		return
	}

	blob := blobs[0]
	sizeBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBuf, uint64(len(blob.Data)))
	if _, err := s.Write(sizeBuf); err != nil {
		log.Printf("DataStream: failed to write size header for blob pull %s: %v", checksumHex, err)
		return
	}

	if _, err := s.Write(blob.Data); err != nil {
		log.Printf("DataStream: failed to send blob %s: %v", checksumHex, err)
	} else if e.Verbose {
		log.Printf("DataStream: successfully served blob %s to client", checksumHex)
	}
}
