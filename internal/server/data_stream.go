package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"p2p-backup/internal/crypto"
	"p2p-backup/internal/rpc"
)

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
	case rpc.OpCodePush:
		e.handleIncomingPush(s)
	case rpc.OpCodePull:
		// Pull request is sent as a frame with 0 size
		header, err := rpc.ReadFrameHeader(s)
		if err != nil {
			log.Printf("DataStream: failed to read pull header from %s: %v", peerID, err)
			return
		}
		e.handleIncomingPull(s, header.Hash)
	case rpc.OpCodePushBatch:
		e.handleIncomingPushBatch(s)
	case rpc.OpCodePullBatch:
		e.handleIncomingPullBatch(s)
	default:
		log.Printf("DataStream: unknown OpCode %d from %s", opCode, peerID)
	}
}

// handleIncomingPush handles a single incoming frame and decides if it's a Piece or a Blob.
func (e *Engine) handleIncomingPush(s network.Stream) {
	peerID := s.Conn().RemotePeer()

	// Use temporary file for all single pushes to protect against memory spikes
	tmpPath := filepath.Join(e.BlobStoreDir, fmt.Sprintf("push_%s_%d.tmp", peerID.String(), time.Now().UnixNano()))
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("DataStream: failed to create tmp file for push: %v", err)
		return
	}
	defer f.Close()

	// 1. Read and stream data to disk using CopyFrame
	header, err := rpc.CopyFrame(s, f, &e.StreamBufferPool)
	if err != nil {
		log.Printf("DataStream: failed to receive frame from %s: %v", peerID, err)
		os.Remove(tmpPath)
		return
	}
	f.Close()

	// 2. Check which map it belongs to (Piece or Blob)
	if metaVal, ok := e.pendingInboundStreams.LoadAndDelete(header.Hash); ok {
		// --- DESTINATION: Peer hosted_shard ---
		meta := metaVal.(rpc.PendingStreamMeta)
		
		finalPath := filepath.Join(e.BlobStoreDir, "peer_"+header.Hash)
		if err := os.Rename(tmpPath, finalPath); err != nil {
			os.Remove(tmpPath)
			return
		}

		ctx := context.Background()
		_, err = e.DB.ExecContext(ctx, 
			"INSERT OR IGNORE INTO hosted_shards (hash, size, peer_id, is_special, piece_index, parent_shard_hash, sequence, total_pieces) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			header.Hash, header.Size, meta.PeerID, meta.IsSpecial, meta.PieceIndex, meta.ParentShardHash, meta.SequenceNumber, meta.TotalPieces)
		
		if err == nil {
			_, _ = e.DB.ExecContext(ctx, "UPDATE peers SET current_storage_size = current_storage_size + ?, total_shards = total_shards + 1, current_shards = current_shards + 1 WHERE id = ?", header.Size, meta.PeerID)
		}

		if e.Verbose {
			log.Printf("DataStream: successfully stored piece %s from peer %s", header.Hash, peerID)
		}

	} else if metaVal, ok := e.pendingClientStreams.LoadAndDelete(header.Hash); ok {
		// --- DESTINATION: Client ingestion pipeline ---
		meta := metaVal.(rpc.PendingClientBlob)

		data, err := os.ReadFile(tmpPath)
		os.Remove(tmpPath)
		if err != nil {
			return
		}

		blobs := []rpc.LocalBlobData{{Hash: header.Hash, Data: data, IsSpecial: meta.Special}}
		if err := e.IngestBlobs(context.Background(), meta.ClientPubKey, blobs, false); err != nil {
			log.Printf("DataStream: ingest failed for %s: %v", header.Hash, err)
		} else if e.Verbose {
			log.Printf("DataStream: successfully ingested blob %s from client %s", header.Hash, peerID)
		}

	} else {
		log.Printf("DataStream: unexpected push %s (not in any pending map) from %s.", header.Hash, peerID)
		os.Remove(tmpPath)
	}
}

// handleIncomingPull decides if it should serve a Piece or a Blob.
func (e *Engine) handleIncomingPull(s network.Stream, hashHex string) {
	peerID := s.Conn().RemotePeer()

	// 1. Check if we are hosting this as a piece for another peer
	filePath := filepath.Join(e.BlobStoreDir, "peer_"+hashHex)
	if f, err := os.Open(filePath); err == nil {
		defer f.Close()
		stat, _ := f.Stat()
		rpc.WriteFrame(s, hashHex, stat.Size(), f, &e.StreamBufferPool)
		return
	}

	// 2. Check if it's in our local shards (client blobs)
	blobs, _, err := e.GetBlobs(context.Background(), []string{hashHex})
	if err == nil && len(blobs) > 0 {
		blob := blobs[0]
		rpc.WriteFrame(s, hashHex, int64(len(blob.Data)), bytes.NewReader(blob.Data), &e.StreamBufferPool)
		return
	}

	// 3. Not found
	log.Printf("DataStream: requested pull %s not found for %s", hashHex, peerID)
	// Framing requires we send something or close the stream. Send a 0-size frame.
	s.Write(rpc.MagicSync)
	emptyHeader := make([]byte, 40)
	s.Write(emptyHeader)
}

func (e *Engine) handleIncomingPullBatch(s network.Stream) {
	count, err := rpc.ReadBatchHeader(s)
	if err != nil {
		return
	}

	var successCount uint32
	for i := uint32(0); i < count; i++ {
		checksumHeader := make([]byte, 32)
		if _, err := io.ReadFull(s, checksumHeader); err != nil {
			break
		}
		checksumHex := hex.EncodeToString(checksumHeader)
		e.handleIncomingPull(s, checksumHex)
		successCount++
	}

	ackBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(ackBuf, successCount)
	s.Write(ackBuf)
}

func (e *Engine) handleIncomingPushBatch(s network.Stream) {
	count, err := rpc.ReadBatchHeader(s)
	if err != nil {
		return
	}

	var successCount uint32
	var ingestedBlobs []rpc.LocalBlobData
	var clientPubKey string

	for i := uint32(0); i < count; i++ {
		data, err := rpc.ReadFrame(s, "", -1, &e.StreamBufferPool)
		if err != nil {
			break
		}
		checksumHex := hex.EncodeToString(crypto.Hash(data))

		if metaVal, ok := e.pendingClientStreams.LoadAndDelete(checksumHex); ok {
			meta := metaVal.(rpc.PendingClientBlob)
			clientPubKey = meta.ClientPubKey
			ingestedBlobs = append(ingestedBlobs, rpc.LocalBlobData{
				Hash:      checksumHex,
				Data:      data,
				IsSpecial: meta.Special,
			})
			successCount++
		}
	}

	if len(ingestedBlobs) > 0 {
		e.IngestBlobs(context.Background(), clientPubKey, ingestedBlobs, false)
	}

	ackBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(ackBuf, successCount)
	s.Write(ackBuf)
}

// SendData is the unified method for pushing raw data over a libp2p stream.
func (e *Engine) SendData(ctx context.Context, pid peer.ID, opCode byte, hash string, reader io.Reader, size int64) error {
	stream, err := e.Host.NewStream(ctx, pid, "/bdr/data/1.0.0")
	if err != nil {
		return fmt.Errorf("failed to open data stream: %w", err)
	}
	defer stream.Close()

	throttledStream := e.NewThrottledStream(ctx, stream)

	if _, err := throttledStream.Write([]byte{opCode}); err != nil {
		return fmt.Errorf("failed to write opCode: %w", err)
	}

	if err := rpc.WriteFrame(throttledStream, hash, size, reader, &e.StreamBufferPool); err != nil {
		return fmt.Errorf("failed to write frame: %w", err)
	}

	return nil
}

// PushPiece opens a raw stream to a peer and pushes the file data.
func (e *Engine) PushPiece(ctx context.Context, peerID int64, dataStream io.Reader, size int64, checksumHex string) error {
	if e.Host == nil {
		return nil
	}

	var pubKeyHex string
	err := e.DB.QueryRowContext(ctx, "SELECT public_key FROM peers WHERE id = ?", peerID).Scan(&pubKeyHex)
	if err != nil {
		return err
	}
	pid, err := crypto.PeerIDFromPubKeyHex(pubKeyHex)
	if err != nil {
		return err
	}

	return e.SendData(ctx, pid, rpc.OpCodePush, checksumHex, dataStream, size)
}

// PullPiece opens a raw stream to a peer and pulls the file data, returning it.
func (e *Engine) PullPiece(ctx context.Context, peerID int64, checksumHex string) ([]byte, error) {
	if e.Host == nil {
		return nil, fmt.Errorf("raw data stream unavailable (no libp2p host)")
	}

	var pubKeyHex string
	err := e.DB.QueryRowContext(ctx, "SELECT public_key FROM peers WHERE id = ?", peerID).Scan(&pubKeyHex)
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

	throttledStream := e.NewThrottledStream(ctx, stream)

	if _, err := throttledStream.Write([]byte{rpc.OpCodePull}); err != nil {
		return nil, fmt.Errorf("failed to write opCode: %w", err)
	}

	// Send pull request as a frame with 0 size
	if _, err := throttledStream.Write(rpc.MagicSync); err != nil {
		return nil, err
	}
	header := make([]byte, 40)
	hashBytes, _ := hex.DecodeString(checksumHex)
	copy(header[0:32], hashBytes)
	if _, err := throttledStream.Write(header); err != nil {
		return nil, err
	}

	// Response should be a standard frame
	data, err := rpc.ReadFrame(throttledStream, checksumHex, -1, &e.StreamBufferPool)
	if err != nil {
		return nil, fmt.Errorf("failed to read frame: %w", err)
	}

	return data, nil
}
