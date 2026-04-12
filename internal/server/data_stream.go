package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"lukechampine.com/blake3"
	"p2p-backup/internal/crypto"
	"p2p-backup/internal/rpc"
)

type PendingStreamMeta struct {
	PeerID          int64
	IsSpecial       bool
	PieceIndex      int
	ParentShardHash string
	SequenceNumber  uint64
	TotalPieces     int
	Size            uint64
}

// HandleUnifiedStream processes incoming raw data streams for /bdr/stream/1.0.0
// This is the new unified protocol that replaces /bdr/data and /bdr/upload.
func (e *Engine) HandleUnifiedStream(s network.Stream) {
	peerID := s.Conn().RemotePeer()
	defer s.Close()

	pubKeyHex, err := crypto.PubKeyHexFromPeerID(peerID)
	if err != nil {
		log.Printf("UnifiedStream: failed to extract pubkey from peer %s: %v", peerID, err)
		return
	}

	// Acquire concurrency semaphore slot
	e.streamSemaphore <- struct{}{}
	defer func() { <-e.streamSemaphore }()

	receiver := rpc.NewStreamReceiver(s)
	ctx := context.Background()

	if e.Verbose {
		log.Printf("UnifiedStream: receiving batch from %s", peerID)
	}

	var responseItems []rpc.StreamItem
	var openFiles []io.ReadCloser
	var totalInboundBytes uint64
	var totalOutboundBytes uint64

	defer func() {
		for _, f := range openFiles {
			f.Close()
		}
	}()

	err = receiver.ReceiveBatch(ctx, func(header rpc.StreamItemHeader, r io.Reader) error {
		checksumHex := hex.EncodeToString(header.Hash[:])

		switch header.OpCode {
		case rpc.OpCodePush:
			err := e.handleIncomingPush(r, pubKeyHex, checksumHex, header.Size, header.Flags)
			if err == nil {
				totalInboundBytes += header.Size
			}
			return err
		case rpc.OpCodePull:
			// Load file but don't send yet
			key := "peer_" + checksumHex
			f, err := e.BlobStore.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("pull item %s missing: %w", checksumHex, err)
			}
			
			// We need the size, use Stat
			size, err := e.BlobStore.Stat(ctx, key)
			if err != nil {
				f.Close()
				return fmt.Errorf("pull item %s stat failed: %w", checksumHex, err)
			}
			
			openFiles = append(openFiles, f)

			item := rpc.StreamItem{
				Header: rpc.StreamItemHeader{
					OpCode: rpc.OpCodePush, // Response is a push from us
					Flags:  rpc.FlagTypePeerShard,
					Size:   uint64(size),
				},
				Data: f,
			}
			copy(item.Header.Hash[:], header.Hash[:])
			responseItems = append(responseItems, item)
			return nil
		default:
			return fmt.Errorf("unknown OpCode %d", header.OpCode)
		}
	})

	var status byte = 0
	if err != nil {
		log.Printf("UnifiedStream: batch processing failed for %s: %v", peerID, err)
		status = 1 // Generic error
		if strings.Contains(err.Error(), "checksum mismatch") {
			status = 2
		} else if strings.Contains(err.Error(), "quota") || strings.Contains(err.Error(), "limit") {
			status = 3
		}
	}

	// Send an ACK once the entire batch is received successfully
	// For pulls, the response batch itself acts as the ACK.
	if len(responseItems) > 0 {
		sender := rpc.NewStreamSender(s)
		buf := e.StreamBufferPool.Get().([]byte)
		defer e.StreamBufferPool.Put(buf)

		if err := sender.SendBatchWithBuffer(ctx, responseItems, buf); err != nil {
			log.Printf("UnifiedStream: failed to send response batch to %s: %v", peerID, err)
		} else {
			for _, item := range responseItems {
				totalOutboundBytes += item.Header.Size
			}
		}
	}

	// Always send a final status byte if it's a push (no response batch needed an ACK)
	// or if we had an error.
	s.Write([]byte{status})

	if totalInboundBytes > 0 {
		_, _ = e.DB.ExecContext(ctx, "UPDATE peers SET inbound_bytes = inbound_bytes + ? WHERE public_key = ?", totalInboundBytes, pubKeyHex)
	}
	if totalOutboundBytes > 0 {
		_, _ = e.DB.ExecContext(ctx, "UPDATE peers SET outbound_bytes = outbound_bytes + ? WHERE public_key = ?", totalOutboundBytes, pubKeyHex)
	}
}

func (e *Engine) handleIncomingPush(s io.Reader, pubKeyHex, checksumHex string, size uint64, flags byte) error {
	// 1. Quota & Auth Check (if applicable)
	if flags == rpc.FlagTypeClientBlob {
		if err := e.AuthorizeAndCheckQuota(context.Background(), pubKeyHex, int64(size)); err != nil {
			return fmt.Errorf("auth/quota check failed: %w", err)
		}
	}

	buf := e.StreamBufferPool.Get().([]byte)
	defer e.StreamBufferPool.Put(buf)

	// FAST PATH: In-memory buffering for Client Blobs (usually 4MB) to eliminate double Disk I/O
	if flags == rpc.FlagTypeClientBlob && size <= uint64(len(buf)) {
		written, err := io.ReadFull(s, buf[:size])
		if err != nil && err != io.ErrUnexpectedEOF {
			return fmt.Errorf("stream read failed: %w", err)
		}
		if uint64(written) != size {
			return fmt.Errorf("incomplete transfer: got %d, expected %d", written, size)
		}

		hasher := blake3.New(32, nil)
		hasher.Write(buf[:size])
		actualHash := hex.EncodeToString(hasher.Sum(nil))
		if actualHash != checksumHex {
			return fmt.Errorf("checksum mismatch: got %s, expected %s", actualHash, checksumHex)
		}

		r := bytes.NewReader(buf[:size])
		return e.IngestItemsStreamed(context.Background(), pubKeyHex, checksumHex, size, r)
	}

	// FALLBACK (Original logic for 256MB Peer Shards):
	// 2. Stream to temporary file
	if err := os.MkdirAll(e.SpoolDir, 0755); err != nil {
		return fmt.Errorf("failed to create spool dir: %w", err)
	}
	tmpPath := filepath.Join(e.SpoolDir, fmt.Sprintf("tmp_%s", checksumHex))
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create tmp file: %w", err)
	}

	hasher := blake3.New(32, nil)
	tee := io.TeeReader(s, hasher)

	written, err := io.CopyBuffer(f, io.LimitReader(tee, int64(size)), buf)
	f.Close()
	// Drain the remaining data for this item if copy failed but size is known
	if err != nil {
		remaining := int64(size) - written
		if remaining > 0 {
			io.CopyN(io.Discard, s, remaining)
		}
		os.Remove(tmpPath)
		return fmt.Errorf("stream copy failed: %w", err)
	}

	if uint64(written) != size {
		os.Remove(tmpPath)
		return fmt.Errorf("incomplete transfer: got %d, expected %d", written, size)
	}

	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if actualHash != checksumHex {
		os.Remove(tmpPath)
		return fmt.Errorf("checksum mismatch: got %s, expected %s", actualHash, checksumHex)
	}

	// 3. Routing based on type
	if flags == rpc.FlagTypePeerShard {
		return e.finalizePeerShard(checksumHex, size, tmpPath)
	} else if flags == rpc.FlagTypeClientBlob {
		return e.finalizeClientBlob(pubKeyHex, checksumHex, size, tmpPath)
	}

	os.Remove(tmpPath)
	return fmt.Errorf("unknown flags 0x%02x", flags)
}

func (e *Engine) finalizePeerShard(checksumHex string, size uint64, tmpPath string) error {
	metaVal, ok := e.pendingInboundStreams.LoadAndDelete(checksumHex)
	if !ok {
		os.Remove(tmpPath)
		return fmt.Errorf("no pending metadata for shard %s", checksumHex)
	}
	meta := metaVal.(PendingStreamMeta)

	ctx := context.Background()
	
	f, err := os.Open(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to open tmp file for blob store: %w", err)
	}
	defer f.Close()

	if err := e.BlobStore.Put(ctx, "peer_"+checksumHex, f, int64(size)); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to put shard in blob store: %w", err)
	}
	f.Close()
	os.Remove(tmpPath)

	tx, err := e.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx,
		`INSERT INTO hosted_shards (hash, size, peer_id, is_special, piece_index, parent_shard_hash, sequence, total_pieces, ref_count)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
		 ON CONFLICT(hash, peer_id) DO UPDATE SET ref_count = ref_count + 1`,
		checksumHex, size, meta.PeerID, meta.IsSpecial, meta.PieceIndex, meta.ParentShardHash, meta.SequenceNumber, meta.TotalPieces)

	if err != nil {
		return err
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected > 0 {
		_, err = tx.ExecContext(ctx, "UPDATE peers SET current_storage_size = current_storage_size + ?, total_shards = total_shards + 1, current_shards = current_shards + 1 WHERE id = ?", size, meta.PeerID)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (e *Engine) finalizeClientBlob(pubKeyHex, checksumHex string, size uint64, tmpPath string) error {
	// Re-open tmp file as reader for IngestBlobsStreamed
	f, err := os.Open(tmpPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := e.IngestItemsStreamed(context.Background(), pubKeyHex, checksumHex, size, f); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("IngestItemsStreamed failed: %w", err)
	}

	f.Close()
	os.Remove(tmpPath)
	return nil
}


// PushPieceBatched opens a raw stream to a peer and pushes a batch of items.
func (e *Engine) PushPieceBatched(ctx context.Context, peerID int64, items []rpc.StreamItem) error {
	if e.Host == nil {
		return nil
	}

	client, err := e.GetOrDialPeer(ctx, peerID)
	if err != nil {
		return err
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

	stream, err := e.Host.NewStream(ctx, pid, "/bdr/stream/1.0.0")
	if err != nil {
		return err
	}
	defer stream.Close()

	throttledStream := e.NewThrottledStream(ctx, stream)
	sender := rpc.NewStreamSender(throttledStream)

	buf := e.StreamBufferPool.Get().([]byte)
	defer e.StreamBufferPool.Put(buf)

	if err := sender.SendBatchWithBuffer(ctx, items, buf); err != nil {
		return err
	}

	// Read Status Byte
	ack := make([]byte, 1)
	if _, err := io.ReadFull(throttledStream, ack); err != nil {
		return fmt.Errorf("failed to read response status: %w", err)
	}

	if ack[0] != 0 {
		return fmt.Errorf("remote batch processing failed (status %d)", ack[0])
	}

	var totalOutboundBytes uint64
	for _, item := range items {
		totalOutboundBytes += item.Header.Size
	}
	if totalOutboundBytes > 0 {
		_, _ = e.DB.ExecContext(ctx, "UPDATE peers SET outbound_bytes = outbound_bytes + ? WHERE public_key = ?", totalOutboundBytes, pubKeyHex)
	}

	return nil
}

// PullPiece opens a raw stream to a peer and pulls the file data, writing it to out.
func (e *Engine) PullPiece(ctx context.Context, peerID int64, checksumHex string, out io.Writer) error {
	if e.Host == nil {
		return fmt.Errorf("raw data stream unavailable (no libp2p host)")
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

	return e.PullPieceRaw(ctx, pid, checksumHex, out)
}

// PullPieceRaw opens a raw stream to a specific libp2p peer and pulls the data, writing it to out.
func (e *Engine) PullPieceRaw(ctx context.Context, pid peer.ID, checksumHex string, out io.Writer) error {
	stream, err := e.Host.NewStream(ctx, pid, "/bdr/stream/1.0.0")
	if err != nil {
		return fmt.Errorf("failed to open data stream: %w", err)
	}
	defer stream.Close()

	// Apply bandwidth throttling
	throttledStream := e.NewThrottledStream(ctx, stream)

	// Single-item batch for Pull
	sender := rpc.NewStreamSender(throttledStream)
	checksumBytes, _ := hex.DecodeString(checksumHex)
	item := rpc.StreamItem{
		Header: rpc.StreamItemHeader{
			OpCode: rpc.OpCodePull,
			Flags:  rpc.FlagTypePeerShard,
			Size:   0, // Pull request has no data
		},
		Data: bytes.NewReader(nil),
	}
	copy(item.Header.Hash[:], checksumBytes)

	if err := sender.SendBatch(ctx, []rpc.StreamItem{item}); err != nil {
		return fmt.Errorf("failed to send pull request: %w", err)
	}

	// Read batch response back
	receiver := rpc.NewStreamReceiver(throttledStream)
	var totalInboundBytes uint64
	err = receiver.ReceiveBatch(ctx, func(header rpc.StreamItemHeader, r io.Reader) error {
		// Verify hash
		receivedHash := hex.EncodeToString(header.Hash[:])
		if receivedHash != checksumHex {
			return fmt.Errorf("checksum mismatch in response: expected %s, got %s", checksumHex, receivedHash)
		}

		hasher := blake3.New(32, nil)
		tee := io.TeeReader(r, hasher)

		buf := e.StreamBufferPool.Get().([]byte)
		defer e.StreamBufferPool.Put(buf)

		_, err := io.CopyBuffer(out, tee, buf)
		if err != nil {
			return err
		}

		actualHash := hex.EncodeToString(hasher.Sum(nil))
		if actualHash != checksumHex {
			return fmt.Errorf("payload checksum mismatch: expected %s, got %s", checksumHex, actualHash)
		}

		totalInboundBytes += header.Size
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to receive pull response batch: %w", err)
	}

	// Read Status Byte
	ack := make([]byte, 1)
	if _, err := io.ReadFull(throttledStream, ack); err != nil {
		return fmt.Errorf("failed to read response status: %w", err)
	}

	if ack[0] != 0 {
		return fmt.Errorf("remote pull processing failed (status %d)", ack[0])
	}

	if totalInboundBytes > 0 {
		pubKeyHex, _ := crypto.PubKeyHexFromPeerID(pid)
		_, _ = e.DB.ExecContext(ctx, "UPDATE peers SET inbound_bytes = inbound_bytes + ? WHERE public_key = ?", totalInboundBytes, pubKeyHex)
	}

	return nil
}
