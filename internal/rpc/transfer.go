package rpc

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	"lukechampine.com/blake3"
)

// WriteFrame handles magic bytes + header + data copy for a single item.
func WriteFrame(w io.Writer, hash string, size int64, reader io.Reader, pool *sync.Pool) error {
	// 1. Write Magic Sync
	if _, err := w.Write(MagicSync); err != nil {
		return fmt.Errorf("failed to write sync bytes: %w", err)
	}

	// 2. Write Header: [Checksum (32b)][Size (8b)]
	hashBytes, err := hex.DecodeString(hash)
	if err != nil {
		return fmt.Errorf("invalid hash: %w", err)
	}
	header := make([]byte, 40)
	copy(header[0:32], hashBytes)
	binary.BigEndian.PutUint64(header[32:40], uint64(size))

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("failed to write frame header: %w", err)
	}

	// 3. Copy Data using pool
	buf := pool.Get().([]byte)
	defer pool.Put(buf)

	written, err := io.CopyBuffer(w, reader, buf)
	if err != nil {
		return fmt.Errorf("failed to stream frame data: %w", err)
	}
	if written != size {
		return fmt.Errorf("frame size mismatch: wrote %d, expected %d", written, size)
	}

	return nil
}

// ReadFrame reads and validates a single frame from the stream.
// It returns the data bytes if successful. Note: Use for small frames (e.g. 4MB blobs).
func ReadFrame(r io.Reader, expectedHash string, expectedSize int64, pool *sync.Pool) ([]byte, error) {
	header, err := ReadFrameHeader(r)
	if err != nil {
		return nil, err
	}

	if expectedHash != "" && header.Hash != expectedHash {
		return nil, fmt.Errorf("checksum mismatch in header: expected %s, got %s", expectedHash, header.Hash)
	}
	if expectedSize >= 0 && header.Size != expectedSize {
		return nil, fmt.Errorf("size mismatch in header: expected %d, got %d", expectedSize, header.Size)
	}

	// 3. Stream data into memory
	data := make([]byte, header.Size)
	hasher := blake3.New(32, nil)
	tee := io.TeeReader(r, hasher)

	if _, err := io.ReadFull(tee, data); err != nil {
		return nil, fmt.Errorf("failed to read frame data: %w", err)
	}

	if hex.EncodeToString(hasher.Sum(nil)) != header.Hash {
		return nil, fmt.Errorf("checksum validation failed for frame %s", header.Hash)
	}

	return data, nil
}

// FrameHeader represents the parsed metadata of a frame.
type FrameHeader struct {
	Hash string
	Size int64
}

// ReadFrameHeader reads the magic bytes and the 40-byte header.
func ReadFrameHeader(r io.Reader) (FrameHeader, error) {
	// 1. Read Magic Sync
	magic := make([]byte, 2)
	if _, err := io.ReadFull(r, magic); err != nil {
		return FrameHeader{}, fmt.Errorf("failed to read magic sync: %w", err)
	}
	if magic[0] != MagicSync[0] || magic[1] != MagicSync[1] {
		return FrameHeader{}, fmt.Errorf("invalid magic sync: got %x", magic)
	}

	// 2. Read Header: [Checksum (32b)][Size (8b)]
	header := make([]byte, 40)
	if _, err := io.ReadFull(r, header); err != nil {
		return FrameHeader{}, fmt.Errorf("failed to read frame header: %w", err)
	}
	checksumHex := hex.EncodeToString(header[0:32])
	size := int64(binary.BigEndian.Uint64(header[32:40]))

	return FrameHeader{Hash: checksumHex, Size: size}, nil
}

// CopyFrame reads a frame header and streams the data directly to a writer.
// Use this for large frames (e.g. 256MB shards) to avoid memory spikes.
func CopyFrame(r io.Reader, w io.Writer, pool *sync.Pool) (FrameHeader, error) {
	header, err := ReadFrameHeader(r)
	if err != nil {
		return header, err
	}

	hasher := blake3.New(32, nil)
	tee := io.TeeReader(r, hasher)

	buf := pool.Get().([]byte)
	defer pool.Put(buf)

	written, err := io.CopyBuffer(w, io.LimitReader(tee, header.Size), buf)
	if err != nil {
		return header, fmt.Errorf("failed to stream frame data: %w", err)
	}

	if written != header.Size {
		return header, fmt.Errorf("frame incomplete: got %d, expected %d", written, header.Size)
	}

	if hex.EncodeToString(hasher.Sum(nil)) != header.Hash {
		return header, fmt.Errorf("checksum validation failed for frame %s", header.Hash)
	}

	return header, nil
}

// WriteBatchHeader writes the batch opcode and count.
func WriteBatchHeader(w io.Writer, opCode byte, count uint32) error {
	header := make([]byte, 5)
	header[0] = opCode
	binary.BigEndian.PutUint32(header[1:5], count)
	_, err := w.Write(header)
	return err
}

// ReadBatchHeader reads the batch count.
func ReadBatchHeader(r io.Reader) (uint32, error) {
	countBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, countBuf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(countBuf), nil
}
