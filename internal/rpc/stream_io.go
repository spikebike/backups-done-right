package rpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	OpCodePush byte = 0x01
	OpCodePull byte = 0x02

	FlagTypePeerShard  byte = 0x01
	FlagTypeClientBlob byte = 0x02
)

// StreamItemHeader defines the per-item metadata in a batched stream.
type StreamItemHeader struct {
	OpCode byte
	Hash   [32]byte
	Size   uint64
	Flags  byte
}

// WriteTo serializes the header into the writer (exactly 42 bytes).
func (h *StreamItemHeader) WriteTo(w io.Writer) (int64, error) {
	buf := make([]byte, 42)
	buf[0] = h.OpCode
	copy(buf[1:33], h.Hash[:])
	binary.BigEndian.PutUint64(buf[33:41], h.Size)
	buf[41] = h.Flags
	n, err := w.Write(buf)
	return int64(n), err
}

// ReadFrom deserializes the header from the reader (exactly 42 bytes).
func (h *StreamItemHeader) ReadFrom(r io.Reader) (int64, error) {
	buf := make([]byte, 42)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return int64(n), err
	}
	h.OpCode = buf[0]
	copy(h.Hash[:], buf[1:33])
	h.Size = binary.BigEndian.Uint64(buf[33:41])
	h.Flags = buf[41]
	return int64(n), nil
}

// BatchHeader defines the top-level metadata for a batched stream.
type BatchHeader struct {
	Count    uint32
	Reserved uint32
}

func (h *BatchHeader) WriteTo(w io.Writer) (int64, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], h.Count)
	binary.BigEndian.PutUint32(buf[4:8], h.Reserved)
	n, err := w.Write(buf)
	return int64(n), err
}

func (h *BatchHeader) ReadFrom(r io.Reader) (int64, error) {
	buf := make([]byte, 8)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return int64(n), err
	}
	h.Count = binary.BigEndian.Uint32(buf[0:4])
	h.Reserved = binary.BigEndian.Uint32(buf[4:8])
	return int64(n), nil
}

// StreamItem represents a single item and its data in a batch.
type StreamItem struct {
	Header StreamItemHeader
	Data   io.Reader
}

// StreamSender handles writing a batch of items to a stream.
type StreamSender struct {
	Writer io.Writer
}

func NewStreamSender(w io.Writer) *StreamSender {
	return &StreamSender{Writer: w}
}

func (s *StreamSender) SendBatch(ctx context.Context, items []StreamItem) error {
	return s.SendBatchWithBuffer(ctx, items, nil)
}

func (s *StreamSender) SendBatchWithBuffer(ctx context.Context, items []StreamItem, buf []byte) error {
	batch := BatchHeader{Count: uint32(len(items))}
	if _, err := batch.WriteTo(s.Writer); err != nil {
		return fmt.Errorf("write batch header: %w", err)
	}

	for _, item := range items {
		if _, err := item.Header.WriteTo(s.Writer); err != nil {
			return fmt.Errorf("write item header: %w", err)
		}

		var n int64
		var err error
		if buf != nil {
			n, err = io.CopyBuffer(s.Writer, item.Data, buf)
		} else {
			n, err = io.Copy(s.Writer, item.Data)
		}

		if err != nil {
			return fmt.Errorf("write item data: %w", err)
		} else if uint64(n) != item.Header.Size {
			return fmt.Errorf("short write: sent %d but expected %d", n, item.Header.Size)
		}
	}
	return nil
}

// StreamReceiver handles reading a batch of items from a stream.
type StreamReceiver struct {
	Reader io.Reader
}

func NewStreamReceiver(r io.Reader) *StreamReceiver {
	return &StreamReceiver{Reader: r}
}

func (s *StreamReceiver) ReceiveBatch(ctx context.Context, handler func(header StreamItemHeader, r io.Reader) error) error {
	batch := BatchHeader{}
	if _, err := batch.ReadFrom(s.Reader); err != nil {
		return fmt.Errorf("read batch header: %w", err)
	}

	for i := uint32(0); i < batch.Count; i++ {
		header := StreamItemHeader{}
		if _, err := header.ReadFrom(s.Reader); err != nil {
			return fmt.Errorf("read item header: %w", err)
		}

		// Create a limited reader for this item's data to ensure the handler doesn't over-read
		lr := io.LimitReader(s.Reader, int64(header.Size))
		if err := handler(header, lr); err != nil {
			return fmt.Errorf("handle item %d: %w", i, err)
		}

		// Drain remaining bytes if handler didn't read everything
		if _, err := io.Copy(io.Discard, lr); err != nil {
			return fmt.Errorf("drain item %d: %w", i, err)
		}
	}
	return nil
}
