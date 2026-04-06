package rpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
)

func TestStreamSenderReceiver(t *testing.T) {
	ctx := context.Background()
	buf := new(bytes.Buffer)

	items := []StreamItem{
		{
			Header: StreamItemHeader{
				OpCode: OpCodePush,
				Size:   5,
				Flags:  FlagTypeClientBlob,
			},
			Data: bytes.NewReader([]byte("hello")),
		},
		{
			Header: StreamItemHeader{
				OpCode: OpCodePush,
				Size:   6,
				Flags:  FlagTypePeerShard,
			},
			Data: bytes.NewReader([]byte("world!")),
		},
	}
	copy(items[0].Header.Hash[:], "hash1")
	copy(items[1].Header.Hash[:], "hash2")

	// Test SendBatch
	sender := NewStreamSender(buf)
	if err := sender.SendBatch(ctx, items); err != nil {
		t.Fatalf("SendBatch failed: %v", err)
	}

	// Test ReceiveBatch
	receivedCount := 0
	receiver := NewStreamReceiver(buf)
	err := receiver.ReceiveBatch(ctx, func(header StreamItemHeader, r io.Reader) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}

		if string(data) != string([]byte("hello")[0:0]) && receivedCount == 0 {
			// wait, io.ReadAll should get "hello"
		}

		expectedData := []string{"hello", "world!"}[receivedCount]
		if string(data) != expectedData {
			t.Errorf("item %d: expected data %q, got %q", receivedCount, expectedData, string(data))
		}

		expectedHash := [32]byte{}
		copy(expectedHash[:], []string{"hash1", "hash2"}[receivedCount])
		if header.Hash != expectedHash {
			t.Errorf("item %d: hash mismatch", receivedCount)
		}

		receivedCount++
		return nil
	})

	if err != nil {
		t.Fatalf("ReceiveBatch failed: %v", err)
	}

	if receivedCount != 2 {
		t.Errorf("expected 2 items, got %d", receivedCount)
	}
}

func TestStreamSenderWithBuffer(t *testing.T) {
	ctx := context.Background()
	buf := new(bytes.Buffer)
	copyBuf := make([]byte, 1024)

	items := []StreamItem{
		{
			Header: StreamItemHeader{
				OpCode: OpCodePush,
				Size:   11,
				Flags:  FlagTypeClientBlob,
			},
			Data: bytes.NewReader([]byte("buffered io")),
		},
	}

	sender := NewStreamSender(buf)
	if err := sender.SendBatchWithBuffer(ctx, items, copyBuf); err != nil {
		t.Fatalf("SendBatchWithBuffer failed: %v", err)
	}

	receiver := NewStreamReceiver(buf)
	err := receiver.ReceiveBatch(ctx, func(header StreamItemHeader, r io.Reader) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if string(data) != "buffered io" {
			t.Errorf("expected %q, got %q", "buffered io", string(data))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ReceiveBatch failed: %v", err)
	}
}

func TestPullBatchCycle(t *testing.T) {
	ctx := context.Background()
	clientToServer := new(bytes.Buffer)
	serverToClient := new(bytes.Buffer)

	// --- 1. Client Sends Request Batch ---
	requestItems := []StreamItem{
		{
			Header: StreamItemHeader{
				OpCode: OpCodePull,
				Size:   0,
				Flags:  FlagTypePeerShard,
			},
			Data: bytes.NewReader(nil),
		},
	}
	copy(requestItems[0].Header.Hash[:], "wanted-hash")

	clientSender := NewStreamSender(clientToServer)
	if err := clientSender.SendBatch(ctx, requestItems); err != nil {
		t.Fatalf("Client send request failed: %v", err)
	}

	// --- 2. Server Receives Request Batch & Sends Response Batch ---
	serverReceiver := NewStreamReceiver(clientToServer)
	var serverResponseItems []StreamItem

	err := serverReceiver.ReceiveBatch(ctx, func(header StreamItemHeader, r io.Reader) error {
		if header.OpCode != OpCodePull {
			return fmt.Errorf("expected pull opcode")
		}

		// Mock finding the data
		data := []byte("found-it")
		respItem := StreamItem{
			Header: StreamItemHeader{
				OpCode: OpCodePush,
				Size:   uint64(len(data)),
				Flags:  FlagTypePeerShard,
			},
			Data: bytes.NewReader(data),
		}
		copy(respItem.Header.Hash[:], header.Hash[:])
		serverResponseItems = append(serverResponseItems, respItem)
		return nil
	})
	if err != nil {
		t.Fatalf("Server receive request failed: %v", err)
	}

	serverSender := NewStreamSender(serverToClient)
	if err := serverSender.SendBatch(ctx, serverResponseItems); err != nil {
		t.Fatalf("Server send response failed: %v", err)
	}

	// --- 3. Client Receives Response Batch ---
	clientReceiver := NewStreamReceiver(serverToClient)
	receivedCount := 0
	err = clientReceiver.ReceiveBatch(ctx, func(header StreamItemHeader, r io.Reader) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if string(data) != "found-it" {
			t.Errorf("expected 'found-it', got %q", string(data))
		}
		receivedCount++
		return nil
	})

	if err != nil {
		t.Fatalf("Client receive response failed: %v", err)
	}
	if receivedCount != 1 {
		t.Errorf("expected 1 response item, got %d", receivedCount)
	}
}
