package client

import (
	"context"
	"fmt"
	"p2p-backup/internal/rpc"
	"p2p-backup/internal/server"
)

// PeerMeta represents a discovered networked Swarm Node securely.
type PeerMeta struct {
	ID                  int64
	IPAddress           string
	PublicKey           string
	Status              string
	StorageLimitGB      int
	CurrentStorageSize  int64
	OutboundStorageSize int64
	ContactInfo         string
	TotalShards         uint64
	CurrentShards       uint64
	FirstSeen           string
	LastSeen            string
	ChallengesMade      uint32
	ChallengesPassed    uint32
	ConnectionsOk       uint32
	IntegrityAttempts   uint32
}

// ClientMeta represents a client authorized to use the server.
type ClientMeta struct {
	ID                 uint64
	PublicKey          string
	Status             string
	LastSeen           string
	MaxStorageSize     uint64
	CurrentStorageSize uint64
}

// RPCClient defines the interface for communicating with the backup server.
type RPCClient interface {
	OfferBlobs(ctx context.Context, blobs []rpc.BlobMeta) ([]uint32, error)
	PrepareUploadClient(ctx context.Context, blobs []rpc.BlobMeta) error
	PushBlob(ctx context.Context, hashHex string, data []byte) error
	PushBlobBatch(ctx context.Context, jobs []rpc.UploadJob) error
	ListSpecialBlobs(ctx context.Context) ([]rpc.BlobMeta, error)
	GetStatus(ctx context.Context) (rpc.StatusInfo, error)
	PullBlob(ctx context.Context, hashHex string) ([]byte, error)
	PullBlobBatch(ctx context.Context, hashes []string) ([]rpc.LocalBlobData, error)
	DeleteBlobs(ctx context.Context, hashes []string) error
	ListAllBlobs(ctx context.Context) ([]string, error)
	AddPeer(ctx context.Context, address string) error
	UpdatePeer(ctx context.Context, id int64, status string, maxGB uint64) error
	ListPeers(ctx context.Context) ([]PeerMeta, error)
	ListClients(ctx context.Context) ([]ClientMeta, error)
	AddClient(ctx context.Context, publicKey string, status string, maxGB uint64) error
	UpdateClient(ctx context.Context, id uint64, status string, maxGB uint64) error
	Close() error
}

type MockRPCClient struct {
	engine interface {
		OfferBlobs(ctx context.Context, clientPubKey string, blobs []rpc.BlobMeta) ([]uint32, error)
		PrepareUpload(ctx context.Context, clientPubKey string, metas []rpc.PendingStreamMeta) error
		IngestBlobs(ctx context.Context, clientPubKey string, blobs []rpc.LocalBlobData, isGC bool) error
		ListSpecialBlobs(ctx context.Context) ([]rpc.BlobMeta, error)
		GetStatus(ctx context.Context) (rpc.StatusInfo, error)
		GetBlobs(ctx context.Context, hashes []string) ([]rpc.LocalBlobData, []string, error)
		DeleteBlobs(ctx context.Context, hashes []string) error
		ListAllBlobs(ctx context.Context) ([]string, error)
		AddPeer(ctx context.Context, address string) error
		UpdatePeer(ctx context.Context, id int64, status string, maxStorageBytes int64) error
		ListPeers(ctx context.Context) ([]server.PeerDBInfo, error)
		ListClients(ctx context.Context) ([]server.ClientDBInfo, error)
		AddClient(ctx context.Context, callerPubKey string, clientPubKey string, status string, maxStorageBytes uint64) error
		UpdateClient(ctx context.Context, callerPubKey string, id uint64, status string, maxStorageBytes uint64) error
	}
}

func NewMockRPCClient(engine interface {
	OfferBlobs(ctx context.Context, clientPubKey string, blobs []rpc.BlobMeta) ([]uint32, error)
	PrepareUpload(ctx context.Context, clientPubKey string, metas []rpc.PendingStreamMeta) error
	IngestBlobs(ctx context.Context, clientPubKey string, blobs []rpc.LocalBlobData, isGC bool) error
	ListSpecialBlobs(ctx context.Context) ([]rpc.BlobMeta, error)
	GetStatus(ctx context.Context) (rpc.StatusInfo, error)
	GetBlobs(ctx context.Context, hashes []string) ([]rpc.LocalBlobData, []string, error)
	DeleteBlobs(ctx context.Context, hashes []string) error
	ListAllBlobs(ctx context.Context) ([]string, error)
	AddPeer(ctx context.Context, address string) error
	UpdatePeer(ctx context.Context, id int64, status string, maxStorageBytes int64) error
	ListPeers(ctx context.Context) ([]server.PeerDBInfo, error)
	ListClients(ctx context.Context) ([]server.ClientDBInfo, error)
	AddClient(ctx context.Context, callerPubKey string, clientPubKey string, status string, maxStorageBytes uint64) error
	UpdateClient(ctx context.Context, callerPubKey string, id uint64, status string, maxStorageBytes uint64) error
}) *MockRPCClient {
	return &MockRPCClient{engine: engine}
}

func (m *MockRPCClient) OfferBlobs(ctx context.Context, blobs []rpc.BlobMeta) ([]uint32, error) {
	if m.engine != nil {
		return m.engine.OfferBlobs(ctx, "insecure-local-client", blobs)
	}
	// Mock: Server needs all blobs
	needed := make([]uint32, len(blobs))
	for i := range blobs {
		needed[i] = uint32(i)
	}
	return needed, nil
}

func (m *MockRPCClient) PrepareUploadClient(ctx context.Context, blobs []rpc.BlobMeta) error {
	if m.engine != nil {
		var metas []rpc.PendingStreamMeta
		// In mock, we can just pass the data directly in PushBlob
		return m.engine.PrepareUpload(ctx, "insecure-local-client", metas)
	}
	return nil
}

func (m *MockRPCClient) PushBlob(ctx context.Context, hashHex string, data []byte) error {
	if m.engine != nil {
		blobs := []rpc.LocalBlobData{{Hash: hashHex, Data: data}}
		return m.engine.IngestBlobs(ctx, "insecure-local-client", blobs, false)
	}
	return nil
}

func (m *MockRPCClient) PushBlobBatch(ctx context.Context, jobs []rpc.UploadJob) error {
	if m.engine != nil {
		var blobs []rpc.LocalBlobData
		for _, j := range jobs {
			blobs = append(blobs, rpc.LocalBlobData{Hash: j.Hash, Data: j.Data})
		}
		return m.engine.IngestBlobs(ctx, "insecure-local-client", blobs, false)
	}
	return nil
}

func (m *MockRPCClient) PullBlob(ctx context.Context, hashHex string) ([]byte, error) {
	if m.engine != nil {
		blobs, _, err := m.engine.GetBlobs(ctx, []string{hashHex})
		if err != nil || len(blobs) == 0 {
			return nil, fmt.Errorf("blob not found")
		}
		return blobs[0].Data, nil
	}
	return nil, fmt.Errorf("mock error: engine not set")
}

func (m *MockRPCClient) PullBlobBatch(ctx context.Context, hashes []string) ([]rpc.LocalBlobData, error) {
	if m.engine != nil {
		blobs, _, err := m.engine.GetBlobs(ctx, hashes)
		if err != nil {
			return nil, err
		}
		return blobs, nil
	}
	return nil, fmt.Errorf("mock error: engine not set")
}

func (m *MockRPCClient) ListSpecialBlobs(ctx context.Context) ([]rpc.BlobMeta, error) {
	if m.engine != nil {
		return m.engine.ListSpecialBlobs(ctx)
	}
	return nil, nil
}

func (m *MockRPCClient) GetStatus(ctx context.Context) (rpc.StatusInfo, error) {
	if m.engine != nil {
		return m.engine.GetStatus(ctx)
	}
	return rpc.StatusInfo{}, nil
}

func (m *MockRPCClient) DeleteBlobs(ctx context.Context, hashes []string) error {
	if m.engine != nil {
		return m.engine.DeleteBlobs(ctx, hashes)
	}
	return nil
}

func (m *MockRPCClient) ListAllBlobs(ctx context.Context) ([]string, error) {
	if m.engine != nil {
		return m.engine.ListAllBlobs(ctx)
	}
	return nil, nil
}

func (m *MockRPCClient) AddPeer(ctx context.Context, address string) error {
	if m.engine != nil {
		return m.engine.AddPeer(ctx, address)
	}
	return nil
}

func (m *MockRPCClient) UpdatePeer(ctx context.Context, id int64, status string, maxGB uint64) error {
	if m.engine != nil {
		return m.engine.UpdatePeer(ctx, id, status, int64(maxGB)*1024*1024*1024)
	}
	return nil
}

func (m *MockRPCClient) ListPeers(ctx context.Context) ([]PeerMeta, error) {
	if m.engine != nil {
		dbPeers, err := m.engine.ListPeers(ctx)
		if err != nil {
			return nil, err
		}
		var peers []PeerMeta
		for _, p := range dbPeers {
			peers = append(peers, PeerMeta{
				ID:                  p.ID,
				IPAddress:           p.Address,
				PublicKey:           p.PublicKey,
				Status:              p.Status,
				StorageLimitGB:      int(p.MaxStorageSize / (1024 * 1024 * 1024)),
				CurrentStorageSize:  p.CurrentStorageSize,
				OutboundStorageSize: p.OutboundStorageSize,
				ContactInfo:         p.ContactInfo,
				TotalShards:         p.TotalShards,
				CurrentShards:       p.CurrentShards,
				FirstSeen:           p.FirstSeen,
				LastSeen:            p.LastSeen,
				ChallengesMade:      p.ChallengesMade,
				ChallengesPassed:    p.ChallengesPassed,
				ConnectionsOk:       p.ConnectionsOk,
				IntegrityAttempts:   p.IntegrityAttempts,
			})
		}
		return peers, nil
	}
	return nil, nil
}

func (m *MockRPCClient) ListClients(ctx context.Context) ([]ClientMeta, error) {
	if m.engine != nil {
		dbClients, err := m.engine.ListClients(ctx)
		if err != nil {
			return nil, err
		}
		var clients []ClientMeta
		for _, c := range dbClients {
			clients = append(clients, ClientMeta{
				ID:                 c.ID,
				PublicKey:          c.PublicKey,
				Status:             c.Status,
				LastSeen:           c.LastSeen,
				MaxStorageSize:     c.MaxStorageSize,
				CurrentStorageSize: c.CurrentStorageSize,
			})
		}
		return clients, nil
	}
	return nil, nil
}

func (m *MockRPCClient) AddClient(ctx context.Context, publicKey string, status string, maxGB uint64) error {
	if m.engine != nil {
		return m.engine.AddClient(ctx, "mock-admin", publicKey, status, maxGB*1024*1024*1024)
	}
	return nil
}

func (m *MockRPCClient) UpdateClient(ctx context.Context, id uint64, status string, maxGB uint64) error {
	if m.engine != nil {
		return m.engine.UpdateClient(ctx, "mock-admin", id, status, maxGB*1024*1024*1024)
	}
	return nil
}

func (m *MockRPCClient) Close() error {
	return nil
}
