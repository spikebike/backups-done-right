package client

import (
	"context"
	"encoding/hex"
	"io"

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
	InboundBytes        int64
	OutboundBytes       int64
	ContactInfo         string
	Source              string
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
	OfferItems(ctx context.Context, items []rpc.Metadata) (rpc.UploadPlan, error)
	UploadItemsStreamed(ctx context.Context, items []rpc.StreamItem) error
	ListSpecialItems(ctx context.Context) ([]rpc.Metadata, error)
	GetStatus(ctx context.Context) (rpc.StatusInfo, error)
	DownloadItems(ctx context.Context, hashes []string) ([]rpc.ItemData, []string, error)
	DeleteItems(ctx context.Context, hashes []string) error
	ListAllItems(ctx context.Context) ([]string, error)
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
		OfferItems(ctx context.Context, pubKey string, items []rpc.Metadata) (rpc.UploadPlan, error)
		IngestItemsStreamed(ctx context.Context, clientPubKey string, hash string, size uint64, r io.Reader) error
		ListSpecialItems(ctx context.Context, pubKey string) ([]rpc.Metadata, error)
		GetStatus(ctx context.Context) (rpc.StatusInfo, error)
		GetItems(ctx context.Context, hashes []string) ([]rpc.ItemData, []string, error)
		DeleteItems(ctx context.Context, hashes []string) error
		ListAllItems(ctx context.Context) ([]string, error)
		AddPeer(ctx context.Context, address string) error
		UpdatePeer(ctx context.Context, id int64, status string, maxStorageBytes int64) error
		ListPeers(ctx context.Context) ([]server.PeerDBInfo, error)
		ListClients(ctx context.Context) ([]server.ClientDBInfo, error)
		AddClient(ctx context.Context, callerPubKey string, clientPubKey string, status string, maxStorageBytes uint64) error
		UpdateClient(ctx context.Context, callerPubKey string, id uint64, status string, maxStorageBytes uint64) error
	}
}

func NewMockRPCClient(engine interface {
	OfferItems(ctx context.Context, pubKey string, items []rpc.Metadata) (rpc.UploadPlan, error)
	IngestItemsStreamed(ctx context.Context, clientPubKey string, hash string, size uint64, r io.Reader) error
	ListSpecialItems(ctx context.Context, pubKey string) ([]rpc.Metadata, error)
	GetStatus(ctx context.Context) (rpc.StatusInfo, error)
	GetItems(ctx context.Context, hashes []string) ([]rpc.ItemData, []string, error)
	DeleteItems(ctx context.Context, hashes []string) error
	ListAllItems(ctx context.Context) ([]string, error)
	AddPeer(ctx context.Context, address string) error
	UpdatePeer(ctx context.Context, id int64, status string, maxStorageBytes int64) error
	ListPeers(ctx context.Context) ([]server.PeerDBInfo, error)
	ListClients(ctx context.Context) ([]server.ClientDBInfo, error)
	AddClient(ctx context.Context, callerPubKey string, clientPubKey string, status string, maxStorageBytes uint64) error
	UpdateClient(ctx context.Context, callerPubKey string, id uint64, status string, maxStorageBytes uint64) error
}) *MockRPCClient {
	return &MockRPCClient{engine: engine}
}

func (m *MockRPCClient) OfferItems(ctx context.Context, items []rpc.Metadata) (rpc.UploadPlan, error) {
	if m.engine != nil {
		return m.engine.OfferItems(ctx, "insecure-local-client", items)
	}
	// Default mock behavior
	var plan rpc.UploadPlan
	for i := range items {
		plan.NeededIndices = append(plan.NeededIndices, int32(i))
	}
	return plan, nil
}

func (m *MockRPCClient) UploadItemsStreamed(ctx context.Context, items []rpc.StreamItem) error {
	if m.engine != nil {
		for _, item := range items {
			if err := m.engine.IngestItemsStreamed(ctx, "insecure-local-client", hex.EncodeToString(item.Header.Hash[:]), item.Header.Size, item.Data); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

func (m *MockRPCClient) ListSpecialItems(ctx context.Context) ([]rpc.Metadata, error) {
	if m.engine != nil {
		return m.engine.ListSpecialItems(ctx, "insecure-local-client")
	}
	return nil, nil
}

func (m *MockRPCClient) DownloadItems(ctx context.Context, hashes []string) ([]rpc.ItemData, []string, error) {
	if m.engine != nil {
		return m.engine.GetItems(ctx, hashes)
	}
	return nil, hashes, nil
}

func (m *MockRPCClient) DeleteItems(ctx context.Context, hashes []string) error {
	if m.engine != nil {
		return m.engine.DeleteItems(ctx, hashes)
	}
	return nil
}

func (m *MockRPCClient) ListAllItems(ctx context.Context) ([]string, error) {
	if m.engine != nil {
		return m.engine.ListAllItems(ctx)
	}
	return nil, nil
}

func (m *MockRPCClient) GetStatus(ctx context.Context) (rpc.StatusInfo, error) {
	if m.engine != nil {
		return m.engine.GetStatus(ctx)
	}
	return rpc.StatusInfo{}, nil
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
				Source:              p.Source,
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
