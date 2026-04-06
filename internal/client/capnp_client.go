package client

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"

	capnp "capnproto.org/go/capnp/v3"
	capnprpc "capnproto.org/go/capnp/v3/rpc"
	"capnproto.org/go/capnp/v3/rpc/transport"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"p2p-backup/internal/crypto"
	"p2p-backup/internal/rpc"
)

type customCodec struct {
	decoder *capnp.Decoder
	encoder *capnp.Encoder
	closer  func() error
}

func (c *customCodec) Encode(m *capnp.Message) error {
	return c.encoder.Encode(m)
}
func (c *customCodec) Decode() (*capnp.Message, error) {
	msg, err := c.decoder.Decode()
	if err != nil {
		return nil, err
	}
	msg.TraverseLimit = 512 * 1024 * 1024
	return msg, nil
}
func (c *customCodec) Close() error {
	return c.closer()
}

type CapnpRPCClient struct {
	stream     io.ReadWriteCloser
	rpcConn    *capnprpc.Conn
	clientStub rpc.BackupServer
	p2pHost    host.Host
	serverID   peer.ID
}

func NewCapnpRPCClient(ctx context.Context, p2pHost host.Host, serverAddrStr string, expectedServerPubKeyHex string, verbose bool, extraVerbose bool) (*CapnpRPCClient, error) {
	serverPID, err := crypto.PeerIDFromPubKeyHex(expectedServerPubKeyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid server public key: %w", err)
	}

	maddr, err := multiaddr.NewMultiaddr(serverAddrStr)
	if err != nil {
		// Fallback to trying to parse as host:port and construct a QUIC multiaddr
		hostAddr, portStr, splitErr := net.SplitHostPort(serverAddrStr)
		if splitErr != nil {
			return nil, fmt.Errorf("invalid server address format: %w", splitErr)
		}
		maddr, err = multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/udp/%s/quic-v1", hostAddr, portStr))
		if err != nil {
			return nil, fmt.Errorf("invalid server address format: %w", err)
		}
	}

	addrInfo := peer.AddrInfo{
		ID:    serverPID,
		Addrs: []multiaddr.Multiaddr{maddr},
	}

	if err := p2pHost.Connect(ctx, addrInfo); err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	stream, err := p2pHost.NewStream(ctx, serverPID, "/bdr/rpc/1.0.0")
	if err != nil {
		return nil, fmt.Errorf("failed to open RPC stream: %w", err)
	}

	if verbose {
		log.Printf("Connected to Server %s at %s\n", serverPID.String(), serverAddrStr)
	}

	decoder := capnp.NewDecoder(stream)
	decoder.MaxMessageSize = 512 * 1024 * 1024 // 512MB limit for massive restores
	encoder := capnp.NewEncoder(stream)

	codec := &customCodec{
		decoder: decoder,
		encoder: encoder,
		closer:  stream.Close,
	}

	rpcConn := capnprpc.NewConn(transport.New(codec), nil)
	clientStub := rpc.BackupServer(rpcConn.Bootstrap(ctx))

	return &CapnpRPCClient{
		stream:     stream,
		rpcConn:    rpcConn,
		clientStub: clientStub,
		p2pHost:    p2pHost,
		serverID:   serverPID,
	}, nil
}

func (c *CapnpRPCClient) OfferItems(ctx context.Context, items []rpc.Metadata) (rpc.UploadPlan, error) {
	req, release := c.clientStub.OfferItems(ctx, func(p rpc.BackupServer_offerItems_Params) error {
		capnpItems, err := p.NewItems(int32(len(items)))
		if err != nil {
			return err
		}
		for i, item := range items {
			cm, err := rpc.MetadataToCapnp(p.Segment(), item)
			if err != nil {
				return err
			}
			capnpItems.Set(i, cm)
		}
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return rpc.UploadPlan{}, fmt.Errorf("failed to read OfferItems results: %w", err)
	}

	neededList, err := res.NeededIndices()
	if err != nil {
		return rpc.UploadPlan{}, fmt.Errorf("failed to read needed indices: %w", err)
	}

	var plan rpc.UploadPlan
	plan.NeededIndices = make([]int32, neededList.Len())
	for i := 0; i < neededList.Len(); i++ {
		plan.NeededIndices[i] = int32(neededList.At(i))
	}

	return plan, nil
}

func (c *CapnpRPCClient) DeleteItems(ctx context.Context, hashes []string) error {
	req, release := c.clientStub.DeleteItems(ctx, func(p rpc.BackupServer_deleteItems_Params) error {
		capnpHashes, err := p.NewChecksums(int32(len(hashes)))
		if err != nil {
			return err
		}
		for i, h := range hashes {
			hashBytes, _ := hex.DecodeString(h)
			capnpHashes.Set(i, hashBytes)
		}
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return fmt.Errorf("failed to read DeleteItems results: %w", err)
	}

	if !res.Success() {
		errMsg, _ := res.Error()
		return fmt.Errorf("server rejected delete: %s", errMsg)
	}

	return nil
}

func (c *CapnpRPCClient) ListAllItems(ctx context.Context) ([]string, error) {
	req, release := c.clientStub.ListAllItems(ctx, func(p rpc.BackupServer_listAllItems_Params) error {
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return nil, fmt.Errorf("failed to read ListAllItems results: %w", err)
	}

	capnpHashes, err := res.Checksums()
	if err != nil {
		return nil, fmt.Errorf("failed to read checksums list: %w", err)
	}

	var hashes []string
	for i := 0; i < capnpHashes.Len(); i++ {
		hashBytes, _ := capnpHashes.At(i)
		hashes = append(hashes, hex.EncodeToString(hashBytes))
	}

	return hashes, nil
}

func (c *CapnpRPCClient) UploadItemsStreamed(ctx context.Context, items []rpc.StreamItem) error {
	if c.p2pHost == nil {
		return fmt.Errorf("raw data stream unavailable (no libp2p host)")
	}

	s, err := c.p2pHost.NewStream(ctx, c.serverID, "/bdr/stream/1.0.0")
	if err != nil {
		return fmt.Errorf("failed to open unified stream: %w", err)
	}
	defer s.Close()

	sender := rpc.NewStreamSender(s)
	if err := sender.SendBatch(ctx, items); err != nil {
		return err
	}

	// 5. Read ACK byte from server to ensure batch was processed
	ack := make([]byte, 1)
	if _, err := io.ReadFull(s, ack); err != nil {
		return fmt.Errorf("failed to read server ACK: %w", err)
	}

	if ack[0] != 0 {
		return fmt.Errorf("server batch processing failed (status %d)", ack[0])
	}
	return nil
}

func (c *CapnpRPCClient) AddPeer(ctx context.Context, address string) error {
	req, release := c.clientStub.AddPeer(ctx, func(p rpc.BackupServer_addPeer_Params) error {
		if err := p.SetAddress(address); err != nil {
			return err
		}
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return fmt.Errorf("failed to read AddPeer results: %w", err)
	}

	if !res.Success() {
		errMsg, _ := res.Error()
		return fmt.Errorf("server rejected AddPeer: %s", errMsg)
	}
	return nil
}

func (c *CapnpRPCClient) UpdatePeer(ctx context.Context, id int64, status string, maxGB uint64) error {
	req, release := c.clientStub.UpdatePeer(ctx, func(p rpc.BackupServer_updatePeer_Params) error {
		p.SetId(uint64(id))
		if err := p.SetStatus(status); err != nil {
			return err
		}
		p.SetMaxStorageSize(maxGB * 1024 * 1024 * 1024)
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return fmt.Errorf("failed to read UpdatePeer results: %w", err)
	}

	if !res.Success() {
		errMsg, _ := res.Error()
		return fmt.Errorf("server rejected UpdatePeer: %s", errMsg)
	}
	return nil
}

func (c *CapnpRPCClient) ListPeers(ctx context.Context) ([]PeerMeta, error) {
	req, release := c.clientStub.ListPeers(ctx, func(p rpc.BackupServer_listPeers_Params) error {
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return nil, fmt.Errorf("failed to read ListPeers results: %w", err)
	}

	capnpPeers, err := res.Peers()
	if err != nil {
		return nil, fmt.Errorf("failed to read peers list: %w", err)
	}

	var peers []PeerMeta
	for i := 0; i < capnpPeers.Len(); i++ {
		cp := capnpPeers.At(i)
		addr, _ := cp.Address()
		pubKey, _ := cp.PublicKey()
		status, _ := cp.Status()
		firstSeen, _ := cp.FirstSeen()
		lastSeen, _ := cp.LastSeen()
		contact, _ := cp.ContactInfo()
		source, _ := cp.Source()
		peers = append(peers, PeerMeta{
			ID:                  int64(cp.Id()),
			IPAddress:           addr,
			PublicKey:           pubKey,
			Status:              status,
			StorageLimitGB:      int(cp.MaxStorageSize()),
			CurrentStorageSize:  int64(cp.CurrentStorageSize()),
			OutboundStorageSize: int64(cp.OutboundStorageSize()),
			ContactInfo:         contact,
			Source:              source,
			TotalShards:         cp.TotalShards(),
			CurrentShards:       cp.CurrentShards(),
			FirstSeen:           firstSeen,
			LastSeen:            lastSeen,
			ChallengesMade:      cp.ChallengesMade(),
			ChallengesPassed:    cp.ChallengesPassed(),
			ConnectionsOk:       cp.ConnectionsOk(),
			IntegrityAttempts:   cp.IntegrityAttempts(),
		})
	}
	return peers, nil
}

func (c *CapnpRPCClient) ListSpecialItems(ctx context.Context) ([]rpc.Metadata, error) {
	req, release := c.clientStub.ListSpecialItems(ctx, nil)
	defer release()

	res, err := req.Struct()
	if err != nil {
		return nil, fmt.Errorf("failed to read ListSpecialItems results: %w", err)
	}

	capnpItems, err := res.Items()
	if err != nil {
		return nil, err
	}

	var items []rpc.Metadata
	for i := 0; i < capnpItems.Len(); i++ {
		items = append(items, rpc.MetadataFromCapnp(capnpItems.At(i)))
	}
	return items, nil
}

func (c *CapnpRPCClient) GetStatus(ctx context.Context) (rpc.StatusInfo, error) {
	req, release := c.clientStub.GetStatus(ctx, func(p rpc.BackupServer_getStatus_Params) error {
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return rpc.StatusInfo{}, fmt.Errorf("failed to read GetStatus results: %w", err)
	}

	status, err := res.Status()
	if err != nil {
		return rpc.StatusInfo{}, fmt.Errorf("failed to get status struct: %w", err)
	}

	return rpc.StatusInfo{
		UptimeSeconds:             status.UptimeSeconds(),
		TotalShards:               status.TotalShards(),
		FullyReplicatedShards:     status.FullyReplicatedShards(),
		PartiallyReplicatedShards: status.PartiallyReplicatedShards(),
		HostedPeerShards:          status.HostedPeerShards(),
		QueuedBytes:               status.QueuedBytes(),
	}, nil
}

func (c *CapnpRPCClient) DownloadItems(ctx context.Context, hashes []string) ([]rpc.ItemData, []string, error) {
	req, release := c.clientStub.DownloadItems(ctx, func(p rpc.BackupServer_downloadItems_Params) error {
		capnpHashes, err := p.NewChecksums(int32(len(hashes)))
		if err != nil {
			return err
		}
		for i, h := range hashes {
			hashBytes, _ := hex.DecodeString(h)
			capnpHashes.Set(i, hashBytes)
		}
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read DownloadItems results: %w", err)
	}

	capnpItems, err := res.Items()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read downloaded items: %w", err)
	}

	var foundItems []rpc.ItemData
	for i := 0; i < capnpItems.Len(); i++ {
		ci := capnpItems.At(i)
		cm, err := ci.Meta()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read item %d metadata: %w", i, err)
		}
		meta := rpc.MetadataFromCapnp(cm)
		dataBytes, _ := ci.Data()

		// Copy data
		dataCopy := make([]byte, len(dataBytes))
		copy(dataCopy, dataBytes)

		actualHash := crypto.Hash(dataCopy)
		if hex.EncodeToString(actualHash) != meta.Hash {
			return nil, nil, fmt.Errorf("downloaded item %s failed checksum validation", meta.Hash)
		}

		foundItems = append(foundItems, rpc.ItemData{
			Meta: meta,
			Data: dataCopy,
		})
	}

	capnpMissing, err := res.Missing()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read missing items: %w", err)
	}

	var missingHashes []string
	for i := 0; i < capnpMissing.Len(); i++ {
		hashBytes, _ := capnpMissing.At(i)
		missingHashes = append(missingHashes, hex.EncodeToString(hashBytes))
	}

	return foundItems, missingHashes, nil
}

func (c *CapnpRPCClient) ListClients(ctx context.Context) ([]ClientMeta, error) {
	req, release := c.clientStub.ListClients(ctx, nil)
	defer release()

	res, err := req.Struct()
	if err != nil {
		return nil, fmt.Errorf("failed to read ListClients results: %w", err)
	}

	capnpClients, err := res.Clients()
	if err != nil {
		return nil, err
	}

	var clients []ClientMeta
	for i := 0; i < capnpClients.Len(); i++ {
		cc := capnpClients.At(i)
		pk, _ := cc.PublicKey()
		status, _ := cc.Status()
		ls, _ := cc.LastSeen()
		clients = append(clients, ClientMeta{
			ID:                 cc.Id(),
			PublicKey:          pk,
			Status:             status,
			LastSeen:           ls,
			MaxStorageSize:     cc.MaxStorageSize(),
			CurrentStorageSize: cc.CurrentStorageSize(),
		})
	}

	return clients, nil
}

func (c *CapnpRPCClient) AddClient(ctx context.Context, publicKey string, status string, maxGB uint64) error {
	req, release := c.clientStub.AddClient(ctx, func(p rpc.BackupServer_addClient_Params) error {
		p.SetPublicKey(publicKey)
		p.SetStatus(status)
		p.SetMaxStorageSize(maxGB * 1024 * 1024 * 1024)
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return fmt.Errorf("failed to read AddClient results: %w", err)
	}

	if !res.Success() {
		errMsg, _ := res.Error()
		return fmt.Errorf("server rejected AddClient: %s", errMsg)
	}

	return nil
}

func (c *CapnpRPCClient) UpdateClient(ctx context.Context, id uint64, status string, maxGB uint64) error {
	req, release := c.clientStub.UpdateClient(ctx, func(p rpc.BackupServer_updateClient_Params) error {
		p.SetId(id)
		p.SetStatus(status)
		p.SetMaxStorageSize(maxGB * 1024 * 1024 * 1024)
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return fmt.Errorf("failed to read UpdateClient results: %w", err)
	}

	if !res.Success() {
		errMsg, _ := res.Error()
		return fmt.Errorf("server rejected update: %s", errMsg)
	}

	return nil
}

func (c *CapnpRPCClient) Close() error {
	c.clientStub.Release()
	if err := c.rpcConn.Close(); err != nil {
		c.stream.Close()
		return err
	}
	return c.stream.Close()
}
