package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"p2p-backup/internal/crypto"
	"p2p-backup/internal/rpc"

	capnp "capnproto.org/go/capnp/v3"
	capnprpc "capnproto.org/go/capnp/v3/rpc"
	"capnproto.org/go/capnp/v3/rpc/transport"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
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

func (c *CapnpRPCClient) OfferBlobs(ctx context.Context, blobs []rpc.BlobMeta) ([]uint32, error) {
	req, release := c.clientStub.OfferBlobs(ctx, func(p rpc.BackupServer_offerBlobs_Params) error {
		capnpBlobs, err := p.NewBlobs(int32(len(blobs)))
		if err != nil {
			return err
		}
		for i, b := range blobs {
			cb := capnpBlobs.At(i)
			hashBytes, _ := hex.DecodeString(b.Hash)
			cb.SetChecksum(hashBytes)
			cb.SetSize(uint64(b.Size))
			cb.SetSpecial(b.Special)
		}
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return nil, fmt.Errorf("failed to read OfferBlobs results: %w", err)
	}

	neededList, err := res.NeededIndices()
	if err != nil {
		return nil, fmt.Errorf("failed to read needed indices: %w", err)
	}

	neededIndices := make([]uint32, neededList.Len())
	for i := 0; i < neededList.Len(); i++ {
		neededIndices[i] = neededList.At(i)
	}

	return neededIndices, nil
}

func (c *CapnpRPCClient) DeleteBlobs(ctx context.Context, hashes []string) error {
	req, release := c.clientStub.DeleteBlobs(ctx, func(p rpc.BackupServer_deleteBlobs_Params) error {
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
		return fmt.Errorf("failed to read DeleteBlobs results: %w", err)
	}

	if !res.Success() {
		errMsg, _ := res.Error()
		return fmt.Errorf("server rejected delete: %s", errMsg)
	}

	return nil
}

func (c *CapnpRPCClient) ListAllBlobs(ctx context.Context) ([]string, error) {
	req, release := c.clientStub.ListAllBlobs(ctx, func(p rpc.BackupServer_listAllBlobs_Params) error {
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return nil, fmt.Errorf("failed to read ListAllBlobs results: %w", err)
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

func (c *CapnpRPCClient) PrepareUploadClient(ctx context.Context, blobs []rpc.BlobMeta) error {
	req, release := c.clientStub.PrepareUploadClient(ctx, func(p rpc.BackupServer_prepareUploadClient_Params) error {
		capnpBlobs, err := p.NewBlobs(int32(len(blobs)))
		if err != nil {
			return err
		}
		for i, b := range blobs {
			cb := capnpBlobs.At(i)
			hashBytes, _ := hex.DecodeString(b.Hash)
			cb.SetChecksum(hashBytes)
			cb.SetSize(uint64(b.Size))
			cb.SetSpecial(b.Special)
		}
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return fmt.Errorf("failed to read PrepareUpload results: %w", err)
	}

	if !res.Success() {
		errMsg, _ := res.Error()
		return fmt.Errorf("server rejected upload: %s", errMsg)
	}

	return nil
}

func (c *CapnpRPCClient) PushBlobBatch(ctx context.Context, jobs []rpc.UploadJob) error {
	if c.p2pHost == nil {
		return fmt.Errorf("raw data stream unavailable (no libp2p host)")
	}

	stream, err := c.p2pHost.NewStream(ctx, c.serverID, "/bdr/data/1.0.0")
	if err != nil {
		return fmt.Errorf("failed to open batch upload stream: %w", err)
	}
	defer stream.Close()

	// 1. Write Header: [OpCode (1b)][Count (4b)]
	if err := rpc.WriteBatchHeader(stream, rpc.OpCodePushBatch, uint32(len(jobs))); err != nil {
		return fmt.Errorf("failed to write batch header: %w", err)
	}

	// 2. Loop and write each item using unified framing
	// Note: Client doesn't have an Engine, so we create a small temporary pool or pass nil if the helper handles it.
	// Actually, let's just use a simple local buffer for the client since it's not a high-concurrency server.
	tempPool := &sync.Pool{
		New: func() any { return make([]byte, 4*1024*1024) },
	}

	for _, job := range jobs {
		err = rpc.WriteFrame(stream, job.Hash, job.Size, bytes.NewReader(job.Data), tempPool)
		if err != nil {
			return fmt.Errorf("failed to write item frame for %s: %w", job.Hash, err)
		}
	}

	// 3. Read Acknowledgment: [uint32 successCount]
	ackBuf := make([]byte, 4)
	if _, err := io.ReadFull(stream, ackBuf); err != nil {
		return fmt.Errorf("failed to read server batch acknowledgment: %w", err)
	}
	successCount := binary.BigEndian.Uint32(ackBuf)

	if successCount != uint32(len(jobs)) {
		return fmt.Errorf("server only ingested %d/%d blobs in batch", successCount, len(jobs))
	}

	return nil
}

func (c *CapnpRPCClient) PushBlob(ctx context.Context, hashHex string, data []byte) error {
	if c.p2pHost == nil {
		return fmt.Errorf("raw data stream unavailable (no libp2p host)")
	}

	stream, err := c.p2pHost.NewStream(ctx, c.serverID, "/bdr/data/1.0.0")
	if err != nil {
		return fmt.Errorf("failed to open upload stream: %w", err)
	}
	defer stream.Close()

	checksumBytes, _ := hex.DecodeString(hashHex)
	header := make([]byte, 41)
	header[0] = 0x03 // OpCodePushBlob
	copy(header[1:33], checksumBytes)
	binary.BigEndian.PutUint64(header[33:41], uint64(len(data)))

	if _, err := stream.Write(header); err != nil {
		return fmt.Errorf("failed to write stream header: %w", err)
	}

	if _, err := stream.Write(data); err != nil {
		return fmt.Errorf("failed to write stream data: %w", err)
	}

	// No ack needed for raw PushBlob stream in this implementation, the server closes on its end.
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
		peers = append(peers, PeerMeta{
			ID:                  int64(cp.Id()),
			IPAddress:           addr,
			PublicKey:           pubKey,
			Status:              status,
			StorageLimitGB:      int(cp.MaxStorageSize()),
			CurrentStorageSize:  int64(cp.CurrentStorageSize()),
			OutboundStorageSize: int64(cp.OutboundStorageSize()),
			ContactInfo:         contact,
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

func (c *CapnpRPCClient) ListSpecialBlobs(ctx context.Context) ([]rpc.BlobMeta, error) {
	// Optional feature to implement fully later.
	return nil, fmt.Errorf("ListSpecialBlobs not implemented on client yet")
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

func (c *CapnpRPCClient) PullBlobBatch(ctx context.Context, hashes []string) ([]rpc.LocalBlobData, error) {
	if c.p2pHost == nil {
		return nil, fmt.Errorf("raw data stream unavailable (no libp2p host)")
	}

	stream, err := c.p2pHost.NewStream(ctx, c.serverID, "/bdr/data/1.0.0")
	if err != nil {
		return nil, fmt.Errorf("failed to open data stream: %w", err)
	}
	defer stream.Close()

	// 1. Write Header: [OpCode (1b)][Count (4b)]
	if err := rpc.WriteBatchHeader(stream, rpc.OpCodePullBatch, uint32(len(hashes))); err != nil {
		return nil, fmt.Errorf("failed to write batch header: %w", err)
	}

	// 2. Write Hashes: [Checksum (32b)]...
	for _, h := range hashes {
		hashBytes, _ := hex.DecodeString(h)
		if _, err := stream.Write(hashBytes); err != nil {
			return nil, fmt.Errorf("failed to write hash %s: %w", h, err)
		}
	}

	// 3. Read Responses: [Sync(2b)][Size (8b)][Data]... using unified framing
	var results []rpc.LocalBlobData
	tempPool := &sync.Pool{
		New: func() any { return make([]byte, 4*1024*1024) },
	}

	for _, h := range hashes {
		data, err := rpc.ReadFrame(stream, h, -1, tempPool)
		if err != nil {
			return nil, fmt.Errorf("failed to read frame for %s: %w", h, err)
		}
		if len(data) > 0 {
			results = append(results, rpc.LocalBlobData{Hash: h, Data: data})
		}
	}

	// 4. Read final Acknowledgment: [uint32 successCount]
	ackBuf := make([]byte, 4)
	if _, err := io.ReadFull(stream, ackBuf); err != nil {
		return nil, fmt.Errorf("failed to read server pull acknowledgment: %w", err)
	}

	return results, nil
}

func (c *CapnpRPCClient) PullBlob(ctx context.Context, hashHex string) ([]byte, error) {
	if c.p2pHost == nil {
		return nil, fmt.Errorf("raw data stream unavailable (no libp2p host)")
	}

	stream, err := c.p2pHost.NewStream(ctx, c.serverID, "/bdr/data/1.0.0")
	if err != nil {
		return nil, fmt.Errorf("failed to open data stream: %w", err)
	}
	defer stream.Close()

	checksumBytes, _ := hex.DecodeString(hashHex)
	header := make([]byte, 41)
	header[0] = 0x04 // OpCodePullBlob
	copy(header[1:33], checksumBytes)

	if _, err := stream.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write stream header: %w", err)
	}

	sizeBuf := make([]byte, 8)
	if _, err := io.ReadFull(stream, sizeBuf); err != nil {
		return nil, fmt.Errorf("failed to read response size header: %w", err)
	}
	size := binary.BigEndian.Uint64(sizeBuf)
	if size == 0 {
		return nil, fmt.Errorf("blob not found on server")
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(stream, data); err != nil {
		return nil, fmt.Errorf("failed to read blob data: %w", err)
	}

	return data, nil
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
