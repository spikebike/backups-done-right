package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"

	capnp "capnproto.org/go/capnp/v3"
	capnprpc "capnproto.org/go/capnp/v3/rpc"
	"capnproto.org/go/capnp/v3/rpc/transport"
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

type CapnpPeerClient struct {
	stream     io.ReadWriteCloser
	rpcConn    *capnprpc.Conn
	clientStub rpc.PeerNode
	Permanent  bool // If true, Close() only releases the stub, not the connection
}

func (c *CapnpPeerClient) GetStub() rpc.PeerNode {
	return c.clientStub
}

func NewPeerClientFromStub(stub rpc.PeerNode) *CapnpPeerClient {
	return &CapnpPeerClient{clientStub: stub, Permanent: true}
}

func NewCapnpPeerClient(ctx context.Context, e *Engine, address string, expectedPeerPubKeyHex string, localNode rpc.PeerNode) (*CapnpPeerClient, error) {
	peerID, err := crypto.PeerIDFromPubKeyHex(expectedPeerPubKeyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid peer public key: %w", err)
	}

	maddr, err := multiaddr.NewMultiaddr(address)
	if err != nil {
		// Fallback to trying to parse as host:port and construct a QUIC multiaddr
		hostAddr, portStr, splitErr := net.SplitHostPort(address)
		if splitErr != nil {
			return nil, fmt.Errorf("invalid peer address format: %w", splitErr)
		}
		maddr, err = multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/udp/%s/quic-v1", hostAddr, portStr))
		if err != nil {
			return nil, fmt.Errorf("invalid peer address format: %w", err)
		}
	}

	addrInfo := peer.AddrInfo{
		ID:    peerID,
		Addrs: []multiaddr.Multiaddr{maddr},
	}

	if err := e.Host.Connect(ctx, addrInfo); err != nil {
		return nil, fmt.Errorf("failed to connect to peer: %w", err)
	}

	stream, err := e.Host.NewStream(ctx, peerID, "/bdr/rpc/1.0.0")
	if err != nil {
		return nil, fmt.Errorf("failed to open RPC stream: %w", err)
	}

	// Apply bandwidth throttling
	throttledStream := e.NewThrottledStream(ctx, stream)

	decoder := capnp.NewDecoder(throttledStream)
	decoder.MaxMessageSize = 512 * 1024 * 1024 // 512MB limit for large erasure chunks
	encoder := capnp.NewEncoder(throttledStream)

	codec := &customCodec{
		decoder: decoder,
		encoder: encoder,
		closer:  throttledStream.Close,
	}

	var bootstrapClient capnp.Client
	if localNode.IsValid() {
		bootstrapClient = capnp.Client(localNode).AddRef()
	}

	rpcConn := capnprpc.NewConn(transport.New(codec), &capnprpc.Options{
		BootstrapClient: bootstrapClient,
	})
	clientStub := rpc.PeerNode(rpcConn.Bootstrap(ctx))

	return &CapnpPeerClient{
		stream:     throttledStream,
		rpcConn:    rpcConn,
		clientStub: clientStub,
	}, nil
}

func (c *CapnpPeerClient) OfferItems(ctx context.Context, shards []rpc.Metadata) ([]uint32, error) {
	req, release := c.clientStub.OfferItems(ctx, func(p rpc.PeerNode_offerItems_Params) error {
		capnpShards, err := p.NewItems(int32(len(shards)))
		if err != nil {
			return err
		}
		for i, s := range shards {
			cs := capnpShards.At(i)
			hashBytes, _ := hex.DecodeString(s.Hash)
			cs.SetChecksum(hashBytes)
			cs.SetSize(uint64(s.Size))
			cs.SetType(rpc.TransferType(s.Type))
			cs.SetIsSpecial(s.IsSpecial)
			cs.SetPieceIndex(uint32(s.PieceIndex))
			cs.SetSequenceNumber(s.SequenceNumber)
			cs.SetTotalPieces(uint32(s.TotalPieces))
			parentHashBytes, _ := hex.DecodeString(s.ParentShardHash)
			cs.SetParentShardHash(parentHashBytes)
		}
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return nil, fmt.Errorf("failed to read OfferItems results: %w", err)
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

func (c *CapnpPeerClient) PrepareUpload(ctx context.Context, shards []rpc.Metadata) error {
	req, release := c.clientStub.PrepareUpload(ctx, func(p rpc.PeerNode_prepareUpload_Params) error {
		capnpShards, err := p.NewItems(int32(len(shards)))
		if err != nil {
			return err
		}
		for i, s := range shards {
			cs := capnpShards.At(i)
			hashBytes, _ := hex.DecodeString(s.Hash)
			cs.SetChecksum(hashBytes)
			cs.SetSize(uint64(s.Size))
			cs.SetType(rpc.TransferType(s.Type))
			cs.SetIsSpecial(s.IsSpecial)
			cs.SetPieceIndex(uint32(s.PieceIndex))
			cs.SetSequenceNumber(s.SequenceNumber)
			cs.SetTotalPieces(uint32(s.TotalPieces))
			parentHashBytes, _ := hex.DecodeString(s.ParentShardHash)
			cs.SetParentShardHash(parentHashBytes)
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
		return fmt.Errorf("peer rejected upload: %s", errMsg)
	}

	return nil
}

func (c *CapnpPeerClient) ChallengePiece(ctx context.Context, hashBytes []byte, offset uint64) ([]byte, error) {
	req, release := c.clientStub.ChallengePiece(ctx, func(p rpc.PeerNode_challengePiece_Params) error {
		if err := p.SetChecksum(hashBytes); err != nil {
			return err
		}
		p.SetOffset(offset)
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return nil, fmt.Errorf("failed to read ChallengePiece results: %w", err)
	}

	data, err := res.Data()
	if err != nil {
		return nil, fmt.Errorf("failed to read challenge data: %w", err)
	}

	// Make a safe copy of the capnp byte slice
	outBytes := make([]byte, len(data))
	copy(outBytes, data)

	return outBytes, nil
}

func (c *CapnpPeerClient) ReleasePiece(ctx context.Context, hashBytes []byte) error {
	req, release := c.clientStub.ReleasePiece(ctx, func(p rpc.PeerNode_releasePiece_Params) error {
		if err := p.SetChecksum(hashBytes); err != nil {
			return err
		}
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return fmt.Errorf("failed to read ReleasePiece results: %w", err)
	}

	if !res.Success() {
		errMsg, _ := res.Error()
		return fmt.Errorf("peer rejected release: %s", errMsg)
	}

	return nil
}

func (c *CapnpPeerClient) DownloadItems(ctx context.Context, hashes []string) ([]rpc.ItemData, []string, error) {
	req, release := c.clientStub.DownloadItems(ctx, func(p rpc.PeerNode_downloadItems_Params) error {
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
		hashBytes, _ := cm.Checksum()
		meta := rpc.Metadata{
			Hash:       hex.EncodeToString(hashBytes),
			Size:       int64(cm.Size()),
			IsSpecial:  cm.IsSpecial(),
			PieceIndex: int(cm.PieceIndex()),
		}
		dataBytes, _ := ci.Data()
		dataCopy := make([]byte, len(dataBytes))
		copy(dataCopy, dataBytes)

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

func (c *CapnpPeerClient) ListSpecialItems(ctx context.Context) ([]rpc.Metadata, error) {
	req, release := c.clientStub.ListSpecialItems(ctx, nil)
	defer release()

	res, err := req.Struct()
	if err != nil {
		return nil, fmt.Errorf("failed to read ListSpecialItems results: %w", err)
	}

	capnpShards, err := res.Items()
	if err != nil {
		return nil, err
	}

	var pieces []rpc.Metadata
	for i := 0; i < capnpShards.Len(); i++ {
		cs := capnpShards.At(i)
		hashBytes, _ := cs.Checksum()
		parentHash, _ := cs.ParentShardHash()
		pieces = append(pieces, rpc.Metadata{
			Hash:            hex.EncodeToString(hashBytes),
			Size:            int64(cs.Size()),
			IsSpecial:       true,
			PieceIndex:      int(cs.PieceIndex()),
			ParentShardHash: hex.EncodeToString(parentHash),
			SequenceNumber:  cs.SequenceNumber(),
			TotalPieces:     int(cs.TotalPieces()),
		})
	}

	return pieces, nil
}

func (c *CapnpPeerClient) Announce(ctx context.Context, listenAddr, contactInfo string, callback rpc.PeerNode) error {
	req, release := c.clientStub.Announce(ctx, func(p rpc.PeerNode_announce_Params) error {
		p.SetListenAddress(listenAddr)
		p.SetContactInfo(contactInfo)
		p.SetCallback(callback)
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return err
	}
	if !res.Success() {
		return fmt.Errorf("peer rejected announcement")
	}
	return nil
}

// ForceClose closes the underlying connection regardless of the Permanent flag.
func (c *CapnpPeerClient) ForceClose() error {
	c.clientStub.Release()
	if c.rpcConn != nil {
		c.rpcConn.Close()
	}
	if c.stream != nil {
		return c.stream.Close()
	}
	return nil
}

func (c *CapnpPeerClient) Close() error {
	c.clientStub.Release()
	if c.Permanent {
		return nil // Lifecycle managed by pool or background goroutine
	}
	if c.rpcConn != nil {
		if err := c.rpcConn.Close(); err != nil {
			if c.stream != nil {
				c.stream.Close()
			}
			return err
		}
	}
	if c.stream != nil {
		return c.stream.Close()
	}
	return nil
}
