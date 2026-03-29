package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	capnp "capnproto.org/go/capnp/v3"
	capnprpc "capnproto.org/go/capnp/v3/rpc"
	"capnproto.org/go/capnp/v3/rpc/transport"
	"p2p-backup/internal/rpc"
	"p2p-backup/internal/crypto"
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
}

func (c *CapnpPeerClient) GetStub() rpc.PeerNode {
	return c.clientStub
}

func NewPeerClientFromStub(stub rpc.PeerNode) *CapnpPeerClient {
	return &CapnpPeerClient{clientStub: stub}
}

func NewCapnpPeerClient(ctx context.Context, p2pHost host.Host, address string, expectedPeerPubKeyHex string, localNode rpc.PeerNode) (*CapnpPeerClient, error) {
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

	if err := p2pHost.Connect(ctx, addrInfo); err != nil {
		return nil, fmt.Errorf("failed to connect to peer: %w", err)
	}

	stream, err := p2pHost.NewStream(ctx, peerID, "/bdr/rpc/1.0.0")
	if err != nil {
		return nil, fmt.Errorf("failed to open RPC stream: %w", err)
	}

	decoder := capnp.NewDecoder(stream)
	decoder.MaxMessageSize = 512 * 1024 * 1024 // 512MB limit for large erasure chunks
	encoder := capnp.NewEncoder(stream)

	codec := &customCodec{
		decoder: decoder,
		encoder: encoder,
		closer:  stream.Close,
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
		stream:     stream,
		rpcConn:    rpcConn,
		clientStub: clientStub,
	}, nil
}

func (c *CapnpPeerClient) OfferShards(ctx context.Context, shards []rpc.PeerShardMeta) ([]uint32, error) {
	req, release := c.clientStub.OfferShards(ctx, func(p rpc.PeerNode_offerShards_Params) error {
		capnpShards, err := p.NewShards(int32(len(shards)))
		if err != nil {
			return err
		}
		for i, s := range shards {
			cs := capnpShards.At(i)
			hashBytes, _ := hex.DecodeString(s.Hash)
			cs.SetChecksum(hashBytes)
			cs.SetSize(uint64(s.Size))
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
		return nil, fmt.Errorf("failed to read OfferShards results: %w", err)
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

func (c *CapnpPeerClient) UploadShards(ctx context.Context, shards []rpc.LocalBlobData) error {
	req, release := c.clientStub.UploadShards(ctx, func(p rpc.PeerNode_uploadShards_Params) error {
		capnpShards, err := p.NewShards(int32(len(shards)))
		if err != nil {
			return err
		}
		for i, s := range shards {
			cs := capnpShards.At(i)
			hashBytes, _ := hex.DecodeString(s.Hash)
			cs.SetChecksum(hashBytes)
			cs.SetData(s.Data)
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
		return fmt.Errorf("failed to read UploadShards results: %w", err)
	}

	if !res.Success() {
		errMsg, _ := res.Error()
		return fmt.Errorf("peer rejected upload: %s", errMsg)
	}

	return nil
}

func (c *CapnpPeerClient) ChallengePiece(ctx context.Context, hashBytes []byte, offset uint64) ([]byte, error) {
	req, release := c.clientStub.ChallengePiece(ctx, func(p rpc.PeerNode_challengePiece_Params) error {
		if err := p.SetShardChecksum(hashBytes); err != nil {
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
		if err := p.SetShardChecksum(hashBytes); err != nil {
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

func (c *CapnpPeerClient) DownloadPiece(ctx context.Context, hashBytes []byte) ([]byte, error) {
	req, release := c.clientStub.DownloadPiece(ctx, func(p rpc.PeerNode_downloadPiece_Params) error {
		if err := p.SetShardChecksum(hashBytes); err != nil {
			return err
		}
		return nil
	})
	defer release()

	res, err := req.Struct()
	if err != nil {
		return nil, fmt.Errorf("failed to read DownloadPiece results: %w", err)
	}

	data, err := res.Data()
	if err != nil {
		return nil, fmt.Errorf("failed to read piece data: %w", err)
	}

	// Make a safe copy of the capnp byte slice
	outBytes := make([]byte, len(data))
	copy(outBytes, data)

	return outBytes, nil
}

func (c *CapnpPeerClient) ListSpecialPieces(ctx context.Context) ([]rpc.PeerShardMeta, error) {
	req, release := c.clientStub.ListSpecialPieces(ctx, nil)
	defer release()

	res, err := req.Struct()
	if err != nil {
		return nil, fmt.Errorf("failed to read ListSpecialPieces results: %w", err)
	}

	capnpShards, err := res.Shards()
	if err != nil {
		return nil, err
	}

	var pieces []rpc.PeerShardMeta
	for i := 0; i < capnpShards.Len(); i++ {
		cs := capnpShards.At(i)
		hashBytes, _ := cs.Checksum()
		parentHash, _ := cs.ParentShardHash()
		pieces = append(pieces, rpc.PeerShardMeta{
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

func (c *CapnpPeerClient) Announce(ctx context.Context, listenAddr string, callback rpc.PeerNode) error {
	req, release := c.clientStub.Announce(ctx, func(p rpc.PeerNode_announce_Params) error {
		p.SetListenAddress(listenAddr)
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

func (c *CapnpPeerClient) Close() error {
	c.clientStub.Release()
	if err := c.rpcConn.Close(); err != nil {
		if c.stream != nil {
			c.stream.Close()
		}
		return err
	}
	if c.stream != nil {
		return c.stream.Close()
	}
	return nil
}