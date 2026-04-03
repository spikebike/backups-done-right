package server

import (
	"context"
	"encoding/hex"
	"fmt"

	"p2p-backup/internal/rpc" // this imports schema.capnp.go
)

type RPCHandler struct {
	engine       *Engine
	clientPubKey string
}

func NewRPCHandler(engine *Engine, clientPubKey string) *RPCHandler {
	return &RPCHandler{engine: engine, clientPubKey: clientPubKey}
}

// --- BackupServer Methods ---

func (h *RPCHandler) OfferBlobs(ctx context.Context, call rpc.BackupServer_offerBlobs) error {
	args := call.Args()
	capnpBlobs, err := args.Blobs()
	if err != nil {
		return err
	}

	var metaBlobs []rpc.BlobMeta
	for i := 0; i < capnpBlobs.Len(); i++ {
		cb := capnpBlobs.At(i)
		hashBytes, _ := cb.Checksum()
		metaBlobs = append(metaBlobs, rpc.BlobMeta{
			Hash:    hex.EncodeToString(hashBytes),
			Size:    int64(cb.Size()),
			Special: cb.Special(),
		})
	}

	neededIndices, err := h.engine.OfferBlobs(ctx, h.clientPubKey, metaBlobs)
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}
	
	neededList, err := res.NewNeededIndices(int32(len(neededIndices)))
	if err != nil {
		return err
	}
	for i, idx := range neededIndices {
		neededList.Set(i, idx)
	}

	return nil
}

func (h *RPCHandler) UploadBlobs(ctx context.Context, call rpc.BackupServer_uploadBlobs) error {
	args := call.Args()
	capnpBlobs, err := args.Blobs()
	if err != nil {
		return err
	}

	var dataBlobs []rpc.LocalBlobData
	for i := 0; i < capnpBlobs.Len(); i++ {
		cb := capnpBlobs.At(i)
		hashBytes, _ := cb.Checksum()
		dataBytes, _ := cb.Data()
		dataBlobs = append(dataBlobs, rpc.LocalBlobData{
			Hash: hex.EncodeToString(hashBytes),
			Data: dataBytes,
		})
	}

	err = h.engine.UploadBlobs(ctx, h.clientPubKey, dataBlobs)
	
	res, allocErr := call.AllocResults()
	if allocErr != nil {
		return allocErr
	}
	
	if err != nil {
		res.SetSuccess(false)
		res.SetError(err.Error())
	} else {
		res.SetSuccess(true)
	}

	return nil
}

func (h *RPCHandler) ListSpecialBlobs(ctx context.Context, call rpc.BackupServer_listSpecialBlobs) error {
	specialBlobs, err := h.engine.ListSpecialBlobs(ctx)
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	blobList, err := res.NewSpecialBlobs(int32(len(specialBlobs)))
	if err != nil {
		return err
	}

	for i, sb := range specialBlobs {
		cb := blobList.At(i)
		hashBytes, _ := hex.DecodeString(sb.Hash)
		cb.SetChecksum(hashBytes)
		cb.SetSize(uint64(sb.Size))
		cb.SetSpecial(sb.Special)
	}

	return nil
}

func (h *RPCHandler) DownloadBlobs(ctx context.Context, call rpc.BackupServer_downloadBlobs) error {
	args := call.Args()
	capnpHashes, err := args.Checksums()
	if err != nil {
		return err
	}

	var hashes []string
	for i := 0; i < capnpHashes.Len(); i++ {
		hashBytes, _ := capnpHashes.At(i)
		hashes = append(hashes, hex.EncodeToString(hashBytes))
	}

	foundBlobs, missingHashes, err := h.engine.GetBlobs(ctx, hashes)
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	resBlobs, err := res.NewBlobs(int32(len(foundBlobs)))
	if err != nil {
		return err
	}
	for i, b := range foundBlobs {
		cb := resBlobs.At(i)
		hashBytes, _ := hex.DecodeString(b.Hash)
		if err := cb.SetChecksum(hashBytes); err != nil {
			return fmt.Errorf("failed to set checksum for blob %d: %w", i, err)
		}
		if err := cb.SetData(b.Data); err != nil {
			return fmt.Errorf("failed to set data for blob %d (len %d): %w", i, len(b.Data), err)
		}
	}

	resMissing, err := res.NewMissing(int32(len(missingHashes)))
	if err != nil {
		return err
	}
	for i, mh := range missingHashes {
		hashBytes, _ := hex.DecodeString(mh)
		resMissing.Set(i, hashBytes)
	}

	return nil
}

func (h *RPCHandler) GetStatus(ctx context.Context, call rpc.BackupServer_getStatus) error {
	status, err := h.engine.GetStatus(ctx)
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	resStatus, err := res.NewStatus()
	if err != nil {
		return err
	}

	resStatus.SetUptimeSeconds(status.UptimeSeconds)
	resStatus.SetTotalShards(status.TotalShards)
	resStatus.SetFullyReplicatedShards(status.FullyReplicatedShards)
	resStatus.SetPartiallyReplicatedShards(status.PartiallyReplicatedShards)
	resStatus.SetHostedPeerShards(status.HostedPeerShards)
	resStatus.SetQueuedBytes(status.QueuedBytes)

	return nil
}

func (h *RPCHandler) DeleteBlobs(ctx context.Context, call rpc.BackupServer_deleteBlobs) error {
	args := call.Args()
	capnpHashes, err := args.Checksums()
	if err != nil {
		return err
	}

	var hashes []string
	for i := 0; i < capnpHashes.Len(); i++ {
		hashBytes, _ := capnpHashes.At(i)
		hashes = append(hashes, hex.EncodeToString(hashBytes))
	}

	err = h.engine.DeleteBlobs(ctx, hashes)

	res, allocErr := call.AllocResults()
	if allocErr != nil {
		return allocErr
	}

	if err != nil {
		res.SetSuccess(false)
		res.SetError(err.Error())
	} else {
		res.SetSuccess(true)
	}

	return nil
}

func (h *RPCHandler) ListAllBlobs(ctx context.Context, call rpc.BackupServer_listAllBlobs) error {
	hashes, err := h.engine.ListAllBlobs(ctx)
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	checksumList, err := res.NewChecksums(int32(len(hashes)))
	if err != nil {
		return err
	}

	for i, h := range hashes {
		hashBytes, _ := hex.DecodeString(h)
		checksumList.Set(i, hashBytes)
	}

	return nil
}

func (h *RPCHandler) AddPeer(ctx context.Context, call rpc.BackupServer_addPeer) error {
	args := call.Args()
	address, err := args.Address()
	if err != nil {
		return err
	}

	err = h.engine.AddPeer(ctx, address)

	res, allocErr := call.AllocResults()
	if allocErr != nil {
		return allocErr
	}

	if err != nil {
		res.SetSuccess(false)
		res.SetError(err.Error())
	} else {
		res.SetSuccess(true)
	}

	return nil
}

func (h *RPCHandler) UpdatePeer(ctx context.Context, call rpc.BackupServer_updatePeer) error {
	args := call.Args()
	id := args.Id()
	status, err := args.Status()
	if err != nil {
		return err
	}
	maxSize := args.MaxStorageSize()

	err = h.engine.UpdatePeer(ctx, int64(id), status, int64(maxSize))

	res, allocErr := call.AllocResults()
	if allocErr != nil {
		return allocErr
	}

	if err != nil {
		res.SetSuccess(false)
		res.SetError(err.Error())
	} else {
		res.SetSuccess(true)
	}

	return nil
}

func (h *RPCHandler) ListPeers(ctx context.Context, call rpc.BackupServer_listPeers) error {
	peers, err := h.engine.ListPeers(ctx)
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	peerList, err := res.NewPeers(int32(len(peers)))
	if err != nil {
		return err
	}

	for i, p := range peers {
		cp := peerList.At(i)
		cp.SetId(uint64(p.ID))
		cp.SetAddress(p.Address)
		cp.SetPublicKey(p.PublicKey)
		cp.SetStatus(p.Status)
		cp.SetFirstSeen(p.FirstSeen)
		cp.SetLastSeen(p.LastSeen)
		cp.SetMaxStorageSize(uint64(p.MaxStorageSize))
		cp.SetCurrentStorageSize(uint64(p.CurrentStorageSize))
		cp.SetOutboundStorageSize(uint64(p.OutboundStorageSize))
		cp.SetChallengesMade(p.ChallengesMade)
		cp.SetChallengesPassed(p.ChallengesPassed)
		cp.SetConnectionsOk(p.ConnectionsOk)
		cp.SetIntegrityAttempts(p.IntegrityAttempts)
		cp.SetContactInfo(p.ContactInfo)
		cp.SetTotalShards(p.TotalShards)
		cp.SetCurrentShards(p.CurrentShards)
	}

	return nil
}

func (h *RPCHandler) ListClients(ctx context.Context, call rpc.BackupServer_listClients) error {
	clients, err := h.engine.ListClients(ctx)
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	clientList, err := res.NewClients(int32(len(clients)))
	if err != nil {
		return err
	}

	for i, c := range clients {
		cc := clientList.At(i)
		cc.SetId(c.ID)
		cc.SetPublicKey(c.PublicKey)
		cc.SetStatus(c.Status)
		cc.SetLastSeen(c.LastSeen)
		cc.SetMaxStorageSize(c.MaxStorageSize)
		cc.SetCurrentStorageSize(c.CurrentStorageSize)
	}

	return nil
}

func (h *RPCHandler) UpdateClient(ctx context.Context, call rpc.BackupServer_updateClient) error {
	args := call.Args()
	id := args.Id()
	status, err := args.Status()
	if err != nil {
		return err
	}
	maxSize := args.MaxStorageSize()

	err = h.engine.UpdateClient(ctx, h.clientPubKey, id, status, maxSize)

	res, allocErr := call.AllocResults()
	if allocErr != nil {
		return allocErr
	}

	if err != nil {
		res.SetSuccess(false)
		res.SetError(err.Error())
	} else {
		res.SetSuccess(true)
	}

	return nil
}

func (h *RPCHandler) AddClient(ctx context.Context, call rpc.BackupServer_addClient) error {
	args := call.Args()
	pk, err := args.PublicKey()
	if err != nil {
		return err
	}
	status, err := args.Status()
	if err != nil {
		return err
	}
	maxSize := args.MaxStorageSize()

	err = h.engine.AddClient(ctx, h.clientPubKey, pk, status, maxSize)

	res, allocErr := call.AllocResults()
	if allocErr != nil {
		return allocErr
	}

	if err != nil {
		res.SetSuccess(false)
		res.SetError(err.Error())
	} else {
		res.SetSuccess(true)
	}

	return nil
}

// --- PeerNode Methods ---

func (h *RPCHandler) OfferShards(ctx context.Context, call rpc.PeerNode_offerShards) error {
	args := call.Args()
	capnpShards, err := args.Shards()
	if err != nil {
		return err
	}

	var metaShards []rpc.PeerShardMeta
	for i := 0; i < capnpShards.Len(); i++ {
		cs := capnpShards.At(i)
		hashBytes, _ := cs.Checksum()
		parentHash, _ := cs.ParentShardHash()
		metaShards = append(metaShards, rpc.PeerShardMeta{
			Hash:            hex.EncodeToString(hashBytes),
			Size:            int64(cs.Size()),
			IsSpecial:       cs.IsSpecial(),
			PieceIndex:      int(cs.PieceIndex()),
			ParentShardHash: hex.EncodeToString(parentHash),
			SequenceNumber:  cs.SequenceNumber(),
			TotalPieces:     int(cs.TotalPieces()),
		})
	}

	neededIndices, err := h.engine.OfferShards(ctx, h.clientPubKey, metaShards)
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}
	
	neededList, err := res.NewNeededIndices(int32(len(neededIndices)))
	if err != nil {
		return err
	}
	for i, idx := range neededIndices {
		neededList.Set(i, idx)
	}

	return nil
}

func (h *RPCHandler) UploadShards(ctx context.Context, call rpc.PeerNode_uploadShards) error {
	args := call.Args()
	capnpShards, err := args.Shards()
	if err != nil {
		return err
	}

	var dataShards []rpc.LocalBlobData
	for i := 0; i < capnpShards.Len(); i++ {
		cs := capnpShards.At(i)
		hashBytes, err := cs.Checksum()
		if err != nil {
			return fmt.Errorf("failed to read shard %d checksum: %w", i, err)
		}
		dataBytes, err := cs.Data()
		if err != nil {
			return fmt.Errorf("failed to read shard %d data: %w", i, err)
		}
		parentHash, _ := cs.ParentShardHash()
		dataShards = append(dataShards, rpc.LocalBlobData{
			Hash:            hex.EncodeToString(hashBytes),
			Data:            dataBytes,
			IsSpecial:       cs.IsSpecial(),
			PieceIndex:      int(cs.PieceIndex()),
			ParentShardHash: hex.EncodeToString(parentHash),
			SequenceNumber:  cs.SequenceNumber(),
			TotalPieces:     int(cs.TotalPieces()),
		})
	}

	err = h.engine.UploadShards(ctx, h.clientPubKey, dataShards)
	
	res, allocErr := call.AllocResults()
	if allocErr != nil {
		return allocErr
	}
	
	if err != nil {
		res.SetSuccess(false)
		res.SetError(err.Error())
	} else {
		res.SetSuccess(true)
	}

	return nil
}

func (h *RPCHandler) ChallengePiece(ctx context.Context, call rpc.PeerNode_challengePiece) error {
	args := call.Args()
	checksum, err := args.ShardChecksum()
	if err != nil {
		return err
	}
	offset := args.Offset()

	data, err := h.engine.ChallengePiece(ctx, h.clientPubKey, checksum, offset)
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}
	
	err = res.SetData(data)
	return err
}

func (h *RPCHandler) ReleasePiece(ctx context.Context, call rpc.PeerNode_releasePiece) error {
	args := call.Args()
	checksum, err := args.ShardChecksum()
	if err != nil {
		return err
	}

	err = h.engine.ReleasePiece(ctx, checksum, h.clientPubKey)

	res, allocErr := call.AllocResults()
	if allocErr != nil {
		return allocErr
	}

	if err != nil {
		res.SetSuccess(false)
		res.SetError(err.Error())
	} else {
		res.SetSuccess(true)
	}

	return nil
}

func (h *RPCHandler) DownloadPiece(ctx context.Context, call rpc.PeerNode_downloadPiece) error {
	args := call.Args()
	checksum, err := args.ShardChecksum()
	if err != nil {
		return err
	}

	data, err := h.engine.DownloadPiece(ctx, checksum)
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	return res.SetData(data)
}

func (h *RPCHandler) ListSpecialPieces(ctx context.Context, call rpc.PeerNode_listSpecialPieces) error {
	pieces, err := h.engine.ListSpecialPieces(ctx, h.clientPubKey)
	if err != nil {
		return err
	}

	res, err := call.AllocResults()
	if err != nil {
		return err
	}

	capnpShards, err := res.NewShards(int32(len(pieces)))
	if err != nil {
		return err
	}

	for i, p := range pieces {
		cs := capnpShards.At(i)
		hashBytes, _ := hex.DecodeString(p.Hash)
		_ = cs.SetChecksum(hashBytes)
		cs.SetSize(uint64(p.Size))
		cs.SetIsSpecial(true)
		cs.SetPieceIndex(uint32(p.PieceIndex))
		cs.SetSequenceNumber(p.SequenceNumber)
		cs.SetTotalPieces(uint32(p.TotalPieces))
		parentHashBytes, _ := hex.DecodeString(p.ParentShardHash)
		_ = cs.SetParentShardHash(parentHashBytes)
	}

	return nil
}

func (h *RPCHandler) Announce(ctx context.Context, call rpc.PeerNode_announce) error {
	args := call.Args()
	addr, err := args.ListenAddress()
	if err != nil {
		return err
	}
	contact, _ := args.ContactInfo()

	peerID, err := h.engine.AnnouncePeer(ctx, h.clientPubKey, addr, contact)
	if err == nil && peerID > 0 {
		callback := args.Callback()
		if callback.IsValid() {
			// Register this active connection so our OutboundWorker can dial back immediately
			// without needing to establish a new TCP connection (solves NAT issues).
			// We need to add a ref to it so it doesn't get released when the handler returns.
			h.engine.RegisterActivePeer(peerID, callback.AddRef())
		}
	}

	res, allocErr := call.AllocResults()
	if allocErr != nil {
		return allocErr
	}
	res.SetSuccess(err == nil)
	return nil
}
