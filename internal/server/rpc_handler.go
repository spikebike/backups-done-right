package server

import (
	"context"
	"encoding/hex"
	"time"

	"capnproto.org/go/capnp/v3"
	capnpserver "capnproto.org/go/capnp/v3/server"
	"p2p-backup/internal/rpc"
)

// RPCHandler manages RPC calls from both clients (BackupServer) and other nodes (PeerNode).
type RPCHandler struct {
	engine       *Engine
	clientPubKey string
}

func NewRPCHandler(engine *Engine, clientPubKey string) *RPCHandler {
	return &RPCHandler{engine: engine, clientPubKey: clientPubKey}
}

func (h *RPCHandler) NewServer() *capnpserver.Server {
	methods := rpc.BackupServer_Methods(nil, &backupServerAdapter{h})
	methods = rpc.PeerNode_Methods(methods, &peerNodeAdapter{h})
	return capnpserver.New(methods, h, nil)
}

// --- Internal Helpers for Code Weight Reduction ---

func (h *RPCHandler) decodeMetadataList(list rpc.TransferMetadata_List) []rpc.Metadata {
	var metas []rpc.Metadata
	for i := 0; i < list.Len(); i++ {
		metas = append(metas, rpc.MetadataFromCapnp(list.At(i)))
	}
	return metas
}

func (h *RPCHandler) encodeMetadataList(seg *capnp.Segment, items []rpc.Metadata) (rpc.TransferMetadata_List, error) {
	list, err := rpc.NewTransferMetadata_List(seg, int32(len(items)))
	if err != nil {
		return rpc.TransferMetadata_List{}, err
	}
	for i, item := range items {
		cm, err := rpc.MetadataToCapnp(seg, item)
		if err != nil {
			return rpc.TransferMetadata_List{}, err
		}
		if err := list.Set(i, cm); err != nil {
			return rpc.TransferMetadata_List{}, err
		}
	}
	return list, nil
}

func (h *RPCHandler) decodeChecksumList(list capnp.DataList) []string {
	var hashes []string
	for i := 0; i < list.Len(); i++ {
		hashBytes, _ := list.At(i)
		hashes = append(hashes, hex.EncodeToString(hashBytes))
	}
	return hashes
}

func (h *RPCHandler) encodeChecksumList(seg *capnp.Segment, hashes []string) (capnp.DataList, error) {
	list, err := capnp.NewDataList(seg, int32(len(hashes)))
	if err != nil {
		return capnp.DataList{}, err
	}
	for i, hash := range hashes {
		hashBytes, _ := hex.DecodeString(hash)
		list.Set(i, hashBytes)
	}
	return list, nil
}

// --- Adapters ---

type backupServerAdapter struct{ *RPCHandler }
type peerNodeAdapter struct{ *RPCHandler }

// --- BackupServer Implementation ---

func (a *backupServerAdapter) OfferItems(ctx context.Context, call rpc.BackupServer_offerItems) error {
	list, _ := call.Args().Items()
	plan, err := a.engine.OfferItems(ctx, a.clientPubKey, a.decodeMetadataList(list))
	if err != nil {
		return err
	}
	res, _ := call.AllocResults()
	indices, _ := res.NewNeededIndices(int32(len(plan.NeededIndices)))
	for i, idx := range plan.NeededIndices {
		indices.Set(i, uint32(idx))
	}
	return nil
}

func (a *backupServerAdapter) UploadItems(ctx context.Context, call rpc.BackupServer_uploadItems) error {
	capnpItems, _ := call.Args().Items()
	var items []rpc.ItemData
	for i := 0; i < capnpItems.Len(); i++ {
		ci := capnpItems.At(i)
		meta, _ := ci.Meta()
		data, _ := ci.Data()
		items = append(items, rpc.ItemData{Meta: rpc.MetadataFromCapnp(meta), Data: data})
	}
	err := a.engine.UploadItems(ctx, a.clientPubKey, items)
	res, _ := call.AllocResults()
	res.SetSuccess(err == nil)
	if err != nil {
		res.SetError(err.Error())
	}
	return nil
}

func (a *backupServerAdapter) ListSpecialItems(ctx context.Context, call rpc.BackupServer_listSpecialItems) error {
	items, err := a.engine.ListSpecialItems(ctx, a.clientPubKey)
	if err != nil {
		return err
	}
	res, _ := call.AllocResults()
	list, err := a.encodeMetadataList(res.Segment(), items)
	if err == nil {
		res.SetItems(list)
	}
	return err
}

func (a *backupServerAdapter) DownloadItems(ctx context.Context, call rpc.BackupServer_downloadItems) error {
	list, _ := call.Args().Checksums()
	hashes := a.decodeChecksumList(list)
	found, missing, err := a.engine.GetItems(ctx, hashes)
	if err != nil {
		return err
	}
	res, _ := call.AllocResults()
	itemList, _ := res.NewItems(int32(len(found)))
	for i, item := range found {
		ci := itemList.At(i)
		cm, _ := rpc.MetadataToCapnp(ci.Segment(), item.Meta)
		ci.SetMeta(cm)
		ci.SetData(item.Data)
	}
	missingList, _ := a.encodeChecksumList(res.Segment(), missing)
	res.SetMissing(missingList)
	return nil
}

func (a *backupServerAdapter) GetStatus(ctx context.Context, call rpc.BackupServer_getStatus) error {
	status, err := a.engine.GetStatus(ctx)
	if err != nil {
		return err
	}
	res, _ := call.AllocResults()
	s, _ := res.NewStatus()
	s.SetUptimeSeconds(status.UptimeSeconds)
	s.SetTotalShards(status.TotalShards)
	s.SetFullyReplicatedShards(status.FullyReplicatedShards)
	s.SetPartiallyReplicatedShards(status.PartiallyReplicatedShards)
	s.SetHostedPeerShards(status.HostedPeerShards)
	s.SetQueuedBytes(status.QueuedBytes)
	return nil
}

func (a *backupServerAdapter) DeleteItems(ctx context.Context, call rpc.BackupServer_deleteItems) error {
	list, _ := call.Args().Checksums()
	hashes := a.decodeChecksumList(list)
	err := a.engine.DeleteItems(ctx, hashes)
	res, _ := call.AllocResults()
	res.SetSuccess(err == nil)
	return nil
}

func (a *backupServerAdapter) ListAllItems(ctx context.Context, call rpc.BackupServer_listAllItems) error {
	hashes, err := a.engine.ListAllItems(ctx)
	if err != nil {
		return err
	}
	res, _ := call.AllocResults()
	list, err := a.encodeChecksumList(res.Segment(), hashes)
	if err == nil {
		res.SetChecksums(list)
	}
	return err
}

func (a *backupServerAdapter) AddPeer(ctx context.Context, call rpc.BackupServer_addPeer) error {
	addr, _ := call.Args().Address()
	err := a.engine.AddPeer(ctx, addr)
	res, _ := call.AllocResults()
	res.SetSuccess(err == nil)
	return nil
}

func (a *backupServerAdapter) UpdatePeer(ctx context.Context, call rpc.BackupServer_updatePeer) error {
	args := call.Args()
	status, _ := args.Status()
	err := a.engine.UpdatePeer(ctx, int64(args.Id()), status, int64(args.MaxStorageSize()))
	res, _ := call.AllocResults()
	res.SetSuccess(err == nil)
	return nil
}

func (a *backupServerAdapter) ListPeers(ctx context.Context, call rpc.BackupServer_listPeers) error {
	peers, err := a.engine.ListPeers(ctx)
	if err != nil {
		return err
	}
	res, _ := call.AllocResults()
	list, _ := res.NewPeers(int32(len(peers)))
	for i, p := range peers {
		cp := list.At(i)
		cp.SetId(uint64(p.ID))
		cp.SetAddress(p.Address)
		cp.SetPublicKey(p.PublicKey)
		cp.SetStatus(p.Status)
		cp.SetFirstSeen(p.FirstSeen)
		cp.SetLastSeen(p.LastSeen)
		cp.SetMaxStorageSize(uint64(p.MaxStorageSize))
		cp.SetCurrentStorageSize(uint64(p.CurrentStorageSize))
		cp.SetOutboundStorageSize(uint64(p.OutboundStorageSize))
		cp.SetContactInfo(p.ContactInfo)
		cp.SetChallengesMade(p.ChallengesMade)
		cp.SetChallengesPassed(p.ChallengesPassed)
		cp.SetConnectionsOk(p.ConnectionsOk)
		cp.SetIntegrityAttempts(p.IntegrityAttempts)
		cp.SetTotalShards(p.TotalShards)
		cp.SetCurrentShards(p.CurrentShards)
		cp.SetSource(p.Source)
	}
	return nil
}

func (a *backupServerAdapter) ListClients(ctx context.Context, call rpc.BackupServer_listClients) error {
	clients, err := a.engine.ListClients(ctx)
	if err != nil {
		return err
	}
	res, _ := call.AllocResults()
	list, _ := res.NewClients(int32(len(clients)))
	for i, c := range clients {
		cc := list.At(i)
		cc.SetId(c.ID)
		cc.SetPublicKey(c.PublicKey)
		cc.SetStatus(c.Status)
		cc.SetLastSeen(c.LastSeen)
		cc.SetMaxStorageSize(c.MaxStorageSize)
		cc.SetCurrentStorageSize(c.CurrentStorageSize)
	}
	return nil
}

func (a *backupServerAdapter) UpdateClient(ctx context.Context, call rpc.BackupServer_updateClient) error {
	status, _ := call.Args().Status()
	err := a.engine.UpdateClient(ctx, a.clientPubKey, call.Args().Id(), status, call.Args().MaxStorageSize())
	res, _ := call.AllocResults()
	res.SetSuccess(err == nil)
	return nil
}

func (a *backupServerAdapter) AddClient(ctx context.Context, call rpc.BackupServer_addClient) error {
	pk, _ := call.Args().PublicKey()
	status, _ := call.Args().Status()
	err := a.engine.AddClient(ctx, a.clientPubKey, pk, status, call.Args().MaxStorageSize())
	res, _ := call.AllocResults()
	res.SetSuccess(err == nil)
	return nil
}

// --- PeerNode Implementation ---

func (a *peerNodeAdapter) OfferItems(ctx context.Context, call rpc.PeerNode_offerItems) error {
	list, _ := call.Args().Items()
	plan, err := a.engine.OfferItems(ctx, a.clientPubKey, a.decodeMetadataList(list))
	if err != nil {
		return err
	}
	res, _ := call.AllocResults()
	neededList, _ := res.NewNeededIndices(int32(len(plan.NeededIndices)))
	for i, idx := range plan.NeededIndices {
		neededList.Set(i, uint32(idx))
	}
	return nil
}

func (a *peerNodeAdapter) PrepareUpload(ctx context.Context, call rpc.PeerNode_prepareUpload) error {
	list, _ := call.Args().Items()
	err := a.engine.PreparePeerUpload(ctx, a.clientPubKey, a.decodeMetadataList(list))
	res, _ := call.AllocResults()
	res.SetSuccess(err == nil)
	return nil
}

func (a *peerNodeAdapter) ChallengePiece(ctx context.Context, call rpc.PeerNode_challengePiece) error {
	checksum, _ := call.Args().Checksum()
	data, err := a.engine.ChallengePiece(ctx, a.clientPubKey, checksum, call.Args().Offset())
	if err != nil {
		return err
	}
	res, _ := call.AllocResults()
	res.SetData(data)
	return nil
}

func (a *peerNodeAdapter) ReleasePiece(ctx context.Context, call rpc.PeerNode_releasePiece) error {
	checksum, _ := call.Args().Checksum()
	err := a.engine.ReleasePiece(ctx, checksum, a.clientPubKey)
	res, _ := call.AllocResults()
	res.SetSuccess(err == nil)
	return nil
}

func (a *peerNodeAdapter) ListSpecialItems(ctx context.Context, call rpc.PeerNode_listSpecialItems) error {
	items, err := a.engine.ListSpecialItems(ctx, a.clientPubKey)
	if err != nil {
		return err
	}
	res, _ := call.AllocResults()
	list, err := a.encodeMetadataList(res.Segment(), items)
	if err == nil {
		res.SetItems(list)
	}
	return err
}

func (a *peerNodeAdapter) Announce(ctx context.Context, call rpc.PeerNode_announce) error {
	args := call.Args()
	addr, _ := args.ListenAddress()
	contact, _ := args.ContactInfo()
	peerID, err := a.engine.AnnouncePeer(ctx, a.clientPubKey, addr, contact)
	if err == nil && peerID > 0 {
		callback := args.Callback()
		if callback.IsValid() {
			a.engine.RegisterActivePeer(peerID, callback.AddRef())

			// 2. Reciprocal Announce: Tell the initiator who WE are.
			// We call this synchronously so the initiator's database is updated
			// before this RPC returns success. We use a short timeout to prevent deadlocks.
			subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_, announceRel := callback.Announce(subCtx, func(p rpc.PeerNode_announce_Params) error {
				p.SetListenAddress(a.engine.ListenAddress)
				p.SetContactInfo(a.engine.ContactInfo)
				return nil
			})
			if announceRel != nil {
				announceRel()
			}
		}
	}
	res, _ := call.AllocResults()
	res.SetSuccess(err == nil)
	return nil
}
