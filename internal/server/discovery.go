package server

import (
	"context"
	"log"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	discoveryutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/multiformats/go-multiaddr"
	"p2p-backup/internal/crypto"
)

const DiscoveryServiceTag = "bdr-v1.0"

// StartDiscoveryWorker initializes the DHT and periodically finds/announces peers.
func (e *Engine) StartDiscoveryWorker(ctx context.Context) {
	var dhtOpts []dht.Option
	// 1. Initialize DHT
	// We use ModeAuto so that nodes with public IPs (like VPS) can become 
	// DHT servers and participate in the routing table, making them much 
	// easier to find. Nodes behind NAT will remain in client mode.
	dhtOpts = append(dhtOpts, dht.Mode(dht.ModeAuto))
	kademliaDHT, err := dht.New(ctx, e.Host, dhtOpts...)
	if err != nil {
		log.Printf("DiscoveryWorker: failed to create DHT: %v", err)
		return
	}

	// 2. Bootstrap the DHT
	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		log.Printf("DiscoveryWorker: failed to bootstrap DHT: %v", err)
		return
	}

	// Share DHT globally for peer tracking fallbacks
	e.DHT = kademliaDHT

	// 3. Connect to initial bootstrap peers if provided in config or use default libp2p ones
	// For now, we rely on manual addpeer to seed the routing table, or we can add public ones.

	routingDiscovery := routing.NewRoutingDiscovery(kademliaDHT)

	// 4. Periodically Advertise ourselves and find others
	go func() {
		// Wait 30s initially to warm up the DHT routing table
		// before we try to announce ourselves.
		time.Sleep(30 * time.Second)

		for {
			// Advertise our presence
			discoveryutil.Advertise(ctx, routingDiscovery, DiscoveryServiceTag)

			if e.Verbose {
				log.Printf("DiscoveryWorker: Announced our presence to the DHT (%s)", DiscoveryServiceTag)
			}

			// Find others
			peerChan, err := routingDiscovery.FindPeers(ctx, DiscoveryServiceTag)
			if err != nil {
				log.Printf("DiscoveryWorker: failed to find peers: %v", err)
			} else {
				for peer := range peerChan {
					if peer.ID == e.Host.ID() {
						continue
					}

					// Attempt to connect to discovered peers and trigger a handshake.
					// We use RegisterAndHandshakeDHT which handles the DB entry 
					// and metadata exchange.
					if err := e.RegisterAndHandshakeDHT(ctx, peer); err == nil {
						if e.Verbose {
							log.Printf("DiscoveryWorker: Successfully connected and handshaked with discovered peer: %s", peer.ID.String())
						}
					}
				}
			}

			// 5. Retry known peers from database (manually added or previous acquaintances)
			e.retryKnownPeers(ctx)

			select {
			case <-ctx.Done():
				return
			case <-time.After(15 * time.Minute):
				// Re-discovery cycle
			}
		}
	}()
}

func (e *Engine) retryKnownPeers(ctx context.Context) {
	// Query peers that haven't been seen in 15 minutes or never
	// We prioritize those manually added or storing our shards
	rows, err := e.DB.QueryContext(ctx, "SELECT id, ip_address, public_key, first_seen, is_manual, current_shards FROM peers WHERE last_seen IS NULL OR last_seen < datetime('now', '-15 minutes')")
	if err != nil {
		log.Printf("DiscoveryWorker: failed to query known peers for retry: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var addrStr, pubKeyHex, firstSeenStr string
		var isManual int
		var currentShards int
		if err := rows.Scan(&id, &addrStr, &pubKeyHex, &firstSeenStr, &isManual, &currentShards); err != nil {
			continue
		}

		pid, err := crypto.PeerIDFromPubKeyHex(pubKeyHex)
		if err != nil {
			continue
		}

		maddr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			continue
		}

		peerInfo := peer.AddrInfo{
			ID:    pid,
			Addrs: []multiaddr.Multiaddr{maddr},
		}

		// Attempt to connect to last known address
		if err := e.Host.Connect(ctx, peerInfo); err != nil {
			// If dial fails, and they are essential (manual or data-bearing), try the DHT fallback
			if (isManual == 1 || currentShards > 0) && e.DHT != nil {
				if e.Verbose {
					log.Printf("DiscoveryWorker: Essential peer %s not reachable at %s, searching DHT...", pubKeyHex, addrStr)
				}
				newInfo, dhtErr := e.DHT.FindPeer(ctx, pid)
				if dhtErr == nil && len(newInfo.Addrs) > 0 {
					// Found! Update DB with the first valid address and retry connection
					newAddr := newInfo.Addrs[0].String()
					if e.Verbose {
						log.Printf("DiscoveryWorker: Found new address for peer %s: %s", pubKeyHex, newAddr)
					}
					_, _ = e.DB.ExecContext(ctx, "UPDATE peers SET ip_address = ? WHERE id = ?", newAddr, id)

					// Re-dial with full info from DHT
					if err := e.Host.Connect(ctx, newInfo); err == nil {
						if e.Verbose {
							log.Printf("DiscoveryWorker: Successfully connected to moved peer %s via DHT", pubKeyHex)
						}
						_, _ = e.DB.ExecContext(ctx, "UPDATE peers SET last_seen = CURRENT_TIMESTAMP WHERE id = ?", id)
						continue
					}
				}
			}

			// Failed both direct and DHT fallbacks. Check for 72-hour eviction.
			firstSeen, _ := time.Parse("2006-01-02 15:04:05", firstSeenStr)
			if !firstSeen.IsZero() && time.Since(firstSeen) > 72*time.Hour {
				log.Printf("DiscoveryWorker: Evicting peer %s after 72 hours of failed connection attempts", pubKeyHex)
				_, _ = e.DB.ExecContext(ctx, "DELETE FROM peers WHERE id = ?", id)
			}
			continue
		}

		// Success! Mark as seen 
		if e.Verbose {
			log.Printf("DiscoveryWorker: Successfully reconnected to known peer %s", pubKeyHex)
		}
		_, _ = e.DB.ExecContext(ctx, "UPDATE peers SET last_seen = CURRENT_TIMESTAMP WHERE id = ?", id)
	}
}
