package server

import (
	"context"
	"log"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	discoveryutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

const DiscoveryServiceTag = "bdr-v1.0"

// StartDiscoveryWorker initializes the DHT and periodically finds/announces peers.
func (e *Engine) StartDiscoveryWorker(ctx context.Context, mode string) {
	var dhtOpts []dht.Option
	if mode == "client" {
		dhtOpts = append(dhtOpts, dht.Mode(dht.ModeClient))
	} else {
		dhtOpts = append(dhtOpts, dht.Mode(dht.ModeServer))
	}

	// 1. Initialize DHT
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

	// 3. Connect to initial bootstrap peers if provided in config or use default libp2p ones
	// For now, we rely on manual addpeer to seed the routing table, or we can add public ones.

	routingDiscovery := routing.NewRoutingDiscovery(kademliaDHT)

	// 4. Periodically Advertise ourselves and find others
	go func() {
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
					
					// Attempt to connect to discovered peers.
					// libp2p's Host.Connect is smart and won't re-dial existing connections.
					if err := e.Host.Connect(ctx, peer); err == nil {
						if e.Verbose {
							log.Printf("DiscoveryWorker: Successfully connected to discovered peer: %s", peer.ID.String())
						}
						
						// Once connected, our RPC handler will eventually see them
						// via Announce or they can be manually added.
						// We don't auto-register them in the DB yet to avoid cluttering 
						// until they actually trade data or the user trusts them.
					}
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(15 * time.Minute):
				// Re-discovery cycle
			}
		}
	}()
}
