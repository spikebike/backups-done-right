package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/multiformats/go-multiaddr"
)

const DiscoveryServiceTag = "bdr-v1.0"

func main() {
	bootstrapFlag := flag.String("bootstrap", "", "Optional multiaddr of a peer to bootstrap from")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Create a basic libp2p host
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		log.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	fmt.Printf("Scanner ID: %s\n", h.ID())

	// 2. Initialize DHT in Auto mode
	kademliaDHT, err := dht.New(ctx, h, dht.Mode(dht.ModeAuto))
	if err != nil {
		log.Fatalf("failed to create DHT: %v", err)
	}

	// 3. Bootstrap the DHT
	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		log.Fatalf("failed to bootstrap DHT: %v", err)
	}

	// Connect to public nodes
	bootstrapPeers := dht.DefaultBootstrapPeers
	for _, addr := range bootstrapPeers {
		pi, _ := peer.AddrInfoFromP2pAddr(addr)
		h.Connect(ctx, *pi)
	}

	// Connect to manual bootstrap peer if provided
	if *bootstrapFlag != "" {
		maddr, err := multiaddr.NewMultiaddr(*bootstrapFlag)
		if err != nil {
			log.Fatalf("invalid bootstrap multiaddr: %v", err)
		}
		pi, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			log.Fatalf("invalid bootstrap multiaddr: %v", err)
		}
		fmt.Printf("Connecting to manual bootstrap peer: %s\n", pi.ID)
		if err := h.Connect(ctx, *pi); err != nil {
			log.Printf("Warning: failed to connect to manual bootstrap peer: %v", err)
		}
	}

	// Wait a moment for bootstrap to find some routing table entries
	fmt.Println("Warming up DHT (15s)...")
	time.Sleep(15 * time.Second)

	routingDiscovery := routing.NewRoutingDiscovery(kademliaDHT)

	// 4. Look for peers with the service tag
	fmt.Printf("Searching for peers with tag '%s'...\n", DiscoveryServiceTag)
	
	for {
		peerChan, err := routingDiscovery.FindPeers(ctx, DiscoveryServiceTag)
		if err != nil {
			log.Printf("Error finding peers: %v", err)
			continue
		}

		found := 0
		for peer := range peerChan {
			if peer.ID == h.ID() {
				continue
			}
			found++
			fmt.Printf("Found Peer: %s\n", peer.ID)
			for _, addr := range peer.Addrs {
				fmt.Printf("  - %s\n", addr)
			}
		}

		if found == 0 {
			fmt.Println("No peers found yet. Retrying in 10s...")
		} else {
			fmt.Printf("Scan complete. Found %d peers. Refreshing in 30s...\n", found)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(30 * time.Second):
		}
	}
}
