package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"crypto/ed25519"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"encoding/asn1"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:8081")
	if err != nil {
		fmt.Println("Dial err:", err)
		return
	}
	defer conn.Close()

	// multistream-select handshake
	_, err = conn.Write([]byte("\x13/multistream/1.0.0\n"))
	if err != nil { return }
	
	buf := make([]byte, 1024)
	conn.Read(buf)
	
	// request tls
	_, err = conn.Write([]byte("\x0b/tls/1.0.0\n"))
	if err != nil { return }
	conn.Read(buf) // read ack

	tlsConn := tls.Client(conn, &tls.Config{
		InsecureSkipVerify: true,
		NextProtos: []string{"libp2p"},
	})
	
	if err := tlsConn.Handshake(); err != nil {
		fmt.Println("TLS Handshake err:", err)
		return
	}
	
	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) > 0 {
		cert := state.PeerCertificates[0]
		
		extensionOID := asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 53594, 1, 1}
		for _, ext := range cert.Extensions {
			if ext.Id.Equal(extensionOID) {
				pubKey, err := libp2pcrypto.UnmarshalPublicKey(ext.Value)
				if err == nil {
					pid, _ := peer.IDFromPublicKey(pubKey)
					fmt.Printf("/ip4/127.0.0.1/tcp/8081/p2p/%s\n", pid.String())
					return
				}
			}
		}

		if edKey, ok := cert.PublicKey.(ed25519.PublicKey); ok {
			pubKey, _ := libp2pcrypto.UnmarshalEd25519PublicKey(edKey)
			pid, _ := peer.IDFromPublicKey(pubKey)
			fmt.Printf("/ip4/127.0.0.1/tcp/8081/p2p/%s\n", pid.String())
			return
		}
	}
	fmt.Println("Could not find Peer ID")
}
