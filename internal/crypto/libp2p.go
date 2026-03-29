package crypto

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/hex"
	"fmt"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Libp2pKeyFromEd25519 converts a standard Go ed25519 private key to a libp2p private key.
func Libp2pKeyFromEd25519(priv ed25519.PrivateKey) (libp2pcrypto.PrivKey, error) {
	return libp2pcrypto.UnmarshalEd25519PrivateKey(priv)
}

// PeerIDFromPubKeyHex converts our database's hex-encoded public key into a libp2p peer.ID
func PeerIDFromPubKeyHex(pubKeyHex string) (peer.ID, error) {
	pubKeyBytes, err := hex.DecodeString(pubKeyHex)
	if err != nil {
		return "", fmt.Errorf("invalid public key hex: %w", err)
	}

	// Our system historically uses x509.MarshalPKIXPublicKey which includes ASN.1 overhead (usually ~44 bytes)
	// libp2p expects the raw 32 byte ed25519 public key.
	if len(pubKeyBytes) > 32 {
		parsedKey, err := x509.ParsePKIXPublicKey(pubKeyBytes)
		if err == nil {
			if edKey, ok := parsedKey.(ed25519.PublicKey); ok {
				pubKeyBytes = edKey
			}
		}
	}

	libp2pPubKey, err := libp2pcrypto.UnmarshalEd25519PublicKey(pubKeyBytes)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal public key: %w", err)
	}

	return peer.IDFromPublicKey(libp2pPubKey)
}

// PubKeyHexFromPeerID extracts the ed25519 public key from a libp2p peer.ID and formats it as hex
// We format it as a PKIX public key to maintain compatibility with our existing database schema
func PubKeyHexFromPeerID(pid peer.ID) (string, error) {
	pubKey, err := pid.ExtractPublicKey()
	if err != nil {
		return "", fmt.Errorf("failed to extract public key from peer ID: %w", err)
	}

	rawBytes, err := pubKey.Raw()
	if err != nil {
		return "", fmt.Errorf("failed to get raw bytes from public key: %w", err)
	}

	pkixBytes, err := x509.MarshalPKIXPublicKey(ed25519.PublicKey(rawBytes))
	if err != nil {
		return "", fmt.Errorf("failed to marshal PKIX public key: %w", err)
	}

	return hex.EncodeToString(pkixBytes), nil
}
