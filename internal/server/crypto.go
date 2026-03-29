package server

import (
	"crypto/sha256"
	"io"

	"golang.org/x/crypto/hkdf"
)

// DerivePeerKey generates a unique symmetric encryption key for a specific peer
// using HKDF (HMAC-based Extract-and-Expand Key Derivation Function) with SHA-256.
func DerivePeerKey(masterServerKey []byte, peerPublicKey []byte) ([]byte, error) {
	// We use the master server key as the initial keying material (IKM),
	// the peer's public key (e.g., raw bytes or PEM) as the salt,
	// and a context-specific info string.
	kdf := hkdf.New(sha256.New, masterServerKey, peerPublicKey, []byte("p2p-backup-peer-encryption"))
	
	key := make([]byte, 32) // 32 bytes for XChaCha20-Poly1305 (or similar symmetric cipher)
	if _, err := io.ReadFull(kdf, key); err != nil {
		return nil, err
	}
	
	return key, nil
}
