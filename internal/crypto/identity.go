package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	"github.com/tyler-smith/go-bip39"
	"golang.org/x/crypto/hkdf"
	"lukechampine.com/blake3"
	"hash"
)

func blake3Wrapper() hash.Hash {
	return blake3.New(32, nil)
}

// GenerateMnemonic creates a new 24-word recovery phrase.
func GenerateMnemonic() (string, error) {
	entropy, err := bip39.NewEntropy(256)
	if err != nil {
		return "", err
	}
	return bip39.NewMnemonic(entropy)
}

// DerivedIdentity holds the deterministic keys generated from a mnemonic.
type DerivedIdentity struct {
	TLSCert   tls.Certificate
	MasterKey []byte
}

// DeriveIdentity generates a TLS certificate and a master encryption key from a BIP-39 mnemonic.
func DeriveIdentity(mnemonic string, isServer bool) (*DerivedIdentity, error) {
	if !bip39.IsMnemonicValid(mnemonic) {
		return nil, fmt.Errorf("invalid mnemonic")
	}

	// 1. Generate Seed from Mnemonic
	seed := bip39.NewSeed(mnemonic, "") // Optional passphrase could be added here

	// 2. Derive TLS Private Key (Ed25519)
	// We use HKDF to derive a 32-byte seed for Ed25519 from the 64-byte BIP39 seed.
	tlsSeedReader := hkdf.New(blake3Wrapper, seed, nil, []byte("p2p-backup-tls-identity"))
	tlsSeed := make([]byte, 32)
	if _, err := tlsSeedReader.Read(tlsSeed); err != nil {
		return nil, err
	}
	privKey := ed25519.NewKeyFromSeed(tlsSeed)

	// 3. Derive Master Encryption Key
	masterKeyReader := hkdf.New(blake3Wrapper, seed, nil, []byte("p2p-backup-master-encryption"))
	masterKey := make([]byte, 32)
	if _, err := masterKeyReader.Read(masterKey); err != nil {
		return nil, err
	}

	// 4. Create a Self-Signed TLS Certificate
	commonName := "P2P-Backup-Client"
	if isServer {
		commonName = "P2P-Backup-Server"
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().Unix()),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(100 * 365 * 24 * time.Hour), // 100 years
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, privKey.Public(), privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate: %w", err)
	}

	// Create PEM blocks
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	
	// For Ed25519, we need to marshal the private key to PKCS8
	privBytes, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	return &DerivedIdentity{
		TLSCert:   tlsCert,
		MasterKey: masterKey,
	}, nil
}
