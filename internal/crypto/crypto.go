package crypto

import (
	"errors"
	"crypto/rand"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"
	"lukechampine.com/blake3"
)

const (
	// KeySize is the required size for XChaCha20-Poly1305 keys (256 bits)
	KeySize = chacha20poly1305.KeySize
	// NonceSizeX is the size of the XChaCha20 nonce (192 bits)
	NonceSizeX = chacha20poly1305.NonceSizeX
)

var (
	ErrInvalidCiphertext = errors.New("ciphertext too short")
)

// DeriveKey uses Argon2id to derive a 32-byte encryption key from a password.
// In a real deployment, the salt should be generated once randomly and stored
// in the SQLite database or config file.
func DeriveKey(password string, salt []byte) []byte {
	// Argon2id recommended parameters:
	// time: 1 iteration, memory: 64MB, threads: 4, key length: 32 bytes
	return argon2.IDKey([]byte(password), salt, 1, 64*1024, 4, KeySize)
}

// Encrypt secures the plaintext using XChaCha20-Poly1305.
// It uses the first 24 bytes of the plaintext's BLAKE3 hash as the nonce.
// This provides deterministic encryption (Convergent Encryption) for deduplication.
func Encrypt(key []byte, plaintext []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}

	// Generate BLAKE3 hash of the plaintext
	hash := Hash(plaintext)

	// Use the first 24 bytes (192 bits) of the hash as the nonce
	nonce := hash[:NonceSizeX]

	// Seal appends the encrypted data and authentication tag to the nonce.
	// This means our final byte slice is: [24-byte nonce][encrypted data][16-byte tag]
	ciphertext := aead.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// EncryptTo is like Encrypt but writes into a caller-provided buffer to avoid
// heap allocation. The dst slice is reset to length 0 but its backing array is
// reused when capacity is sufficient. Returns the ciphertext slice (a sub-slice
// of dst's backing array) and its length.
func EncryptTo(key []byte, plaintext []byte, dst []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}

	hash := Hash(plaintext)
	nonce := hash[:NonceSizeX]

	// Total output: nonce (24) + plaintext + poly1305 tag (16)
	needed := NonceSizeX + len(plaintext) + aead.Overhead()

	// Reuse dst's backing array if it has enough capacity
	if cap(dst) >= needed {
		dst = dst[:NonceSizeX]
	} else {
		dst = make([]byte, NonceSizeX, needed)
	}
	copy(dst, nonce)

	// Seal appends encrypted data + tag after the nonce
	ciphertext := aead.Seal(dst, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt extracts the nonce and decrypts the XChaCha20-Poly1305 ciphertext.
func Decrypt(key []byte, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < NonceSizeX {
		return nil, ErrInvalidCiphertext
	}

	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}

	// Extract the 24-byte nonce from the beginning
	nonce := ciphertext[:NonceSizeX]
	encryptedData := ciphertext[NonceSizeX:]

	// Open authenticates and decrypts the data
	plaintext, err := aead.Open(nil, nonce, encryptedData, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

// Hash generates a 32-byte BLAKE3 checksum for the given data.
// This is used for both plaintext deduplication and encrypted blob addressing.
func Hash(data []byte) []byte {
	hash := blake3.Sum256(data)
	return hash[:]
}

// ReadRand fills the buffer with cryptographically secure random bytes.
func ReadRand(b []byte) (int, error) {
	return rand.Read(b)
}
