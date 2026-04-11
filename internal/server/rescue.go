package server

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"github.com/libp2p/go-libp2p/core/peer"
	"p2p-backup/internal/crypto"
	"p2p-backup/internal/rpc"
)

// AttemptRescue attempts to recover the server database from a peer that has a mirrored copy of the special metadata shard.
func (e *Engine) AttemptRescue(ctx context.Context, peer rpc.PeerNode, pid peer.ID, destDir string) error {
	if e.Verbose {
		log.Println("RESCUE: Attempting to recover database from peer...")
	}

	// 1. Get special pieces from peer
	req, release := peer.ListSpecialItems(ctx, nil)
	defer release()
	res, err := req.Struct()
	if err != nil {
		return fmt.Errorf("list special items: %w", err)
	}
	items, _ := res.Items()
	if items.Len() == 0 {
		return fmt.Errorf("no special pieces found on peer")
	}

	// 2. Identify the newest mirrored shard
	var highestSeq uint64
	var targetPiece rpc.Metadata
	found := false
	for i := 0; i < items.Len(); i++ {
		s := rpc.MetadataFromCapnp(items.At(i))
		if s.IsSpecial && (s.SequenceNumber > highestSeq || !found) {
			highestSeq = s.SequenceNumber
			targetPiece = s
			found = true
		}
	}

	if !found {
		return fmt.Errorf("no special mirrored shards found")
	}

	// 3. Download the piece
	hashHex := targetPiece.Hash

	buf := new(bytes.Buffer)
	err = e.PullPieceRaw(ctx, pid, hashHex, buf)
	if err != nil {
		return fmt.Errorf("pull piece direct: %w", err)
	}
	data := buf.Bytes()

	if e.Verbose {
		log.Printf("RESCUE: Downloaded %d bytes from peer", len(data))
	}

	// Trim trailing zeros added by shard padding
	// AEAD tags are random, so they are extremely unlikely to end in many zeros.
	actualData := data
	for len(actualData) > 0 && actualData[len(actualData)-1] == 0 {
		actualData = actualData[:len(actualData)-1]
	}

	if e.Verbose {
		log.Printf("RESCUE: Trimmed data length: %d", len(actualData))
	}

	// 3.5 Verify Signature (first 64 bytes)
	if len(actualData) < 64 {
		return fmt.Errorf("recovery shard is too small to contain signature")
	}
	signature := actualData[:64]
	ciphertext := actualData[64:]

	if !crypto.VerifyRecoveryShard(e.MasterKey, signature, ciphertext) {
		return fmt.Errorf("recovery shard signature verification failed (corruption or malicious peer)")
	}

	// 4. Decrypt Bundle
	decryptedBundle, err := crypto.Decrypt(e.MasterKey, ciphertext)
	if err != nil {
		return fmt.Errorf("bundle decryption failed (wrong mnemonic or corruption): %w", err)
	}

	// 5. Unpack Bundle
	decoder, err := zstd.NewReader(bytes.NewReader(decryptedBundle))
	if err != nil {
		return fmt.Errorf("zstd init failed: %w", err)
	}
	defer decoder.Close()

	tr := tar.NewReader(decoder)

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination dir: %w", err)
	}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar read error: %w", err)
		}

		outPath := filepath.Join(destDir, hdr.Name)
		outFile, err := os.Create(outPath)
		if err != nil {
			return fmt.Errorf("failed to create recovered file %s: %w", outPath, err)
		}
		
		if _, err := io.Copy(outFile, tr); err != nil {
			outFile.Close()
			return fmt.Errorf("failed to write data to %s: %w", outPath, err)
		}
		outFile.Close()
		
		if e.Verbose {
			log.Printf("RESCUE: Recovered file: %s", outPath)
		}
	}

	// Generate add_recovered_peers.sh script
	peersJSONPath := filepath.Join(destDir, "peers.json")
	if peersData, err := os.ReadFile(peersJSONPath); err == nil {
		scriptPath := filepath.Join(destDir, "add_recovered_peers.sh")
		scriptContent := "#!/bin/bash\necho 'Adding recovered peers...'\n"
		
		var discoveryList []PeerDiscoveryInfo
		if err := json.Unmarshal(peersData, &discoveryList); err == nil {
			for _, p := range discoveryList {
				if len(p.Endpoints) > 0 {
					scriptContent += fmt.Sprintf("./client addpeer %s\n", p.Endpoints[0])
				}
			}
			os.WriteFile(scriptPath, []byte(scriptContent), 0755)
			if e.Verbose {
				log.Printf("RESCUE: Generated peer recovery script at %s", scriptPath)
			}
		}
	}

	if e.Verbose {
		log.Printf("RESCUE: SUCCESS! Recovered bundle to %s", destDir)
	}
	return nil
}
