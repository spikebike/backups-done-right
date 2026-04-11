package client

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"p2p-backup/internal/crypto"
)

// Restorer handles downloading and decrypting files from the backup server.
type Restorer struct {
	DB        *sql.DB
	RPCClient RPCClient
	Key       []byte
	OutputDir string
	Verbose   bool
}

// NewRestorer creates a new Restorer.
func NewRestorer(db *sql.DB, rpcClient RPCClient, key []byte, outputDir string, verbose bool) *Restorer {
	return &Restorer{
		DB:        db,
		RPCClient: rpcClient,
		Key:       key,
		OutputDir: outputDir,
		Verbose:   verbose,
	}
}

// Restore intelligently determines if the target is a file or a directory pattern
// and restores all matching files.
func (r *Restorer) Restore(ctx context.Context, targetPath string, targetBackupID int64) error {
	dirPath := filepath.Dir(targetPath)
	fileName := filepath.Base(targetPath)

	var count int
	var err error

	count, err = CheckRestorableFile(r.DB, ctx, dirPath, fileName, targetBackupID)

	if err == nil && count > 0 {
		return r.RestoreFile(ctx, targetPath, targetBackupID, targetPath)
	}

	// Try as a directory prefix
	filesToRestore, err := FindRestorablePaths(r.DB, ctx, targetPath, targetBackupID)

	if len(filesToRestore) == 0 {
		return fmt.Errorf("no files found matching %s", targetPath)
	}

	for _, f := range filesToRestore {
		if err := r.RestoreFile(ctx, f, targetBackupID, targetPath); err != nil {
			log.Printf("Failed to restore %s: %v", f, err)
		}
	}

	return nil
}

// RestoreFile queries the local database for a file's chunks, downloads them,
// decrypts them, and reassembles the file into the output directory.
func (r *Restorer) RestoreFile(ctx context.Context, relativePath string, targetBackupID int64, baseTargetPath string) error {
	// 1. Look up the file and its directory in the database
	dirPath := filepath.Dir(relativePath)
	fileName := filepath.Base(relativePath)

	fileID, err := GetFileIDByPathAndName(r.DB, ctx, dirPath, fileName)

	if err == sql.ErrNoRows {
		return fmt.Errorf("file not found in backup database: %s", relativePath)
	} else if err != nil {
		return fmt.Errorf("database error looking up file: %w", err)
	}

	// Fetch the specific or latest version of this file
	meta, err := GetFileVersionMeta(r.DB, ctx, fileID, targetBackupID)
	versionID := meta.VersionID
	originalSize := meta.OriginalSize
	fileMtime := meta.Mtime
	fileUID := meta.UID
	fileGID := meta.GID
	fileMode := meta.Mode

	if err == sql.ErrNoRows {
		return fmt.Errorf("no versions found for file %s (up to backup %d)", relativePath, targetBackupID)
	} else if err != nil {
		return fmt.Errorf("database error looking up file version: %w", err)
	}

	// 2. Get the list of encrypted chunk hashes for this file, in order
	chunkHashes, err := GetFileChunkHashes(r.DB, ctx, versionID)
	if err != nil {
		return fmt.Errorf("database error looking up file chunks: %w", err)
	}

	// 3. Prepare the output file
	safePath := relativePath
	if baseTargetPath != "" && strings.HasPrefix(relativePath, baseTargetPath) {
		rel, err := filepath.Rel(baseTargetPath, relativePath)
		if err == nil && rel != "" && rel != "." {
			safePath = rel
		} else if rel == "." {
			safePath = filepath.Base(relativePath)
		}
	} else {
		if filepath.IsAbs(safePath) {
			safePath = strings.TrimPrefix(safePath, filepath.VolumeName(safePath))
			safePath = strings.TrimPrefix(safePath, "/")
			safePath = strings.TrimPrefix(safePath, "\\")
		}
	}

	outPath := filepath.Join(r.OutputDir, safePath)

	if len(chunkHashes) == 0 && originalSize == 0 {
		// Empty file
		return r.writeEmptyFile(outPath)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	outFile, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// 4. Download and Decrypt chunks in batches of 10
	var bytesRestored int64
	batchSize := 10

	zstdDecoder, err := zstd.NewReader(nil)
	if err != nil {
		return fmt.Errorf("failed to initialize zstd decoder: %w", err)
	}
	defer zstdDecoder.Close()

	for i := 0; i < len(chunkHashes); i += batchSize {
		end := i + batchSize
		if end > len(chunkHashes) {
			end = len(chunkHashes)
		}
		batchHashes := chunkHashes[i:end]

		foundBlobs, missingHashes, err := r.RPCClient.DownloadItems(ctx, batchHashes)
		if err != nil {
			return fmt.Errorf("failed to download items from server: %w", err)
		}

		if len(missingHashes) > 0 {
			return fmt.Errorf("server is missing %d required chunks for this file (e.g., %s)", len(missingHashes), missingHashes[0])
		}

		chunkDataMap := make(map[string][]byte)
		for _, b := range foundBlobs {
			chunkDataMap[b.Meta.Hash] = b.Data
		}

		for j, hash := range batchHashes {
			ciphertext, ok := chunkDataMap[hash]
			if !ok {
				return fmt.Errorf("critical error: server said it found blob %s but didn't return data", hash)
			}

			compressed, err := crypto.Decrypt(r.Key, ciphertext)
			if err != nil {
				return fmt.Errorf("failed to decrypt chunk %d (%s): %w", i+j, hash, err)
			}

			plaintext, err := zstdDecoder.DecodeAll(compressed, nil)
			if err != nil {
				return fmt.Errorf("failed to decompress chunk %d (%s): %w", i+j, hash, err)
			}

			n, err := outFile.Write(plaintext)
			if err != nil {
				return fmt.Errorf("failed to write decrypted chunk to disk: %w", err)
			}
			bytesRestored += int64(n)
		}
	}

	if r.Verbose {
		log.Printf("Successfully restored %s (%d bytes)", relativePath, bytesRestored)
	}

	// Apply original file metadata
	r.applyMetadata(outPath, fileMtime, fileUID, fileGID, fileMode)

	return nil
}

// PrintHistory prints all tracked versions of a specified file.
func (r *Restorer) PrintHistory(ctx context.Context, relativePath string) error {
	dirPath := filepath.Dir(relativePath)
	fileName := filepath.Base(relativePath)

	fileID, err := GetFileIDByPathAndName(r.DB, ctx, dirPath, fileName)

	if err == sql.ErrNoRows {
		return fmt.Errorf("file not found in backup database: %s", relativePath)
	} else if err != nil {
		return fmt.Errorf("database error looking up file: %w", err)
	}

	history, err := GetFileVersionHistory(r.DB, ctx, fileID)
	if err != nil {
		return fmt.Errorf("database error querying history: %w", err)
	}

	fmt.Printf("\n=== Version History for %s ===\n", fileName)
	fmt.Printf("%-10s | %-10s | %-20s | %-12s | %-32s\n", "Version ID", "Backup ID", "Date", "Size (bytes)", "Checksum (Truncated)")
	fmt.Println(strings.Repeat("-", 90))

	var count int
	for _, row := range history {

		displayHash := row.Hash
		if len(row.Hash) > 32 {
			displayHash = row.Hash[:32] + "..."
		} else if row.Hash == "" {
			displayHash = "(empty file)"
		}

		fmt.Printf("%-10d | %-10d | %-20s | %-12d | %-32s\n", row.VersionID, row.BackupID, row.Date, row.Size, displayHash)
		count++
	}

	if count == 0 {
		fmt.Println("No versions found.")
	}
	fmt.Println("========================================")
	return nil
}

func (r *Restorer) writeEmptyFile(outPath string) error {
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		return err
	}
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	f.Close()
	return nil
}

// applyMetadata restores the original file permissions, ownership, and modification time.
func (r *Restorer) applyMetadata(path string, mtime int64, uid, gid int, mode uint32) {
	if mode != 0 {
		if err := os.Chmod(path, os.FileMode(mode)); err != nil {
			log.Printf("Warning: failed to set permissions on %s: %v", path, err)
		}
	}

	if uid != 0 || gid != 0 {
		if err := os.Chown(path, uid, gid); err != nil {
			log.Printf("Warning: failed to set ownership on %s: %v", path, err)
		}
	}

	if mtime != 0 {
		t := time.Unix(mtime, 0)
		if err := os.Chtimes(path, t, t); err != nil {
			log.Printf("Warning: failed to set mtime on %s: %v", path, err)
		}
	}
}
