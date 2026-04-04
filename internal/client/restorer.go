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

	"p2p-backup/internal/crypto"

	"github.com/klauspost/compress/zstd"
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

	if targetBackupID > 0 {
		err = r.DB.QueryRowContext(ctx, `
			SELECT COUNT(*) 
			FROM files f
			JOIN directories d ON f.dir_id = d.id
			WHERE d.full_path LIKE '%' || ? AND f.filename = ?
		`, dirPath, fileName).Scan(&count)
	} else {
		err = r.DB.QueryRowContext(ctx, `
			SELECT COUNT(*) 
			FROM files f
			JOIN directories d ON f.dir_id = d.id
			WHERE d.full_path LIKE '%' || ? AND f.filename = ? AND f.deleted = 0
		`, dirPath, fileName).Scan(&count)
	}

	if err == nil && count > 0 {
		return r.RestoreFile(ctx, targetPath, targetBackupID, targetPath)
	}

	// Try as a directory prefix
	var rows *sql.Rows
	if targetBackupID > 0 {
		rows, err = r.DB.QueryContext(ctx, `
			SELECT d.full_path, f.filename
			FROM files f
			JOIN directories d ON f.dir_id = d.id
			WHERE d.full_path LIKE '%' || ? || '%'
		`, targetPath)
	} else {
		rows, err = r.DB.QueryContext(ctx, `
			SELECT d.full_path, f.filename
			FROM files f
			JOIN directories d ON f.dir_id = d.id
			WHERE d.full_path LIKE '%' || ? || '%' AND f.deleted = 0
		`, targetPath)
	}

	if err != nil {
		return fmt.Errorf("database error searching for path %s: %w", targetPath, err)
	}
	defer rows.Close()

	var filesToRestore []string
	for rows.Next() {
		var dPath, fName string
		if err := rows.Scan(&dPath, &fName); err != nil {
			return err
		}
		filesToRestore = append(filesToRestore, filepath.Join(dPath, fName))
	}

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

	var fileID int64
	err := r.DB.QueryRowContext(ctx, `
		SELECT f.id
		FROM files f
		JOIN directories d ON f.dir_id = d.id
		WHERE d.full_path LIKE '%' || ? AND f.filename = ?
		LIMIT 1
	`, dirPath, fileName).Scan(&fileID)

	if err == sql.ErrNoRows {
		return fmt.Errorf("file not found in backup database: %s", relativePath)
	} else if err != nil {
		return fmt.Errorf("database error looking up file: %w", err)
	}

	// Fetch the specific or latest version of this file
	var versionID int64
	var originalSize int64
	var fileMtime int64
	var fileUID, fileGID int
	var fileMode uint32

	if targetBackupID > 0 {
		err = r.DB.QueryRowContext(ctx, `
			SELECT id, size, mtime, uid, gid, mode 
			FROM file_versions 
			WHERE file_id = ? AND backup_id <= ?
			ORDER BY id DESC 
			LIMIT 1
		`, fileID, targetBackupID).Scan(&versionID, &originalSize, &fileMtime, &fileUID, &fileGID, &fileMode)
	} else {
		err = r.DB.QueryRowContext(ctx, `
			SELECT id, size, mtime, uid, gid, mode 
			FROM file_versions 
			WHERE file_id = ? 
			ORDER BY id DESC 
			LIMIT 1
		`, fileID).Scan(&versionID, &originalSize, &fileMtime, &fileUID, &fileGID, &fileMode)
	}

	if err == sql.ErrNoRows {
		return fmt.Errorf("no versions found for file %s (up to backup %d)", relativePath, targetBackupID)
	} else if err != nil {
		return fmt.Errorf("database error looking up file version: %w", err)
	}

	// 2. Get the list of encrypted chunk hashes for this file, in order
	rows, err := r.DB.QueryContext(ctx, `
		SELECT hash_encrypted 
		FROM file_blobs 
		WHERE version_id = ? 
		ORDER BY sequence ASC
	`, versionID)
	if err != nil {
		return fmt.Errorf("database error looking up file chunks: %w", err)
	}
	defer rows.Close()

	var chunkHashes []string
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return err
		}
		chunkHashes = append(chunkHashes, hash)
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

		foundBlobs, err := r.RPCClient.PullBlobBatch(ctx, batchHashes)
		if err != nil {
			return fmt.Errorf("failed to pull blob batch: %w", err)
		}

		if len(foundBlobs) < len(batchHashes) {
			// Find missing
			foundMap := make(map[string]bool)
			for _, b := range foundBlobs {
				foundMap[b.Hash] = true
			}
			for _, h := range batchHashes {
				if !foundMap[h] {
					return fmt.Errorf("server is missing required chunk: %s", h)
				}
			}
		}

		chunkDataMap := make(map[string][]byte)
		for _, b := range foundBlobs {
			chunkDataMap[b.Hash] = b.Data
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

	var fileID int64
	err := r.DB.QueryRowContext(ctx, `
		SELECT f.id
		FROM files f
		JOIN directories d ON f.dir_id = d.id
		WHERE d.full_path LIKE '%' || ? AND f.filename = ?
		LIMIT 1
	`, dirPath, fileName).Scan(&fileID)

	if err == sql.ErrNoRows {
		return fmt.Errorf("file not found in backup database: %s", relativePath)
	} else if err != nil {
		return fmt.Errorf("database error looking up file: %w", err)
	}

	rows, err := r.DB.QueryContext(ctx, `
		SELECT v.id, v.backup_id, b.start_time, v.size, v.hash_plain
		FROM file_versions v
		JOIN backups b ON v.backup_id = b.id
		WHERE v.file_id = ?
		ORDER BY v.id DESC
	`, fileID)
	if err != nil {
		return fmt.Errorf("database error querying history: %w", err)
	}
	defer rows.Close()

	fmt.Printf("\n=== Version History for %s ===\n", fileName)
	fmt.Printf("%-10s | %-10s | %-20s | %-12s | %-32s\n", "Version ID", "Backup ID", "Date", "Size (bytes)", "Checksum (Truncated)")
	fmt.Println(strings.Repeat("-", 90))

	var count int
	for rows.Next() {
		var vID, bID, size int64
		var date, hash string
		if err := rows.Scan(&vID, &bID, &date, &size, &hash); err != nil {
			continue
		}

		displayHash := hash
		if len(hash) > 32 {
			displayHash = hash[:32] + "..."
		} else if hash == "" {
			displayHash = "(empty file)"
		}

		fmt.Printf("%-10d | %-10d | %-20s | %-12d | %-32s\n", vID, bID, date, size, displayHash)
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
