package client

import (
	"context"
	"database/sql"
	"p2p-backup/internal/db"
)

// --- Crawler Queries ---

// getDirectoryState fetches the latest file modification times for a given directory.
func getDirectoryState(database *sql.DB, dirPath string) (map[string]int64, error) {
	query := `
		SELECT f.filename, v.mtime 
		FROM files f 
		JOIN file_versions v ON v.file_id = f.id 
		JOIN directories d ON d.id = f.dir_id
		WHERE d.full_path = ? AND f.deleted = 0 
		  AND v.id = (SELECT MAX(id) FROM file_versions WHERE file_id = f.id)
	`
	rows, err := database.Query(query, dirPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dbFiles := make(map[string]int64)
	for rows.Next() {
		var name string
		var mtime int64
		if err := rows.Scan(&name, &mtime); err == nil {
			dbFiles[name] = mtime
		}
	}
	return dbFiles, nil
}

// --- Uploader Queries ---

// updateBackupOfferedCount increments the offered files count for a backup.
func updateBackupOfferedCount(dbJobChan chan<- db.DBJob, backupID int64, count int) {
	dbJobChan <- db.DBJob{
		Query:      "UPDATE backups SET offered_files = offered_files + ? WHERE id = ?",
		Args:       []interface{}{count, backupID},
		ResultChan: make(chan db.DBResult, 1),
	}
}

// updateBackupUploadedCount increments the uploaded files count for a backup.
func updateBackupUploadedCount(dbJobChan chan<- db.DBJob, backupID int64, count int) {
	dbJobChan <- db.DBJob{
		Query:      "UPDATE backups SET uploaded_files = uploaded_files + ? WHERE id = ?",
		Args:       []interface{}{count, backupID},
		ResultChan: make(chan db.DBResult, 1),
	}
}

// --- State Manager Queries ---

// fetchDirID syncronously queries for a dir_id.
func fetchDirID(database *sql.DB, dirPath string) (int64, error) {
	var dirID int64
	err := database.QueryRow("SELECT id FROM directories WHERE full_path = ?", dirPath).Scan(&dirID)
	return dirID, err
}

// insertDirID syncronously inserts or updates a directory and returns its ID.
func insertDirID(dbJobChan chan<- db.DBJob, dirPath string) (int64, error) {
	resChan := make(chan db.DBResult)
	var dirID int64
	dbJobChan <- db.DBJob{
		Query: `
			INSERT INTO directories (full_path, first_seen, last_seen)
			VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT(full_path) DO UPDATE SET last_seen = CURRENT_TIMESTAMP
			RETURNING id;
		`,
		Args:       []interface{}{dirPath},
		ResultChan: resChan,
		Scan: func(row *sql.Row) error {
			return row.Scan(&dirID)
		},
	}
	res := <-resChan
	return dirID, res.Err
}

// markFileDeleted asynchronously marks a file as deleted.
func markFileDeleted(dbJobChan chan<- db.DBJob, dirID int64, fileName string) {
	dbJobChan <- db.DBJob{
		Query: `
			UPDATE files SET deleted = 1 
			WHERE dir_id = ? AND filename = ?
		`,
		Args:       []interface{}{dirID, fileName},
		ResultChan: make(chan db.DBResult, 1),
	}
}

// insertFile syncronously inserts or updates a file and returns its ID.
func insertFile(dbJobChan chan<- db.DBJob, dirID int64, fileName string) (int64, error) {
	resChan := make(chan db.DBResult)
	var fileID int64
	dbJobChan <- db.DBJob{
		Query: `
			INSERT INTO files (dir_id, filename, first_seen, last_seen, deleted)
			VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0)
			ON CONFLICT(dir_id, filename) DO UPDATE SET
				last_seen = CURRENT_TIMESTAMP,
				deleted = 0
			RETURNING id
		`,
		Args:       []interface{}{dirID, fileName},
		ResultChan: resChan,
		Scan: func(row *sql.Row) error {
			return row.Scan(&fileID)
		},
	}
	res := <-resChan
	return fileID, res.Err
}

// insertFileVersion syncronously inserts a file version and returns its ID.
func insertFileVersion(dbJobChan chan<- db.DBJob, fileID, backupID int64, mtime int64, size int64, plainHash string, uid, gid uint32, mode uint32) (int64, error) {
	resChan := make(chan db.DBResult)
	var versionID int64
	dbJobChan <- db.DBJob{
		Query: `
			INSERT INTO file_versions (file_id, backup_id, mtime, size, hash_plain, uid, gid, mode)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			RETURNING id
		`,
		Args:       []interface{}{fileID, backupID, mtime, size, plainHash, uid, gid, mode},
		ResultChan: resChan,
		Scan: func(row *sql.Row) error {
			return row.Scan(&versionID)
		},
	}
	res := <-resChan
	return versionID, res.Err
}

// insertLocalBlob asynchronously inserts a local blob mapping.
func insertLocalBlob(dbJobChan chan<- db.DBJob, plainHash, encHash string, size int) {
	dbJobChan <- db.DBJob{
		Query:      "INSERT OR IGNORE INTO local_blobs (hash_plain, hash_encrypted, size) VALUES (?, ?, ?)",
		Args:       []interface{}{plainHash, encHash, size},
		ResultChan: make(chan db.DBResult, 1),
	}
}

// insertFileBlob asynchronously links a chunk to a file version.
func insertFileBlob(dbJobChan chan<- db.DBJob, versionID int64, encHash string, size int, sequence int) {
	dbJobChan <- db.DBJob{
		Query:      "INSERT INTO file_blobs (version_id, hash_encrypted, size, sequence) VALUES (?, ?, ?, ?)",
		Args:       []interface{}{versionID, encHash, size, sequence},
		ResultChan: make(chan db.DBResult, 1),
	}
}

// markDirectoryBackedUp asynchronously records a directory backup for a job.
func markDirectoryBackedUp(dbJobChan chan<- db.DBJob, backupID, dirID int64) {
	dbJobChan <- db.DBJob{
		Query:      "INSERT INTO directory_backups (backup_id, dir_id, completed_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
		Args:       []interface{}{backupID, dirID},
		ResultChan: make(chan db.DBResult, 1),
	}
}

// --- Restorer Queries ---

// CheckRestorableFile checks if a specific file exists to be restored.
func CheckRestorableFile(db *sql.DB, ctx context.Context, dirPath, fileName string, targetBackupID int64) (int, error) {
	var count int
	var err error
	if targetBackupID > 0 {
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*) 
			FROM files f
			JOIN directories d ON f.dir_id = d.id
			WHERE d.full_path LIKE '%' || ? AND f.filename = ?
		`, dirPath, fileName).Scan(&count)
	} else {
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*) 
			FROM files f
			JOIN directories d ON f.dir_id = d.id
			WHERE d.full_path LIKE '%' || ? AND f.filename = ? AND f.deleted = 0
		`, dirPath, fileName).Scan(&count)
	}
	return count, err
}

// FindRestorablePaths finds all files under a directory prefix to restore.
func FindRestorablePaths(db *sql.DB, ctx context.Context, targetPath string, targetBackupID int64) ([]string, error) {
	var rows *sql.Rows
	var err error
	if targetBackupID > 0 {
		rows, err = db.QueryContext(ctx, `
			SELECT d.full_path, f.filename
			FROM files f
			JOIN directories d ON f.dir_id = d.id
			WHERE d.full_path LIKE '%' || ? || '%'
		`, targetPath)
	} else {
		rows, err = db.QueryContext(ctx, `
			SELECT d.full_path, f.filename
			FROM files f
			JOIN directories d ON f.dir_id = d.id
			WHERE d.full_path LIKE '%' || ? || '%' AND f.deleted = 0
		`, targetPath)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []string
	for rows.Next() {
		var dPath, fName string
		if err := rows.Scan(&dPath, &fName); err != nil {
			return nil, err
		}
		files = append(files, dPath+"/"+fName) // Simple join
	}
	return files, nil
}

// GetFileIDByPathAndName fetches the file ID.
func GetFileIDByPathAndName(db *sql.DB, ctx context.Context, dirPath, fileName string) (int64, error) {
	var fileID int64
	err := db.QueryRowContext(ctx, `
		SELECT f.id
		FROM files f
		JOIN directories d ON f.dir_id = d.id
		WHERE d.full_path LIKE '%' || ? AND f.filename = ?
		LIMIT 1
	`, dirPath, fileName).Scan(&fileID)
	return fileID, err
}

// FileVersionMeta holds file metadata from database.
type FileVersionMeta struct {
	VersionID    int64
	OriginalSize int64
	Mtime        int64
	UID          int
	GID          int
	Mode         uint32
}

// GetFileVersionMeta fetches the requested version.
func GetFileVersionMeta(db *sql.DB, ctx context.Context, fileID, targetBackupID int64) (FileVersionMeta, error) {
	var meta FileVersionMeta
	var err error
	if targetBackupID > 0 {
		err = db.QueryRowContext(ctx, `
			SELECT id, size, mtime, uid, gid, mode 
			FROM file_versions 
			WHERE file_id = ? AND backup_id <= ?
			ORDER BY id DESC 
			LIMIT 1
		`, fileID, targetBackupID).Scan(&meta.VersionID, &meta.OriginalSize, &meta.Mtime, &meta.UID, &meta.GID, &meta.Mode)
	} else {
		err = db.QueryRowContext(ctx, `
			SELECT id, size, mtime, uid, gid, mode 
			FROM file_versions 
			WHERE file_id = ? 
			ORDER BY id DESC 
			LIMIT 1
		`, fileID).Scan(&meta.VersionID, &meta.OriginalSize, &meta.Mtime, &meta.UID, &meta.GID, &meta.Mode)
	}
	return meta, err
}

// GetFileChunkHashes retrieves all chunk hashes for a file version.
func GetFileChunkHashes(db *sql.DB, ctx context.Context, versionID int64) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT hash_encrypted 
		FROM file_blobs 
		WHERE version_id = ? 
		ORDER BY sequence ASC
	`, versionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunkHashes []string
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		chunkHashes = append(chunkHashes, hash)
	}
	return chunkHashes, nil
}

// VersionHistoryRow holds data for a file version.
type VersionHistoryRow struct {
	VersionID int64
	BackupID  int64
	Date      string
	Size      int64
	Hash      string
}

// GetFileVersionHistory fetches all versions of a file.
func GetFileVersionHistory(db *sql.DB, ctx context.Context, fileID int64) ([]VersionHistoryRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT v.id, v.backup_id, b.start_time, v.size, v.hash_plain
		FROM file_versions v
		JOIN backups b ON v.backup_id = b.id
		WHERE v.file_id = ?
		ORDER BY v.id DESC
	`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []VersionHistoryRow
	for rows.Next() {
		var row VersionHistoryRow
		if err := rows.Scan(&row.VersionID, &row.BackupID, &row.Date, &row.Size, &row.Hash); err == nil {
			history = append(history, row)
		}
	}
	return history, nil
}
