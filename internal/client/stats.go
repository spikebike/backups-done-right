package client

import (
	"fmt"
	"sync/atomic"
	"time"
)

// BackupStats tracks performance metrics during a backup run.
type BackupStats struct {
	StartTime     time.Time
	FilesRead     atomic.Int64
	BytesRead     atomic.Int64
	FilesUploaded atomic.Int64
	BytesUploaded atomic.Int64
}

// NewBackupStats creates a new BackupStats instance with the start time set to now.
func NewBackupStats() *BackupStats {
	return &BackupStats{
		StartTime: time.Now(),
	}
}

// AddFileRead records a file being read by the crypto pool.
func (s *BackupStats) AddFileRead(bytes int64) {
	s.FilesRead.Add(1)
	s.BytesRead.Add(bytes)
}

// AddFileUploaded records a blob being uploaded.
func (s *BackupStats) AddFileUploaded(bytes int64) {
	s.FilesUploaded.Add(1)
	s.BytesUploaded.Add(bytes)
}

// PrintSummary prints a human-readable performance report.
func (s *BackupStats) PrintSummary() {
	elapsed := time.Since(s.StartTime)
	secs := elapsed.Seconds()
	if secs == 0 {
		secs = 0.001 // avoid division by zero
	}

	filesRead := s.FilesRead.Load()
	bytesRead := s.BytesRead.Load()
	filesUploaded := s.FilesUploaded.Load()
	bytesUploaded := s.BytesUploaded.Load()

	fmt.Println("\n=== Backup Performance Summary ===")
	fmt.Printf("Runtime:           %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Files read:        %d (%.1f files/sec)\n", filesRead, float64(filesRead)/secs)
	fmt.Printf("Bytes read:        %s (%.1f MB/sec)\n", formatBytes(bytesRead), float64(bytesRead)/(secs*1024*1024))
	fmt.Printf("Files uploaded:    %d (%.1f files/sec)\n", filesUploaded, float64(filesUploaded)/secs)
	fmt.Printf("Bytes uploaded:    %s (%.1f MB/sec)\n", formatBytes(bytesUploaded), float64(bytesUploaded)/(secs*1024*1024))
	fmt.Println("==================================")
}

func formatBytes(b int64) string {
	switch {
	case b >= 1024*1024*1024:
		return fmt.Sprintf("%.2f GB", float64(b)/(1024*1024*1024))
	case b >= 1024*1024:
		return fmt.Sprintf("%.2f MB", float64(b)/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.2f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d B", b)
	}
}
