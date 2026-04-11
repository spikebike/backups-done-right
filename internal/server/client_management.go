package server

import (
	"context"
	"database/sql"
	"fmt"
)

// AuthorizeClient registers a client connection and returns its status and quota.
func (e *Engine) AuthorizeClient(ctx context.Context, pubKeyHex string) (status string, quotaBytes int64, currentSize int64, err error) {
	// Auto-register if new. If it's the admin, trust it immediately.
	defaultStatus := "pending"
	defaultQuota := int64(0)
	if e.AdminPublicKey != "" && pubKeyHex == e.AdminPublicKey {
		defaultStatus = "trusted"
		defaultQuota = 100 * 1024 * 1024 * 1024 // 100GB default for admin
	}

	_, err = e.DB.ExecContext(ctx, "INSERT OR IGNORE INTO clients (public_key, status, max_storage_size) VALUES (?, ?, ?)", pubKeyHex, defaultStatus, defaultQuota)
	if err != nil {
		return "", 0, 0, fmt.Errorf("failed to register client: %w", err)
	}

	err = e.DB.QueryRowContext(ctx, "SELECT status, max_storage_size, current_storage_size FROM clients WHERE public_key = ?", pubKeyHex).Scan(&status, &quotaBytes, &currentSize)
	if err == sql.ErrNoRows {
		return "", 0, 0, nil
	} else if err != nil {
		return "", 0, 0, fmt.Errorf("failed to query client status: %w", err)
	}

	return status, quotaBytes, currentSize, nil
}

// UpdateClient changes a client's status and quota. Only the Admin can do this.
func (e *Engine) UpdateClient(ctx context.Context, callerPubKey string, id uint64, status string, maxStorageBytes uint64) error {
	if e.AdminPublicKey != "" && callerPubKey != e.AdminPublicKey {
		return fmt.Errorf("unauthorized: only admin can update clients")
	}

	if id == 0 {
		return fmt.Errorf("invalid client ID")
	}

	_, err := e.DB.ExecContext(ctx, "UPDATE clients SET status = ?, max_storage_size = ? WHERE id = ?", status, maxStorageBytes, id)
	return err
}

// AddClient manually adds a client by their public key.
func (e *Engine) AddClient(ctx context.Context, callerPubKey string, clientPubKey string, status string, maxStorageBytes uint64) error {
	if e.AdminPublicKey != "" && callerPubKey != e.AdminPublicKey {
		return fmt.Errorf("unauthorized: only admin can add clients")
	}

	_, err := e.DB.ExecContext(ctx, `
		INSERT INTO clients (public_key, status, max_storage_size)
		VALUES (?, ?, ?)
		ON CONFLICT(public_key) DO UPDATE SET 
			status=excluded.status,
			max_storage_size=excluded.max_storage_size
	`, clientPubKey, status, maxStorageBytes)
	return err
}

type ClientDBInfo struct {
	ID                 uint64
	PublicKey          string
	Status             string
	LastSeen           string
	MaxStorageSize     uint64
	CurrentStorageSize uint64
}

func (e *Engine) ListClients(ctx context.Context) ([]ClientDBInfo, error) {
	// Query clients that are NOT also in the peers table.
	// This separates management of the Swarm (peers) from management of Users (clients).
	query := `
		SELECT c.id, c.public_key, c.status, c.last_seen, c.max_storage_size, c.current_storage_size 
		FROM clients c
		LEFT JOIN peers p ON c.public_key = p.public_key
		WHERE p.id IS NULL
		ORDER BY c.last_seen DESC
	`
	rows, err := e.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var clients []ClientDBInfo
	for rows.Next() {
		var c ClientDBInfo
		if err := rows.Scan(&c.ID, &c.PublicKey, &c.Status, &c.LastSeen, &c.MaxStorageSize, &c.CurrentStorageSize); err != nil {
			return nil, err
		}
		clients = append(clients, c)
	}
	return clients, nil
}
