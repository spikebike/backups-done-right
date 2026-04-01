# Integration Testing

The `backups-done-right` project uses Go's standard `testing` package to perform robust, isolated integration tests.

## Architecture of the Test

The current integration test (`tests/integration_test.go`) is designed to verify the entire end-to-end backup pipeline (Client -> Server -> Shards) without the need to spin up actual network ports or manage libp2p connections. It achieves this by connecting the Client's pipeline directly to the Server's Engine in-memory.

### Key Components

1.  **Isolation via `t.TempDir()`**:
    Every test run executes in a completely isolated environment. The test dynamically generates temporary directories for:
    *   Client Source Directory (where files to be backed up are placed)
    *   Client Spool & Upload Directories (for the CryptoPool pipeline)
    *   Client Local Database (SQLite)
    *   Server Blob & Queue Directories
    *   Server State Database (SQLite)

    This ensures that tests never interfere with your actual local databases or configurations, and Go automatically cleans up these directories when the test finishes.

2.  **In-Memory RPC Mocking**:
    Instead of serializing data over QUIC/UDP, the test utilizes `client.MockRPCClient`. This interface allows the Client's Uploader to directly call the methods on the Server's `Engine` struct, perfectly simulating network requests while maintaining the safety and speed of a local function call.

3.  **The Pipeline Flow**:
    The `TestEndToEndBackup` test executes the full lifecycle:
    *   Creates a dummy file in the temporary source directory.
    *   Initializes the Client DB, Crawler, CryptoPool, and Uploader.
    *   Initializes the Server DB and Engine.
    *   Starts the pipeline and waits for completion.
    *   Asserts that the server successfully ingested and sharded the expected data by querying the server's SQLite database.
    *   **Restores** the file using the `client.Restorer` component.
    *   **Verifies** the integrity of the restored file by comparing its content byte-by-byte with the original "source" file.

5.  **Deduplication Verification (`TestDeduplication`)**:
    *   Creates two files with identical content but different names.
    *   Runs the backup pipeline.
    *   Asserts that the server database only contains **one** blob, proving that both Convergent Encryption and server-side deduplication are working.
    *   Restores both files and verifies they both contain the correct data from the shared blob.

6.  **Incremental Backup Verification (`TestIncrementalBackup`)**:
    *   Runs an initial backup of a file.
    *   Runs a second backup with no changes and verifies that the crawler skips the file (no new `file_versions` record).
    *   Modifies the file and runs a third backup, verifying that a new version is created.
    *   Restores both the original and the modified version to ensure point-in-time recovery works.

7.  **File Deletion Verification (`TestFileDeletion`)**:
    *   Backs up a file.
    *   Deletes the file from the source directory.
    *   Runs a second backup and verifies that the file is marked as `deleted = 1` in the local manifest.
    *   Verifies that a "latest" restore attempt fails to find the file.
    *   Verifies that the file can still be successfully recovered by specifying the historical backup ID.

8.  **Server-Side Garbage Collection Verification (`TestServerGC`)**:
    *   Sets a tiny `ShardSize` to force shard sealing.
    *   Backs up a file to create a sealed shard.
    *   Simulates blob deletion by calling `DeleteBlobs` on the server.
    *   Manually ages the `deleted_at` timestamp in the server database.
    *   Manually triggers the Garbage Collection loop via `engine.TriggerGC()`.
    *   Verifies that the wasted shard and its associated data are completely removed from both the database and the filesystem.

## Running the Tests

To run the integration tests, use the standard Go test command from the root of the project:

```bash
go test -v ./tests/...
```

## Adding New Tests

Because the foundation uses isolated directories and bypasses network constraints, you can easily copy the existing test pattern to create new tests for:
*   **Deduplication**: Run the pipeline twice on the same file and assert the server database only stores one copy.
*   **Garbage Collection**: Simulate deleting a file or expiring a backup and calling the `GCWorker` to verify cleanup.
*   **Erasure Coding**: Tweak the shard size thresholds and assert that the `RepairWorker` or background threads correctly split the shards.
