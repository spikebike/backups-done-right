A p2p backup system that uses encryption, deduplication, and trading storage with peers to allow for redundant backups.

This project is not working, progress is being made, if you want to contribute let me know.

Current progress has the following pieces working:
* Scan dirs from config file
* consult database to see if files are new or changed
* for new or changed files checksum and encrypt them into blobs

Create a config file like:
{
	"client": {
		"dirList": ["/tmp/bdr/input","."],
		"dataBaseName": "fsmeta.sql",
		"excludeList": [],
		"threads": 1, 
		"queue_blobs": "/tmp/bdr/output"
	}
}

To run:
 $ go run main.go

This will:
* read flags
* read config file
* read or create a database to track files and directories
* walk the directories from the config file
* find new or modified files by walking the dirlist and comparing to database
* sha256 and encrypt each file, writing a blob to the queue_blobs dir

Next to implement:
* Uploading blobs to server with protobufs (example protobuf code in examples/client-server-tls-proto



