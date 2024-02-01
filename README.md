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

Idea on perf, backing up 9GB of 256MB files on a Ryzen 7900 (non-x):
1 threads 9.00GB 446.11MB/sec in 20.66 seconds
2 threads 9.00GB 857.20MB/sec in 10.75 seconds
4 threads 9.00GB 1.62GB/sec in 5.56 seconds
8 threads 9.00GB 2.85GB/sec in 3.16 seconds
12 threads 9.00GB 4.42GB/sec in 2.04 seconds
24 threads 9.00GB 5.71GB/sec in 1.58 seconds

So in with 24 threads in 1.58 seconds:
* 9GB of plaintext was read, likely mostly from a warm cache
* Files were checksummed with sha256 and encrypted with AES256
* 36 files (9GB) of encrypted blobs were written
