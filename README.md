A p2p backup system that uses encryption, deduplication, and trading storage with peers to allow for redundant backups.

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


