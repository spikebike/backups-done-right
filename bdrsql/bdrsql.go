package bdrsql

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
)
import _ "github.com/mattn/go-sqlite3"

var (
	clientSQLs = []string{
		"create table dirs (id INTEGER PRIMARY KEY, mode INT, ino BIGINT, uid INT, gid INT, path varchar(2048), last_seen BIGINT, deleted INT)",
		"create table files (id INTEGER PRIMARY KEY, mode INT, ino BIGINT, dev BIGINT, uid INT, gid INT, size BIGINT, mtime BIGINT, ctime BIGINT, name varchar(254), dirID BIGINT, last_seen BIGINT, deleted INT, do_upload INT, FOREIGN KEY(dirID) REFERENCES dirs(id))",
		"create index ctimeindex on files (ctime)",
		"create index pathindex on dirs (path)",
	}
)

func Init_db(dataBaseName string, newDB bool, dbg bool) (db *sql.DB, err error) {
	if newDB == true {
		// rm dataBaseName*  (all backups)
		fps, _ := filepath.Glob(dataBaseName + "*")
		for _, fp := range fps {
			err := os.Remove(fp)
			if err != nil {
				return nil, err
			}
		}
	}

	db, err = sql.Open("sqlite3", dataBaseName)
	if err != nil {
		log.Printf("couldn't open database: %s", err)
		os.Exit(1)
	}
	// Allow commits to be buffered, MUCH faster.
	// debug = true makes database writes synchronous and much slower,
	if dbg == false {
		_, err = db.Exec("PRAGMA synchronous=OFF")
		if err != nil {
			log.Printf("%s", err)
		}
	}
	return db, err
}

func CreateClientTables(db *sql.DB, debug bool) error {
	var err error
	for _, sqli := range clientSQLs {
		_, err = db.Exec(sqli)
		if err != nil && debug == true {
			fmt.Printf("%s", err)
		}
	}
	return err
}
