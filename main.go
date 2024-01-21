package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"github.com/spikebike/backups-done-right/bdrsql"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	newDB = flag.Bool("new-db", false, "true = creates a new database | false = use existing database")

	debug bool
)

func backupDir(db *sql.DB, dirList []string, excludeList []string, dataBaseName string) error {
	var (
		fileC       int64
		backupFileC int64
		dirC        int64
		dFile       int64
		dDir        int64
	)

	fileC = 0
	dirC = 0
	backupFileC = 0
	dFile = 0
	dDir = 0
	start := time.Now().Unix()

	for i, dirname := range dirList {
		fmt.Printf("%d: %s\n", i+1, dirname)
		// get dirID of dirname, even if it needs inserted.
		dirID, err := bdrsql.GetSQLID(db, "dirs", "path", dirname)
		// get a map for filename -> modified time
		SQLmap := bdrsql.GetSQLFiles(db, dirID)
		if debug == true {
			fmt.Printf("scanning dir %s ", dirname)
		}
		d, err := os.Open(dirname)
		if err != nil {
			log.Printf("failed to open %s error : %s", dirname, err)
			os.Exit(1)
		}
		fi, err := d.Readdir(-1)
		if err != nil {
			log.Printf("directory %s failed with error %s", dirname, err)
		}
		Fmap := map[string]int64{}
		// Iterate over the entire directory
		dFile = 0
		dDir = 0
		for _, f := range fi {
			if !f.IsDir() {
				fileC++ //track files per backup
				dFile++ //trace files per directory
				// and it's been modified since last backup
				if f.ModTime().Unix() <= SQLmap[f.Name()] {
					// log.Printf("NO backup needed for %s \n",f.Name())
					Fmap[f.Name()] = f.ModTime().Unix()
				} else {
					// log.Printf("backup needed for %s \n",f.Name())
					backupFileC++
					bdrsql.InsertSQLFile(db, f, dirID)
				}
			} else { // is directory
				dirC++ //track directories per backup
				dDir++ //track subdirs per directory
				fullpath := filepath.Join(dirname, f.Name())

				if !checkPath(dirList, excludeList, fullpath) {
					dirList = append(dirList, fullpath)
				}
			}
		}
		// All files that we've seen, set last_seen
		t1 := time.Now().UnixNano()
		bdrsql.SetSQLSeen(db, Fmap, dirID)
		if debug == true {
			t2 := time.Now().UnixNano()
			fmt.Printf("files=%d dirs=%d duration=%dms\n", dFile, dDir, (t2-t1)/1000000)
		}
		i++
	}
	// if we have not seen the files since start it must have been deleted.
	bdrsql.SetSQLDeleted(db, start)

	log.Printf("scanned %d files and %d directories\n", fileC, dirC)
	log.Printf("%d files scheduled for backup\n", backupFileC)

	return nil
}

func checkPath(dirArray []string, excludeArray []string, dir string) bool {
	for _, j := range excludeArray {
		if strings.Contains(dir, j) {
			return true
		}
	}
	for _, i := range dirArray {
		if i == dir {
			return true
		}
	}
	return false
}

func main() {
	var dirList, excludeList []string
	var err error
	fmt.Println("Hello, World!")
	debug = true

	viper.SetConfigName("config.json")
	viper.SetConfigType("json")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("Config file not found...")
	} else {
		name := viper.GetString("name")
		fmt.Println("Reading values from the config file:", name)
	}

	err = viper.UnmarshalKey("client.dirList", &dirList)
	if err != nil {
		log.Fatalf("Unable to decode into struct %v", err)
	}
	dataBaseName := viper.GetString("client.dataBaseName")

	err = viper.UnmarshalKey("client.excludeList", &excludeList)
	if err != nil {
		log.Fatalf("Unable to decode into struct %v", err)
	}

	log.Printf("The database name is: %s\n", dataBaseName)

	db, err := bdrsql.Init_db(dataBaseName, true, debug)
	if err != nil {
		log.Printf("could not open %s, error: %s", dataBaseName, err)
	} else {
		log.Printf("opened database %v\n", dataBaseName)
	}
	if db == nil {
		log.Printf("missing db")
	}

	err = backupDir(db, dirList, excludeList, dataBaseName)
}
