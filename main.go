package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"github.com/spikebike/backups-done-right/bdrsql"
	"github.com/spikebike/backups-done-right/bdrupload"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	pool_flag = flag.Int("threads", 0, "overwrites threads in [Client] section in config.cfg")

	upchan = make(chan *bdrupload.Upchan_t, 100)
	done   = make(chan int64)

	debug bool
	pool  int
)

type ByteSize float64

const (
	_           = iota // ignore first value by assigning to blank identifier
	KB ByteSize = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	EB
	ZB
	YB
)

func (b ByteSize) String() string {
	switch {
	case b >= YB:
		return fmt.Sprintf("%.2fYB", b/YB)
	case b >= ZB:
		return fmt.Sprintf("%.2fZB", b/ZB)
	case b >= EB:
		return fmt.Sprintf("%.2fEB", b/EB)
	case b >= PB:
		return fmt.Sprintf("%.2fPB", b/PB)
	case b >= TB:
		return fmt.Sprintf("%.2fTB", b/TB)
	case b >= GB:
		return fmt.Sprintf("%.2fGB", b/GB)
	case b >= MB:
		return fmt.Sprintf("%.2fMB", b/MB)
	case b >= KB:
		return fmt.Sprintf("%.2fKB", b/KB)
	}
	return fmt.Sprintf("%.2fB", b)
}

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
func MakeOrVerifyDir(filename string) bool {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) { // dir doesn't exit
		err := os.Mkdir(filename, 0700) // create dir
		if err != nil {                 // any error
			log.Printf("Err=%s blobdir=%s", err, filename)
			log.Fatal(err)
		}
	}
	return true
}

func main() {
	var dirList, excludeList []string
	var err error
	var bytes int64
	var bytesDone int64

	resetDB := flag.Bool("resetdb", false, "true = creates a new database | false = use existing database")

	flag.Parse()

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

	// Define pool, flag take priority over config file
	pool_config := viper.GetInt("client.threads")

	if *pool_flag != 0 { // flag gets priority over config file
		pool = *pool_flag
	} else {
		pool = int(pool_config)
	}

	err = viper.UnmarshalKey("client.dirList", &dirList)
	if err != nil {
		log.Fatalf("Unable to decode into struct %v", err)
	} 
	dataBaseName := viper.GetString("client.dataBaseName")

	queueBlobDir := viper.GetString("client.queue_blobs")

	MakeOrVerifyDir(queueBlobDir)
	MakeOrVerifyDir(queueBlobDir + "/tmp")
	MakeOrVerifyDir(queueBlobDir + "/blob")

	err = viper.UnmarshalKey("client.excludeList", &excludeList)
	if err != nil {
		log.Fatalf("Unable to decode into struct %v", err)
	}

	log.Printf("The database name is: %s\n", dataBaseName)
   *resetDB=true
	db, err := bdrsql.Init_db(dataBaseName, *resetDB, debug)
	if err != nil {
		log.Printf("could not open %s, error: %s", dataBaseName, err)
	} else {
		log.Printf("opened database %v\n", dataBaseName)
	}
	if db == nil {
		log.Printf("missing db")
	}
	t0 := time.Now()
	err = backupDir(db, dirList, excludeList, dataBaseName)
	t1 := time.Now()
	duration := t1.Sub(t0)

	if err != nil {
		log.Printf("backupDir failed: %s", err)
		os.Exit(1)
	}

	log.Printf("walking took: %v\n", duration)

	tn0 := time.Now().UnixNano()
	for i := 0; i < pool; i++ {
		go bdrupload.Uploader(upchan, done, debug, queueBlobDir)
	}
	log.Printf("started %d uploaders\n", pool)
	bdrsql.SQLUpload(db, upchan)

	bytesDone = 0
	bytes = 0
	for i := 0; i < pool; i++ {
		bytes = <-done
		bytesDone += bytes
	}
	tn1 := time.Now().UnixNano()
	seconds := float64(tn1-tn0) / 1000000000
	log.Printf("%d threads %s %s/sec in %4.2f seconds\n", pool, ByteSize(float64(bytesDone)), ByteSize(float64(bytesDone)/seconds), seconds)
}
