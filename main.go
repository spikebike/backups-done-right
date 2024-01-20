package main

import (
	"flag"
	"fmt"
	"github.com/spikebike/backups-done-right/bdrsql"
	"log"
)

var (
	newDB = flag.Bool("new-db", false, "true = creates a new database | false = use existing database")

	debug bool
)

func main() {
	fmt.Println("Hello, World!")
	dataBaseName := "fsmeta.sql"
	debug = true

	db, err := bdrsql.Init_db(dataBaseName, true, debug)
	if err != nil {
		log.Printf("could not open %s, error: %s", dataBaseName, err)
	} else {
		log.Printf("opened database %v\n", dataBaseName)
	}
	if db == nil {
		log.Printf("missing db")
	}
}
