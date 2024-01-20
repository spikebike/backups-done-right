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

	db, err := bdrsql.Init_db(dataBaseName, *newDB, true)
	if err != nil {
		log.Printf("could not open %s, error: %s", dataBaseName, err)
	} else {
		log.Printf("opened database %v\n", dataBaseName)
	}
	err = bdrsql.CreateClientTables(db, true)
	if err != nil && debug {
		log.Printf("couldn't create tables: %s", err)
	} else {
		log.Printf("created tables\n")
	}

	fmt.Printf("%+v\n", db)
}
