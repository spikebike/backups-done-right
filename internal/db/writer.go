package db

import (
	"database/sql"
	"log"
)

type DBJob struct {
	Query      string
	Args       []interface{}
	ResultChan chan DBResult
	Scan       func(*sql.Row) error // Optional: for QueryRow
}

type DBResult struct {
	ID  int64
	Err error
}

func StartDBWriter(db *sql.DB, jobChan <-chan DBJob) {
	go func() {
		for job := range jobChan {
			if job.Scan != nil {
				row := db.QueryRow(job.Query, job.Args...)
				err := job.Scan(row)
				job.ResultChan <- DBResult{Err: err}
			} else {
				res, err := db.Exec(job.Query, job.Args...)
				if err != nil {
					log.Printf("DBWriter error: %v", err)
					job.ResultChan <- DBResult{Err: err}
					continue
				}
				id, _ := res.LastInsertId()
				job.ResultChan <- DBResult{ID: id}
			}
		}
	}()
}
