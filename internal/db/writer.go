package db

import (
	"database/sql"
	"log"
	"time"
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
		stmtCache := make(map[string]*sql.Stmt)

		getStmt := func(query string, tx *sql.Tx) (*sql.Stmt, error) {
			stmt, ok := stmtCache[query]
			if !ok {
				var err error
				stmt, err = db.Prepare(query)
				if err != nil {
					return nil, err
				}
				stmtCache[query] = stmt
			}
			if tx != nil {
				return tx.Stmt(stmt), nil
			}
			return stmt, nil
		}

		// Flush settings
		const flushSize = 100
		const flushDura = 50 * time.Millisecond

		var batchJobs []DBJob
		var tx *sql.Tx
		var results []DBResult

		flushBatch := func() {
			if tx == nil {
				return
			}
			if err := tx.Commit(); err != nil {
				log.Printf("DBWriter commit error: %v", err)
				for _, j := range batchJobs {
					if j.ResultChan != nil {
						j.ResultChan <- DBResult{Err: err}
					}
				}
			} else {
				for i, j := range batchJobs {
					if j.ResultChan != nil {
						j.ResultChan <- results[i]
					}
				}
			}
			batchJobs = batchJobs[:0]
			results = results[:0]
			tx = nil
		}

		ticker := time.NewTicker(flushDura)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if len(batchJobs) > 0 {
					flushBatch()
				}
			case job, ok := <-jobChan:
				if !ok {
					flushBatch() // flush remaining if closed
					for _, stmt := range stmtCache {
						stmt.Close()
					}
					return
				}

				// If it's a read (or an INSERT..RETURNING with a Scan), flush the batch first
				// so it can see the latest state, then execute it aggressively outside the batch.
				if job.Scan != nil {
					flushBatch()
					stmt, err := getStmt(job.Query, nil)
					if err != nil {
						if job.ResultChan != nil {
							job.ResultChan <- DBResult{Err: err}
						}
					} else {
						row := stmt.QueryRow(job.Args...)
						err = job.Scan(row)
						if job.ResultChan != nil {
							job.ResultChan <- DBResult{Err: err}
						}
					}
					continue
				}

				// Otherwise it's a write that can be batched
				if tx == nil {
					var err error
					tx, err = db.Begin()
					if err != nil {
						log.Printf("DBWriter begin tx error: %v", err)
						if job.ResultChan != nil {
							job.ResultChan <- DBResult{Err: err}
						}
						continue
					}
				}

				stmt, err := getStmt(job.Query, tx)
				if err != nil {
					log.Printf("DBWriter syntax error: %v on query %s", err, job.Query)
					results = append(results, DBResult{Err: err})
				} else {
					res, err := stmt.Exec(job.Args...)
					if err != nil {
						// Don't pollute logs with UNIQUE constraint violations from crawler scans
						results = append(results, DBResult{Err: err})
					} else {
						id, _ := res.LastInsertId()
						results = append(results, DBResult{ID: id})
					}
				}
				batchJobs = append(batchJobs, job)

				if len(batchJobs) >= flushSize {
					flushBatch()
				}
			}
		}
	}()
}
