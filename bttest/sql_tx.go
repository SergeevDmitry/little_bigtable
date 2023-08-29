package bttest

import (
	"database/sql"
	"log"
	"sync"
)

type sqlTx struct {
	tx           *sql.Tx
	stmtCache    map[string]*sql.Stmt
	stmtCacheMux *sync.Mutex
}

func NewTx(db *sql.DB) *sqlTx {
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("error starting tx: %v", err)
	}

	sTx := &sqlTx{
		tx:           tx,
		stmtCache:    make(map[string]*sql.Stmt),
		stmtCacheMux: &sync.Mutex{},
	}

	return sTx
}

func (tx *sqlTx) getStatement(query string) *sql.Stmt {
	tx.stmtCacheMux.Lock()
	defer tx.stmtCacheMux.Unlock()
	if tx.stmtCache[query] == nil {
		newStatement, err := tx.tx.Prepare(query)
		if err != nil {
			log.Fatalf("error preparing stmt %s: %v", query, err)
		}
		tx.stmtCache[query] = newStatement
	}
	return tx.stmtCache[query]
}

func (tx *sqlTx) Exec(query string, args ...any) (sql.Result, error) {
	stmt := tx.getStatement(query)
	return stmt.Exec(args...)
}

func (tx *sqlTx) Query(query string, args ...any) (*sql.Rows, error) {
	stmt := tx.getStatement(query)
	return stmt.Query(args...)
}

func (tx *sqlTx) QueryRow(query string, args ...any) *sql.Row {
	stmt := tx.getStatement(query)
	return stmt.QueryRow(args...)
}

func (tx *sqlTx) Commit() error {
	return tx.tx.Commit()
}
