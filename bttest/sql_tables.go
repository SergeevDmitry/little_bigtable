package bttest

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log"

	"github.com/mattn/go-sqlite3"
)

// SqlTables persists tables to tables_t
type SqlTables struct {
	db *sql.DB
}

func NewSqlTables(db *sql.DB) *SqlTables {
	return &SqlTables{
		db: db,
	}
}

func (t *table) Scan(src interface{}) error {
	switch src := src.(type) {
	case nil:
		return nil
	case []byte:
	default:
		return fmt.Errorf("unknown type %T", src)
	}

	b := bytes.NewBuffer(src.([]byte))
	err := gob.NewDecoder(b).Decode(&t.families)

	t.counter = uint64(len(t.families))
	for _, f := range t.families {
		if f.Order > t.counter {
			t.counter = f.Order
		}
	}
	return err
}

func (t *table) Bytes() ([]byte, error) {
	if t == nil {
		return nil, nil
	}
	b := new(bytes.Buffer)
	err := gob.NewEncoder(b).Encode(t.families)
	return b.Bytes(), err
}

func (db *SqlTables) Get(tableId string) *table {
	tbl := &table{
		tableId: tableId,
		rows:    NewSqlRows(db.db, tableId),
	}
	err := db.db.QueryRow("SELECT metadata FROM tables_t WHERE table_id = ?", tableId).Scan(tbl)
	if err == sql.ErrNoRows {
		return nil
	}
	return tbl
}

func (db *SqlTables) GetAll() []*table {
	var tables []*table

	rows, err := db.db.Query("SELECT table_id, metadata FROM tables_t")
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		t := table{}
		if err := rows.Scan(&t.tableId, &t); err != nil {
			log.Fatal(err)
		}
		t.rows = NewSqlRows(db.db, t.tableId)
		tables = append(tables, &t)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	return tables
}

func (db *SqlTables) Save(t *table) {
	metadata, err := t.Bytes()
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.db.Exec("INSERT INTO tables_t (table_id, metadata) VALUES (?, ?)", t.tableId, metadata)
	if e, ok := err.(sqlite3.Error); ok && e.Code == 19 {
		_, err = db.db.Exec("UPDATE tables_t SET metadata = ? WHERE table_id = ?", metadata, t.tableId)
	}
	if err != nil {
		log.Fatalf("%#v", err)
	}

	query := "CREATE TABLE IF NOT EXISTS " + t.tableId + " ( \n" +
		"`row_key` TEXT NOT NULL,\n" +
		"`families` BLOB NOT NULL,\n" +
		"PRIMARY KEY (`row_key`)\n" +
		") WITHOUT ROWID"
	// this table could be WITHOUT ROWID but that is only supported in sqllite 3.8.2+
	// https://www.sqlite.org/releaselog/3_8_2.html
	log.Print(query)
	_, err = db.db.Exec(query)
	if err != nil {
		log.Fatalf("%#v", err)
	}
}

func (db *SqlTables) Delete(t *table) {
	_, err := db.db.Exec("DELETE FROM tables_t WHERE table_id = ? ", t.tableId)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.db.Exec("DROP TABLE ?", t.tableId)
	if err != nil {
		log.Fatal(err)
	}
}
