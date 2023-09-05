package bttest

import (
	"context"
	"database/sql"
)

func CreateTables(ctx context.Context, db *sql.DB) error {
	query := "CREATE TABLE IF NOT EXISTS tables_t ( \n" +
		"`table_id` TEXT NOT NULL,\n" +
		"`metadata` BLOG NOT NULL,\n" +
		"PRIMARY KEY  (`table_id`)\n" +
		")"
	// log.Print(query)
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	return nil
}
