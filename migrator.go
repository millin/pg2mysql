package pg2mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

type Migrator interface {
	Migrate() error
}

func NewMigrator(src, dst DB, truncateFirst bool, watcher MigratorWatcher) Migrator {
	return &migrator{
		src:           src,
		dst:           dst,
		truncateFirst: truncateFirst,
		watcher:       watcher,
	}
}

type migrator struct {
	src, dst      DB
	truncateFirst bool
	watcher       MigratorWatcher
}

func (m *migrator) Migrate() error {
	srcSchema, err := BuildSchema(m.src)
	if err != nil {
		return fmt.Errorf("failed to build source schema: %s", err)
	}

	m.watcher.WillDisableConstraints()
	err = m.dst.DisableConstraints()
	if err != nil {
		return fmt.Errorf("failed to disable constraints: %s", err)
	}
	m.watcher.DidDisableConstraints()

	defer func() {
		m.watcher.WillEnableConstraints()
		err = m.dst.EnableConstraints()
		if err != nil {
			m.watcher.EnableConstraintsDidFailWithError(err)
		} else {
			m.watcher.EnableConstraintsDidFinish()
		}
	}()

	for _, table := range srcSchema.Tables {
		if m.truncateFirst {
			m.watcher.WillTruncateTable(table.Name)
			_, err := m.dst.DB().Exec(fmt.Sprintf("TRUNCATE TABLE %s", table.Name))
			if err != nil {
				return fmt.Errorf("failed truncating: %s", err)
			}
			m.watcher.TruncateTableDidFinish(table.Name)
		}

		var recordsInserted int64

		m.watcher.TableMigrationDidStart(table.Name)

		// TODO: likely just only run this if truncating... idk.
		if table.HasColumn("id") {
			recordsInserted, err = migrateByCsvFile(m.watcher, m.src, m.dst, table)
			if err != nil {
				return fmt.Errorf("failed migrating table with ids: %s", err)
			} else {
				m.watcher.TableMigrationDidFinish(table.Name, recordsInserted)
				return nil
			}
		}

		if table.HasColumn("id") {
			recordsInserted, err = migrateWithIDs(m.watcher, m.src, m.dst, table)
			if err != nil {
				return fmt.Errorf("failed migrating table with ids: %s", err)
			}
		} else {
			preparedStmt, err := preparedBulkInsert(m.dst, table, 1)
			if err != nil {
				return fmt.Errorf("failed creating prepared bulk insert statement: %s", err)
			}

			err = EachMissingRow(m.src, m.dst, table, func(scanArgs []interface{}) {
				err = insert(preparedStmt, scanArgs)
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to insert into %s: %s\n", table.Name, err)
					return
				}
				recordsInserted++
			})
			if err != nil {
				return fmt.Errorf("failed migrating table without ids: %s", err)
			}
		}

		m.watcher.TableMigrationDidFinish(table.Name, recordsInserted)
	}

	return nil
}

func migrateByCsvFile(watcher MigratorWatcher, src DB, dst DB, table *Table) (int64, error) {
	columnNamesForSelect := make([]string, len(table.Columns))
	for i := range table.Columns {
		columnNamesForSelect[i] = table.Columns[i].Name
	}

	// find ids already in dst
	rows, err := dst.DB().Query(fmt.Sprintf("SELECT id FROM %s", table.Name))
	if err != nil {
		return 0, fmt.Errorf("failed to select id from rows: %s", err)
	}

	var dstIDs []interface{}
	for rows.Next() {
		var id interface{}
		if err = rows.Scan(&id); err != nil {
			return 0, fmt.Errorf("failed to scan id from row: %s", err)
		}
		dstIDs = append(dstIDs, id)
	}

	if err = rows.Err(); err != nil {
		return 0, fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return 0, fmt.Errorf("failed closing rows: %s", err)
	}

	// select data for ids to migrate from src
	stmt := fmt.Sprintf(
		"SELECT %s FROM %s",
		strings.Join(columnNamesForSelect, ","),
		table.Name,
	)

	if len(dstIDs) > 0 {
		placeholders := make([]string, len(dstIDs))
		for i := range dstIDs {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}

		stmt = fmt.Sprintf("%s WHERE id NOT IN (%s)", stmt, strings.Join(placeholders, ","))
	}
	file := fmt.Sprintf("/tmp/%s.csv", table.Name)

	// This doesn't do anything unless you are on the same server.
	if _, err := os.Stat(file); err == nil {
		err = os.Remove(file)
		if err != nil {
			return 0, fmt.Errorf("failed to remove CSV file: %s", err)
		}
	}

	stmt = fmt.Sprintf("COPY (%s) TO '%s' WITH CSV DELIMITER ','", stmt, file)

	_, err = src.DB().Exec(stmt, dstIDs...)
	if err != nil {
		return 0, fmt.Errorf("failed to write rows to CSV: %s", err)
	}
	cmd := exec.Command("chown", "mysql:mysql", file)
	err = cmd.Run()
	if err != nil {
		return 0, fmt.Errorf("failed to chown CSV file: %s", err)
	}

	// err = os.Chmod(file, 0777)
	// if err != nil {
	// 	return 0, fmt.Errorf("failed to chmod CSV file: %s", err)
	// }
	// err = os.Chown(file, os.Getuid(), os.Getgid())
	// if err != nil {
	// 	return 0, fmt.Errorf("failed to chown CSV file: %s", err)
	// }

	stmt = fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' (%s)", file, table.Name, strings.Join(columnNamesForSelect, ","))
	result, err := dst.DB().Exec(stmt)
	if err != nil {
		return 0, fmt.Errorf("failed to read rows from CSV for table %s: %s", table.Name, err)
	}
	// This doesn't do anything unless you are on the same server.
	if _, err := os.Stat(file); err == nil {
		err = os.Remove(file)
		if err != nil {
			return 0, fmt.Errorf("failed to remove CSV file: %s", err)
		}
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed getting rows affected by insert: %s", err)
	}

	// TODO: uncomment and then detect and skip empty tables.
	// if rowsAffected == 0 {
	// 	return 0, errors.New("no rows affected by insert")
	// }

	return rowsAffected, nil
}

func migrateWithIDs(watcher MigratorWatcher, src DB, dst DB, table *Table) (int64, error) {
	columnNamesForSelect := make([]string, len(table.Columns))
	batchSize := 1000
	var scanArgs, scanValues, values []interface{}
	var preparedStmt *sql.Stmt
	var preparedStmtSize int // number of rows the prepared statment can handle
	var count int64          // number of rows inserted

	for i := range table.Columns {
		columnNamesForSelect[i] = table.Columns[i].Name
	}

	// find ids already in dst
	rows, err := dst.DB().Query(fmt.Sprintf("SELECT id FROM %s", table.Name))
	if err != nil {
		return 0, fmt.Errorf("failed to select id from rows: %s", err)
	}

	var dstIDs []interface{}
	for rows.Next() {
		var id interface{}
		if err = rows.Scan(&id); err != nil {
			return 0, fmt.Errorf("failed to scan id from row: %s", err)
		}
		dstIDs = append(dstIDs, id)
	}

	if err = rows.Err(); err != nil {
		return 0, fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return 0, fmt.Errorf("failed closing rows: %s", err)
	}

	// select data for ids to migrate from src
	stmt := fmt.Sprintf(
		"SELECT %s FROM %s",
		strings.Join(columnNamesForSelect, ","),
		table.Name,
	)

	if len(dstIDs) > 0 {
		placeholders := make([]string, len(dstIDs))
		for i := range dstIDs {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}

		stmt = fmt.Sprintf("%s WHERE id NOT IN (%s)", stmt, strings.Join(placeholders, ","))
	}

	rows, err = src.DB().Query(stmt, dstIDs...)
	if err != nil {
		return 0, fmt.Errorf("failed to select rows: %s", err)
	}

	tx, err := dst.DB().Begin()
	if err != nil {
		return 0, fmt.Errorf("failed begin transaction: %s", err)
	}
	defer tx.Commit()

	hasNext := rows.Next()
	for hasNext {
		scanArgs = make([]interface{}, len(table.Columns))
		scanValues = make([]interface{}, len(table.Columns))
		for i := range table.Columns {
			scanArgs[i] = &scanValues[i]
		}
		if err = rows.Scan(scanArgs...); err != nil {
			return count, fmt.Errorf("failed to scan row: %s", err)
		}

		values = append(values, scanArgs...)
		hasNext = rows.Next()
		size := len(values) / len(table.Columns)

		if size >= batchSize || !hasNext {
			if size != preparedStmtSize || preparedStmt == nil {
				preparedStmt, err = preparedBulkInsert(dst, table, size)
				preparedStmtSize = size

				if err != nil {
					return count, fmt.Errorf("failed creating prepared bulk insert statement for batch: %s", err)
				}
			}

			err = insert(tx.Stmt(preparedStmt), values)
			values = nil

			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to insert into %s: %s\n", table.Name, err)
				continue
			}
			count += int64(size)
		}
	}

	if err = rows.Err(); err != nil {
		return count, fmt.Errorf("failed iterating through rows: %s", err)
	}

	if err = rows.Close(); err != nil {
		return count, fmt.Errorf("failed closing rows: %s", err)
	}

	return count, nil
}

func insert(stmt *sql.Stmt, values []interface{}) error {
	result, err := stmt.Exec(values...)
	if err != nil {
		return fmt.Errorf("failed to exec stmt: %s", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed getting rows affected by insert: %s", err)
	}

	if rowsAffected == 0 {
		return errors.New("no rows affected by insert")
	}

	return nil
}

func preparedBulkInsert(db DB, table *Table, size int) (*sql.Stmt, error) {
	columnNamesForInsert := make([]string, len(table.Columns))
	placeholders := make([]string, len(table.Columns))
	values := make([]string, size)
	for i := range table.Columns {
		columnNamesForInsert[i] = fmt.Sprintf("`%s`", table.Columns[i].Name)
		placeholders[i] = "?"
	}

	holders := strings.Join(placeholders, ",")
	for i := 0; i < size; i++ {
		values[i] = fmt.Sprintf("(%s)", holders)
	}

	preparedStmt, err := db.DB().Prepare(fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		table.Name,
		strings.Join(columnNamesForInsert, ","),
		strings.Join(values, ","),
	))
	if err != nil {
		return preparedStmt, fmt.Errorf("failed creating prepared statement: %s", err)
	}

	return preparedStmt, nil
}
