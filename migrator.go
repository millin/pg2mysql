package pg2mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type Migrator interface {
	Migrate() error
}

func NewMigrator(src, dst DB, truncateFirst bool, workers int, watcher MigratorWatcher) Migrator {
	return &migrator{
		src:           src,
		dst:           dst,
		truncateFirst: truncateFirst,
		workers:       workers,
		watcher:       watcher,
	}
}

type migrator struct {
	src, dst      DB
	truncateFirst bool
	workers       int
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

	jobs := make(chan *Table, len(srcSchema.Tables))
	results := make(chan error, len(srcSchema.Tables))

	for w := 1; w <= m.workers; w++ {
		go migrateWorker(w, m, jobs, results)
	}

	for _, table := range srcSchema.Tables {
		jobs <- table
	}
	close(jobs)

	migrationErrs := new(strings.Builder)
	for _ = range srcSchema.Tables {
		err := <-results
		if err != nil {
			migrationErrs.WriteString(fmt.Sprintf("    %s\n", err))
		}
	}
	if migrationErrs.Len() > 0 {
		return fmt.Errorf("migration failed with error(s):\n%s", migrationErrs)
	}

	return nil
}

func migrateWorker(id int, m *migrator, jobs <-chan *Table, results chan<- error) {
	for j := range jobs {
		var watcher MigratorWatcher
		buffer := new(strings.Builder)
		if m.workers > 1 {
			watcher = NewPrinter(buffer) // this currently breaks the idea of a MigratorWatcher
		} else {
			watcher = m.watcher
		}

		err := migrateTable(m.truncateFirst, watcher, m.src, m.dst, j)

		if buffer.Len() > 0 {
			fmt.Print(buffer.String())
		}
		results <- err
	}
}

func migrateTable(truncate bool, watcher MigratorWatcher, src DB, dst DB, table *Table) error {
	var err error

	if truncate {
		watcher.WillTruncateTable(table.Name)
		_, err := dst.DB().Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", table.Name))
		if err != nil {
			return fmt.Errorf("failed truncating: %s", err)
		}
		watcher.TruncateTableDidFinish(table.Name)
	}

	var recordsInserted int64

	watcher.TableMigrationDidStart(table.Name)

	if table.HasColumn("id") {
		recordsInserted, err = migrateWithIDs(src, dst, table)
		if err != nil {
			return fmt.Errorf("failed migrating table with ids: %s", err)
		}
	} else {
		preparedStmt, err := preparedBulkInsert(dst, table, 1)
		if err != nil {
			return fmt.Errorf("failed creating prepared bulk insert statement: %s", err)
		}

		err = EachMissingRow(src, dst, table, func(scanArgs []interface{}) error {
			err := insert(preparedStmt, scanArgs)
			if err != nil {
				return fmt.Errorf("failed to insert into %s: %s\n", table.Name, err)
			}
			recordsInserted++

			return nil
		})
		if err != nil {
			return fmt.Errorf("failed migrating table without ids: %s", err)
		}
	}

	watcher.TableMigrationDidFinish(table.Name, recordsInserted)

	return nil
}

func migrateWithIDs(src DB, dst DB, table *Table) (int64, error) {
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
	rows, err := dst.DB().Query(fmt.Sprintf("SELECT id FROM `%s`", table.Name))
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
				return count, fmt.Errorf("failed to insert into %s: %s\n", table.Name, err)
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
		"INSERT INTO `%s` (%s) VALUES %s",
		table.Name,
		strings.Join(columnNamesForInsert, ","),
		strings.Join(values, ","),
	))
	if err != nil {
		return preparedStmt, fmt.Errorf("failed creating prepared statement: %s", err)
	}

	return preparedStmt, nil
}
