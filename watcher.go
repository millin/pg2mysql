package pg2mysql

import (
	"fmt"
	"io"
	"os"
	"strings"
)

//go:generate counterfeiter . VerifierWatcher

type VerifierWatcher interface {
	TableVerificationDidStart(tableName string)
	TableVerificationDidFinish(tableName string, missingRows int64, missingIDs []string)
	TableVerificationDidFinishWithError(tableName string, err error)
}

//go:generate counterfeiter . MigratorWatcher

type MigratorWatcher interface {
	WillBuildSchema()
	DidBuildSchema()

	WillDisableConstraints()
	DidDisableConstraints()

	WillEnableConstraints()
	EnableConstraintsDidFinish()
	EnableConstraintsDidFailWithError(err error)

	WillTruncateTable(tableName string)
	TruncateTableDidFinish(tableName string)

	TableMigrationDidStart(tableName string)
	TableMigrationDidFinish(tableName string, recordsInserted int64)

	DidMigrateRow(tableName string)
	DidFailToMigrateRowWithError(tableName string, err error)
}

func NewStdoutPrinter() *StdoutPrinter {
	return NewPrinter(os.Stdout)
}

func NewPrinter(w io.Writer) *StdoutPrinter {
	return &StdoutPrinter{
		writer: w,
	}
}

type StdoutPrinter struct {
	writer io.Writer
}

func (s *StdoutPrinter) write(format string, a ...interface{}) {
	fmt.Fprintf(s.writer, format, a...)
}

func (s *StdoutPrinter) writeln(format string, a ...interface{}) {
	s.write(format, a...)
	s.write("\n")
}

func (s *StdoutPrinter) TableVerificationDidStart(tableName string) {
	s.write("Verifying table %s...", tableName)
}

func (s *StdoutPrinter) TableVerificationDidFinish(tableName string, missingRows int64, missingIDs []string) {
	if missingRows != 0 {
		if missingRows == 1 {
			s.writeln("\n\tFAILED: 1 row missing")
		} else {
			s.write("\n\tFAILED: %d rows missing\n", missingRows)
		}
		if missingIDs != nil {
			s.write("\tMissing IDs: %v\n", strings.Join(missingIDs, ","))
		}
	} else {
		s.done()
	}
}

func (s *StdoutPrinter) done() {
	s.writeln("OK")
}

func (s *StdoutPrinter) TableVerificationDidFinishWithError(tableName string, err error) {
	s.write("failed: %s", err)
}

func (s *StdoutPrinter) WillBuildSchema() {
	s.write("Building schema...")
}

func (s *StdoutPrinter) DidBuildSchema() {
	s.done()
}

func (s *StdoutPrinter) WillDisableConstraints() {
	s.write("Disabling constraints...")
}

func (s *StdoutPrinter) DidDisableConstraints() {
	s.done()
}

func (s *StdoutPrinter) DidFailToDisableConstraints(err error) {
	s.done()
}

func (s *StdoutPrinter) WillEnableConstraints() {
	s.write("Enabling constraints...")
}

func (s *StdoutPrinter) EnableConstraintsDidFailWithError(err error) {
	s.write("failed: %s", err)
}

func (s *StdoutPrinter) EnableConstraintsDidFinish() {
	s.done()
}

func (s *StdoutPrinter) WillTruncateTable(tableName string) {
	s.write("Truncating %s...", tableName)
}

func (s *StdoutPrinter) TruncateTableDidFinish(tableName string) {
	s.done()
}

func (s *StdoutPrinter) TableMigrationDidStart(tableName string) {
	s.write("Migrating %s...", tableName)
}

func (s *StdoutPrinter) TableMigrationDidFinish(tableName string, recordsInserted int64) {
	switch recordsInserted {
	case 0:
		s.writeln("OK (0 records inserted)")
	case 1:
		s.writeln("OK\n  inserted 1 row")
	default:
		s.write("OK\n  inserted %d rows\n", recordsInserted)
	}
}

func (s *StdoutPrinter) DidMigrateRow(tableName string) {
	s.write(".")
}

func (s *StdoutPrinter) DidFailToMigrateRowWithError(tableName string, err error) {
	s.write("x")
}
