package commands

import (
	"fmt"
	"runtime"

	"github.com/millin/pg2mysql"
)

type MigrateCommand struct {
	Truncate bool `long:"truncate" description:"Truncate destination tables before migrating data"`
	Workers  int  `long:"workers" description:"Migrate this many tables in goroutines"`
}

func (c *MigrateCommand) Execute([]string) error {
	mysql := pg2mysql.NewMySQLDB(
		PG2MySQL.Config.MySQL.Database,
		PG2MySQL.Config.MySQL.Username,
		PG2MySQL.Config.MySQL.Password,
		PG2MySQL.Config.MySQL.Host,
		PG2MySQL.Config.MySQL.Port,
	)

	err := mysql.Open()
	if err != nil {
		return fmt.Errorf("failed to open mysql connection: %s", err)
	}
	defer mysql.Close()

	pg := pg2mysql.NewPostgreSQLDB(
		PG2MySQL.Config.PostgreSQL.Database,
		PG2MySQL.Config.PostgreSQL.Username,
		PG2MySQL.Config.PostgreSQL.Password,
		PG2MySQL.Config.PostgreSQL.Host,
		PG2MySQL.Config.PostgreSQL.Port,
		PG2MySQL.Config.PostgreSQL.SSLMode,
	)
	err = pg.Open()
	if err != nil {
		return fmt.Errorf("failed to open pg connection: %s", err)
	}
	defer pg.Close()

	if c.Workers > runtime.NumCPU() {
		// though this is technically valid and OK, it does not help with performance I would think.
		return fmt.Errorf("number of workers exceeds number of CPUs (%d > %d)", c.Workers, runtime.NumCPU())
	}
	if c.Workers < 1 {
		c.Workers = 1
	}

	watcher := pg2mysql.NewStdoutPrinter()
	err = pg2mysql.NewMigrator(pg, mysql, c.Truncate, c.Workers, watcher).Migrate()
	if err != nil {
		return fmt.Errorf("failed migrating: %s", err)
	}

	return nil
}
