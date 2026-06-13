package sqlstorage

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

import (
	"embed"
)

// EmbeddedMigrations Embed all SQL files from the migrations directory.
//
//go:embed migrations/*.sql
var EmbeddedMigrations embed.FS

const MigrationsTable = "gows_migrations"

func (c *GContainer) Migrate() error {
	// Use the embedded migrations as the source
	source, err := iofs.New(EmbeddedMigrations, "migrations")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}
	var driver database.Driver
	switch {
	case c.dialect == "sqlite3" || c.dialect == "sqlite":
		driver, err = sqlite3.WithInstance(c.db.DB, &sqlite3.Config{
			MigrationsTable: MigrationsTable,
		})
	case c.dialect == "postgres":
		// Create a new connection to the database
		// We need to Close it after the migration is done
		// Otherwise, the connection will hang until the program exits
		// "SELECT pg_advisory_unlock($1)"
		conn, err := c.db.Connx(context.Background())
		if err != nil {
			return fmt.Errorf("failed to get database connection: %w", err)
		}
		driver, err = postgres.WithConnection(
			context.Background(),
			conn.Conn,
			&postgres.Config{
				MigrationsTable: MigrationsTable,
			})
		defer driver.Close()
	}

	if err != nil {
		return fmt.Errorf("failed to create database driver: %w", err)
	}
	m, err := migrate.NewWithInstance(
		"iofs",
		source,
		"gowsstore",
		driver,
	)
	if err != nil {
		return fmt.Errorf("failed to create migration instance: %w", err)
	}
	err = m.Up()
	if err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			return nil
		}
		return fmt.Errorf("failed to apply migrations: %w", err)
	}
	return nil
}
