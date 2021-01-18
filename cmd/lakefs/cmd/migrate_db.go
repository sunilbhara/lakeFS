package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/catalog/migrate"
	"github.com/treeverse/lakefs/db"
)

// migrateDBCmd represents the db command
var migrateDBCmd = &cobra.Command{
	Use:   "db",
	Short: "Migrate MVCC based lakeFS database",
	Long:  `Migrate database content from MVCC model to the current format`,
	Run: func(cmd *cobra.Command, args []string) {
		dbParams := cfg.GetDatabaseParams()
		err := db.ValidateSchemaUpToDate(dbParams)
		if errors.Is(err, db.ErrSchemaNotCompatible) {
			fmt.Println("Migration version mismatch, for more information see https://docs.lakefs.io/deploying/upgrade.html")
			os.Exit(1)
		}
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		dbPool := db.BuildDatabaseConnection(dbParams)
		defer dbPool.Close()

		migrateTool, err := migrate.NewMigrate(dbPool, cfg)
		if err != nil {
			fmt.Println("Failed to create a new migrate:", err)
			os.Exit(1)
		}

		repoExpr, _ := cmd.Flags().GetString("repository")
		if err := migrateTool.FilterRepository(repoExpr); err != nil {
			fmt.Println("Failed to setup repository filter:", err)
			os.Exit(1)
		}

		err = migrateTool.Run()
		if err != nil {
			fmt.Println("Migration failed")
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

//nolint:gochecknoinits
func init() {
	migrateCmd.AddCommand(migrateDBCmd)
	migrateDBCmd.Flags().String("repository", "", "Repository filter (regexp)")
	migrateDBCmd.Flags().String("branch", "", "Branch filter (regexp)")
	migrateDBCmd.Flags().Bool("history", true, "Import complete history")
}
