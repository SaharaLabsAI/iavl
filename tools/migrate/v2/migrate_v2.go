package v2

import (
	"bytes"
	"database/sql"
	"fmt"
	"hash"
	"log"
	"os"
	"path/filepath"
	"strings"

	hashpool "github.com/cosmos/iavl/v2/common/pool/hash"
	nodepool3 "github.com/cosmos/iavl/v2/common/pool/node"
	iavl3 "github.com/cosmos/iavl/v2/db/sqlite"
	iavl2 "github.com/sahara/iavl/v2"
	"github.com/spf13/cobra"
	_ "modernc.org/sqlite"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "v2",
		Short: "migrate iavl2/ from v2 to v3 in sqlite",
	}
	cmd.AddCommand(V2toV3Command(), CheckHash())
	return cmd
}

func V2toV3Command() *cobra.Command {
	var (
		dbv2      string
		dbv3      string
		storeKeys []string
	)

	cmd := &cobra.Command{
		Use:   "v2tov3",
		Short: "migrate v2 to v3",
		Run: func(cmd *cobra.Command, args []string) {
			migrate(dbv2, dbv3, storeKeys)
		},
	}

	cmd.Flags().StringVar(&dbv2, "db-v2", "", "Path to the v2 root directory")
	cmd.Flags().StringVar(&dbv3, "db-v3", "", "Path to the v3 root directory")
	cmd.Flags().StringSliceVar(&storeKeys, "store-keys", nil, "Specific store keys to migrate (e.g., bank,acc). If not specified, migrate all stores")
	if err := cmd.MarkFlagRequired("db-v2"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("db-v3"); err != nil {
		panic(err)
	}

	return cmd
}

func migrateTree(oldPath, newPath string) error {
	// Open old db
	oldDB, err := sql.Open("sqlite", oldPath)
	if err != nil {
		return fmt.Errorf("open old db %s: %w", oldPath, err)
	}
	defer oldDB.Close()

	// Create target dir
	os.Remove(newPath)
	if err := os.MkdirAll(filepath.Dir(newPath), 0o777); err != nil {
		return err
	}
	newDB, err := sql.Open("sqlite", newPath)
	if err != nil {
		return fmt.Errorf("open new db %s: %w", newPath, err)
	}
	defer newDB.Close()

	exec := func(sqlStmt string) {
		if _, err := newDB.Exec(sqlStmt); err != nil {
			log.Fatalf("exec [%s]: %v", sqlStmt, err)
		}
	}

	// Create tables
	exec(`CREATE TABLE branch_orphan (
	  version INT, sequence INT, at INT,
	  PRIMARY KEY (at DESC, version, sequence)
	) WITHOUT ROWID;`)
	exec(`CREATE TABLE root (
	  version INT, node_version INT, node_sequence INT, bytes BLOB,
	  PRIMARY KEY (version DESC)
	) WITHOUT ROWID;`)
	exec(`CREATE TABLE tree_1 (
	  version INT, sequence INT, bytes BLOB, orphaned BOOL,
	  PRIMARY KEY (version, sequence)
	) WITHOUT ROWID;`)

	// ATTACH old db
	exec(fmt.Sprintf(`ATTACH DATABASE '%s' AS old;`, oldPath))

	// Migrate data
	log.Printf("migrating tree: table branch_orphan %s → %s\n", oldPath, newPath)
	exec(`INSERT INTO branch_orphan(version, sequence, at)
	      SELECT version, sequence, at FROM old.orphan;`)

	log.Printf("migrating tree: table root %s → %s\n", oldPath, newPath)
	exec(`INSERT INTO root(version, node_version, node_sequence, bytes)
	      SELECT version, node_version, node_sequence, bytes FROM old.root;`)

	log.Printf("migrating tree: table tree_1 %s → %s\n", oldPath, newPath)
	exec(`INSERT INTO tree_1(version, sequence, bytes, orphaned)
	      SELECT version, sequence, bytes, orphaned FROM (
	        SELECT version, sequence, bytes, orphaned,
	               ROW_NUMBER() OVER (PARTITION BY version, sequence ORDER BY rowid) as rn
	        FROM old.tree_1
	      ) WHERE rn = 1;`)

	// DETACH
	exec(`DETACH DATABASE old;`)

	log.Printf("finish migrating tree: %s → %s\n", oldPath, newPath)
	return nil
}

func migrateChangelog(oldPath, newPath string) error {
	log.Printf("migrating changelog: table leaf %s → %s\n", oldPath, newPath)
	oldDB, err := sql.Open("sqlite", oldPath)
	if err != nil {
		return fmt.Errorf("open old changelog db %s: %w", oldPath, err)
	}
	defer oldDB.Close()

	// create target dir
	os.Remove(newPath)
	if err := os.MkdirAll(filepath.Dir(newPath), 0o777); err != nil {
		return err
	}

	newDB, err := sql.Open("sqlite", newPath)
	if err != nil {
		return fmt.Errorf("open new changelog db %s: %w", newPath, err)
	}
	defer newDB.Close()

	tx, err := newDB.Begin()
	if err != nil {
		return err
	}

	// create tables
	createStmt := []string{
		`CREATE TABLE leaf (
			version INT,
			sequence INT,
			key_hash BLOB,
			bytes BLOB,
			orphaned BOOL,
			PRIMARY KEY (key_hash, version DESC)
		);`,
		`CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence);`,
		`CREATE TABLE leaf_orphan (
			version INT,
			sequence INT,
			at INT,
			PRIMARY KEY (at DESC, version, sequence)
		) WITHOUT ROWID;`,
	}
	for _, stmt := range createStmt {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("exec %s: %w", stmt, err)
		}
	}

	// read from old table
	// rows, err := oldDB.Query(`SELECT version, sequence, key, bytes, orphaned FROM leaf`)
	rows, err := oldDB.Query(`SELECT version, sequence, key, bytes FROM leaf`)

	if err != nil {
		return fmt.Errorf("read old leaf: %w", err)
	}
	defer rows.Close()

	// insertStmt, err := tx.Prepare(`INSERT INTO leaf(version, sequence, key_hash, bytes, orphaned) VALUES (?, ?, ?, ?, ?)`)
	insertStmt, err := tx.Prepare(`INSERT INTO leaf(version, sequence, key_hash, bytes) VALUES (?, ?, ?, ?)`)

	if err != nil {
		return err
	}
	defer insertStmt.Close()

	h := hashpool.Blake3Pool.Get().(hash.Hash)
	defer hashpool.Blake3Pool.Put(h)

	for rows.Next() {
		var (
			version, sequence int
			key, value        []byte
			// orphaned          bool
		)
		if err := rows.Scan(&version, &sequence, &key, &value); err != nil {
			return err
		}

		// calculate key_hash
		h.Reset()
		h.Write(key)
		keyHash := h.Sum(nil)

		// if _, err := insertStmt.Exec(version, sequence, keyHash[:], value, orphaned); err != nil {
		// 	return err
		// }
		if _, err := insertStmt.Exec(version, sequence, keyHash[:], value); err != nil {
			return err
		}
	}

	log.Printf("migrating changelog: table leaf_orphan %s → %s\n", oldPath, newPath)

	// ATTACH old db
	if _, err := tx.Exec(fmt.Sprintf(`ATTACH DATABASE '%s' AS old;`, oldPath)); err != nil {
		return fmt.Errorf("failed to attach old database: %w", err)
	}

	if _, err := tx.Exec(`INSERT INTO leaf_orphan(version, sequence, at)
		SELECT version, sequence, at FROM old.leaf_orphan;`); err != nil {
		return fmt.Errorf("migrate leaf_orphan: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	// DETACH
	if _, err := newDB.Exec(`DETACH DATABASE old;`); err != nil {
		return fmt.Errorf("failed to detach old database: %w", err)
	}
	log.Printf("finish migrating changelog: %s → %s\n", oldPath, newPath)

	return nil
}

func migrate(baseOld, baseNew string, storeKeys []string) {
	// baseOld := "data/iavl2"
	// baseNew := "data/iavl3"

	// Mgirate the specified store
	if len(storeKeys) > 0 {
		log.Printf("Migrating specified store keys: %v", storeKeys)
		for _, storeKey := range storeKeys {
			storePath := filepath.Join(baseOld, storeKey)
			if _, err := os.Stat(storePath); os.IsNotExist(err) {
				log.Printf("Store key directory does not exist: %s", storePath)
				continue
			}
			log.Printf("Processing store key: %s", storeKey)
			if err := migrateStore(storePath, filepath.Join(baseNew, storeKey)); err != nil {
				log.Fatal(err)
			}
		}
		return
	}

	// Default migrate all stores under the given path
	log.Printf("Migrating all store keys")
	var walkDir func(dir string) error
	walkDir = func(dir string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}

		for _, entry := range entries {
			path := filepath.Join(dir, entry.Name())

			if entry.IsDir() {
				// Recursively walk subdirectories
				if err := walkDir(path); err != nil {
					return err
				}
				continue
			}

			// Only process .sqlite files
			if !strings.HasSuffix(entry.Name(), ".sqlite") {
				continue
			}

			// Check if file still exists before processing
			if _, err := os.Stat(path); os.IsNotExist(err) {
				fmt.Printf("File no longer exists: %s\n", path)
				continue
			}

			if entry.Name() == "tree.sqlite" {
				fmt.Println("Processing tree.sqlite: ", path)
				rel, _ := filepath.Rel(baseOld, path)  // e.g. "acc/tree.sqlite"
				newPath := filepath.Join(baseNew, rel) // e.g. "data/iavl3/acc/tree.sqlite"
				if err := migrateTree(path, newPath); err != nil {
					return err
				}
			} else if entry.Name() == "changelog.sqlite" {
				fmt.Println("Processing changelog.sqlite: ", path)
				rel, _ := filepath.Rel(baseOld, path)  // e.g. "acc/changelog.sqlite"
				newPath := filepath.Join(baseNew, rel) // e.g. "data/iavl3/acc/changelog.sqlite"
				if err := migrateChangelog(path, newPath); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := walkDir(baseOld); err != nil {
		log.Fatal(err)
	}
}

// migrateStore migrate dbs from unit store dir
func migrateStore(storeOldPath, storeNewPath string) error {
	entries, err := os.ReadDir(storeOldPath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		path := filepath.Join(storeOldPath, entry.Name())

		// Only process .sqlite files
		if !strings.HasSuffix(entry.Name(), ".sqlite") {
			continue
		}

		// Check if file still exists before processing
		if _, err := os.Stat(path); os.IsNotExist(err) {
			fmt.Printf("File no longer exists: %s\n", path)
			continue
		}

		if entry.Name() == "tree.sqlite" {
			fmt.Println("Processing tree.sqlite: ", path)
			newPath := filepath.Join(storeNewPath, "tree.sqlite")
			if err := migrateTree(path, newPath); err != nil {
				return err
			}
		} else if entry.Name() == "changelog.sqlite" {
			fmt.Println("Processing changelog.sqlite: ", path)
			newPath := filepath.Join(storeNewPath, "changelog.sqlite")
			if err := migrateChangelog(path, newPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func CheckHash() *cobra.Command {
	var (
		dbv2 string
		dbv3 string
		sk   string
	)

	cmd := &cobra.Command{
		Use:   "check-hash",
		Short: "check tree root hash between old tree and migrated new tree",
		Run: func(cmd *cobra.Command, args []string) {

			v2sql, err := iavl2.NewSqliteDb(iavl2.NewNodePool(), iavl2.DefaultSqliteDbOptions(iavl2.SqliteDbOptions{Path: fmt.Sprintf("%s/%s", dbv2, sk)}))
			if err != nil {
				panic(err)
			}
			v2version, err := v2sql.LatestVersion()
			if err != nil {
				panic(err)
			}
			fmt.Println("v2 path: ", fmt.Sprintf("%s/%s", dbv2, sk), "version: ", v2version)
			v2root, err := v2sql.LoadRoot(v2version)
			if err != nil {
				panic(err)
			}
			v2hash := v2root.GetHash()
			fmt.Printf("v2 root hash: %x \n", v2hash)

			// v2mt := iavl2.NewMultiTree(iavl2.NewDebugLogger(), dbv2, iavl2.DefaultTreeOptions())
			// if err := v2mt.MountTree(sk); err != nil {
			// 	panic(err)
			// }
			// v2tr := v2mt.Trees[sk]

			// v2tr.LoadVersion(1)
			// v2tr.LatsetVersion()
			// fmt.Println("iavl2 NewMultiTree finish")

			v3sql, err := iavl3.NewDB(iavl3.Options{
				Path:    fmt.Sprintf("%s/%s", dbv3, sk),
				WalSize: 1024 * 1024 * 1024,
			})
			if err != nil {
				panic(err)
			}
			v3version, err := v3sql.LatestVersion()
			if err != nil {
				panic(err)
			}
			if v2version != v3version {
				panic("version not match")
			}

			v3root, err := v3sql.LoadRoot(nodepool3.NewNodePool(), v3version)
			if err != nil {
				panic(err)
			}
			v3hash := v3root.Hash()

			if !bytes.Equal(v2hash, v3hash) {
				panic("hash not match")
			}
			log.Printf("check finished, latest version %d, root hash %x", v2version, v2hash)
		},
	}

	cmd.Flags().StringVar(&dbv2, "db-v2", "", "Path to the v2 root directory")
	cmd.Flags().StringVar(&dbv3, "db-v3", "", "Path to the v3 root directory")
	cmd.Flags().StringVar(&sk, "store-key", "", "The store which is going to be checked")
	if err := cmd.MarkFlagRequired("db-v2"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("db-v3"); err != nil {
		panic(err)
	}
	if err := cmd.MarkFlagRequired("store-key"); err != nil {
		panic(err)
	}

	return cmd
}
