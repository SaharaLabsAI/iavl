package tree

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"testing"
	"unsafe"

	"github.com/cosmos/iavl-bench/bench"
	api "github.com/kocubinski/costor-api"
	"github.com/stretchr/testify/require"

	"github.com/cosmos/iavl/v2/db/sqlite"
	"github.com/cosmos/iavl/v2/logger"
	nodepool "github.com/cosmos/iavl/v2/pool/node"
	"github.com/cosmos/iavl/v2/testutil"
	nodetypes "github.com/cosmos/iavl/v2/types/node"
)

func rehashTree(node *nodetypes.Node) {
	if node.IsLeaf() {
		return
	}
	node.SetHash(nil)

	rehashTree(node.LeftNode())
	rehashTree(node.RightNode())

	node.HashNode()
}

func TestTreeSanity(t *testing.T) {
	cases := []struct {
		name   string
		treeFn func() *Tree
		hashFn func(*Tree) []byte
	}{
		{
			name: "sqlite",
			treeFn: func() *Tree {
				pool := nodepool.NewNodePool()
				dbPath := t.TempDir()
				sql, err := sqlite.NewSqliteDb(pool, sqlite.SqliteDbOptions{Path: dbPath})
				// sql, err := NewInMemorySqliteDb(pool)
				require.NoError(t, err)
				return NewTree(sql, pool, DefaultTreeOptions())
			},
			hashFn: func(tree *Tree) []byte {
				hash, _, err := tree.SaveVersion()
				require.NoError(t, err)
				return hash
			},
		},
		{
			name: "no db",
			treeFn: func() *Tree {
				pool := nodepool.NewNodePool()
				return NewTree(nil, pool, DefaultTreeOptions())
			},
			hashFn: func(tree *Tree) []byte {
				rehashTree(tree.root)
				tree.version.Add(1)
				return tree.root.Hash()
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tree := tc.treeFn()
			opts := testutil.NewTreeBuildOptions()
			itr := opts.Iterator
			var err error
			for ; itr.Valid(); err = itr.Next() {
				if itr.Version() > 150 {
					break
				}
				require.NoError(t, err)
				nodes := itr.Nodes()
				for ; nodes.Valid(); err = nodes.Next() {
					require.NoError(t, err)
					node := nodes.GetNode()
					if node.Delete {
						_, _, err := tree.Remove(node.Key)
						require.NoError(t, err)
					} else {
						_, err := tree.Set(node.Key, node.Value)
						require.NoError(t, err)
					}
				}
				switch itr.Version() {
				case 1:
					h := tc.hashFn(tree)
					require.Equal(t, "48c3113b8ba523d3d539d8aea6fce28814e5688340ba7334935c1248b6c11c7a", hex.EncodeToString(h))
					require.Equal(t, int64(104938), tree.root.Size())
					fmt.Printf("version=%d, hash=%x size=%d\n", itr.Version(), h, tree.root.Size())
				case 150:
					h := tc.hashFn(tree)
					require.Equal(t, "04c42dd1cec683cbbd4974027e4b003b848e389a33d03d7a9105183e6d108dd9", hex.EncodeToString(h))
					require.Equal(t, int64(105030), tree.root.Size())
					fmt.Printf("version=%d, hash=%x size=%d\n", itr.Version(), h, tree.root.Size())
				}
			}
		})
	}
}

func Test_EmptyTree(t *testing.T) {
	pool := nodepool.NewNodePool()
	dbPath := t.TempDir()
	sql, err := sqlite.NewSqliteDb(pool, sqlite.SqliteDbOptions{Path: dbPath})
	require.NoError(t, err)
	tree := NewTree(sql, pool, DefaultTreeOptions())

	_, err = tree.Set([]byte("foo"), []byte("bar"))
	require.NoError(t, err)
	_, err = tree.Set([]byte("baz"), []byte("qux"))
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	_, _, err = tree.Remove([]byte("foo"))
	require.NoError(t, err)
	_, _, err = tree.SaveVersion()
	require.NoError(t, err)

	_, _, err = tree.Remove([]byte("baz"))
	require.NoError(t, err)
	hash, version, err := tree.SaveVersion()
	require.NoError(t, err)

	require.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", hex.EncodeToString(sha256.New().Sum(nil)))
	require.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", hex.EncodeToString(hash))

	err = tree.LoadVersion(version)
	require.NoError(t, err)
}

func Test_Replay(t *testing.T) {
	unsafeBytesToStr := func(b []byte) string {
		return *(*string)(unsafe.Pointer(&b))
	}
	const versions = int64(1_000)
	gen := bench.ChangesetGenerator{
		StoreKey:         "replay",
		Seed:             1,
		KeyMean:          20,
		KeyStdDev:        3,
		ValueMean:        20,
		ValueStdDev:      3,
		InitialSize:      20,
		FinalSize:        500,
		Versions:         versions,
		ChangePerVersion: 10,
		DeleteFraction:   0.2,
	}
	itr, err := gen.Iterator()
	require.NoError(t, err)

	pool := nodepool.NewNodePool()
	tmpDir := t.TempDir()
	sql, err := sqlite.NewSqliteDb(pool, sqlite.SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)
	opts := DefaultTreeOptions()
	tree := NewTree(sql, pool, opts)

	// we must buffer all sets/deletes and order them first for replay to work properly.
	// store v1 and v2 already do this via cachekv write buffering.
	// from cachekv a nil value is treated as a deletion; it is a domain requirement of the SDK that nil values are disallowed
	// since from the perspective of the cachekv they are indistinguishable from a deletion.

	ingest := func(start, last int64) {
		for ; itr.Valid(); err = itr.Next() {
			if itr.Version() > last {
				break
			}
			require.NoError(t, err)
			changeset := itr.Nodes()
			cache := make(map[string]*api.Node)
			for ; changeset.Valid(); err = changeset.Next() {
				require.NoError(t, err)
				node := changeset.GetNode()
				if itr.Version() < start {
					continue
				}
				if !node.Delete {
					// merge multiple sets into one set
					cache[unsafeBytesToStr(node.Key)] = node
				} else {
					cache[unsafeBytesToStr(node.Key)] = nil
				}
			}
			keys := make([]string, 0, len(cache))
			for k := range cache {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				node := cache[k]
				if node == nil {
					_, _, err := tree.Remove([]byte(k))
					require.NoError(t, err)
				} else {
					_, err := tree.Set([]byte(k), node.Value)
					require.NoError(t, err)
				}
			}

			if len(cache) > 0 {
				_, v, err := tree.SaveVersion()
				fmt.Printf("version=%d, hash=%x\n", v, tree.Hash())
				require.NoError(t, err)
			}
		}

		require.NoError(t, tree.Close())
	}

	ingest(1, 150)

	sql, err = sqlite.NewSqliteDb(pool, sqlite.SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)
	tree = NewTree(sql, pool, opts)
	err = tree.LoadVersion(140)
	require.NoError(t, err)
	itr, err = gen.Iterator()
	require.NoError(t, err)
	ingest(141, 170)

	sql, err = sqlite.NewSqliteDb(pool, sqlite.SqliteDbOptions{Path: tmpDir})
	require.NoError(t, err)
	tree = NewTree(sql, pool, opts)
	err = tree.LoadVersion(170)
	require.NoError(t, err)
	itr, err = gen.Iterator()
	require.NoError(t, err)
	ingest(171, 250)
}

func Test_Prune_Logic(t *testing.T) {
	const versions = int64(1_000)
	gen := bench.ChangesetGenerator{
		StoreKey:         "replay",
		Seed:             1,
		KeyMean:          20,
		KeyStdDev:        3,
		ValueMean:        20,
		ValueStdDev:      3,
		InitialSize:      20,
		FinalSize:        500,
		Versions:         versions,
		ChangePerVersion: 10,
		DeleteFraction:   0.2,
	}
	itr, err := gen.Iterator()
	require.NoError(t, err)

	pool := nodepool.NewNodePool()
	tmpDir := t.TempDir()
	sql, err := sqlite.NewSqliteDb(pool, sqlite.SqliteDbOptions{Path: tmpDir, ShardTrees: false, Logger: logger.NewDebugLogger()})
	require.NoError(t, err)
	treeOpts := DefaultTreeOptions()
	tree := NewTree(sql, pool, treeOpts)

	for ; itr.Valid(); err = itr.Next() {
		require.NoError(t, err)
		changeset := itr.Nodes()
		for ; changeset.Valid(); err = changeset.Next() {
			require.NoError(t, err)
			node := changeset.GetNode()
			if node.Delete {
				_, _, err := tree.Remove(node.Key)
				require.NoError(t, err)
			} else {
				_, err := tree.Set(node.Key, node.Value)
				require.NoError(t, err)
			}
		}
		_, version, err := tree.SaveVersion()
		fmt.Printf("version=%d, hash=%x\n", version, tree.Hash())
		switch version {
		case 30:
			require.NoError(t, tree.DeleteVersionsTo(20))
		case 100:
			require.NoError(t, tree.DeleteVersionsTo(100))
		case 150:
			require.NoError(t, tree.DeleteVersionsTo(140))
		case 650:
			require.NoError(t, tree.DeleteVersionsTo(650))
		}
		require.NoError(t, err)
	}
}
