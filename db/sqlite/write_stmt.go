package sqlite

// Tables (two databases: branches and leaves)
const StmtCreateTreeTables = `
CREATE TABLE branch_orphan (version int, sequence int, at int, PRIMARY KEY (at DESC, version, sequence)) WITHOUT ROWID;
CREATE TABLE root (version int, node_version int, node_sequence int, bytes blob, PRIMARY KEY (version DESC)) WITHOUT ROWID;
`

// NOTE: tree_%d should be filled
const StmtCreateTreeBranchShardTableFormat = `
CREATE TABLE tree_%d (version int, sequence int, bytes blob, orphaned bool, PRIMARY KEY (version, sequence)) WITHOUT ROWID;
`

// NOTE: we need leaf_idx, so we cannot use `WITHOUT ROWID` because leaf_idx must store the full PRIMARY KEY as their row reference
const StmtCreateLeafTables = `
CREATE TABLE leaf (version int, sequence int, key_hash blob, bytes blob, orphaned bool, PRIMARY KEY (key_hash, version DESC));
CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence);
CREATE TABLE leaf_orphan (version int, sequence int, at int, PRIMARY KEY (at DESC, version, sequence)) WITHOUT ROWID;
`

// Insert
const StmtInsertRoot = `
INSERT OR REPLACE INTO root(version, node_version, node_sequence, bytes) VALUES (?, ?, ?, ?)
`

// NOTE: Every time we mutate node during balance/set/remove, touched branch
// nodes always get new node key.
// But Test_Replay will try to ingest nodes so we should allow REPLACE here.
const StmtInsertBranchShardFormat = `
INSERT OR REPLACE INTO tree_%d (version, sequence, bytes) VALUES (?, ?, ?)
`

const StmtInsertBranchOrphan = `
INSERT OR REPLACE INTO branch_orphan (version, sequence, at) VALUES (?, ?, ?)
`

const StmtInsertLeaf = `
INSERT OR REPLACE INTO leaf (version, sequence, key_hash, bytes) VALUES (?, ?, ?, ?)
`

const StmtInsertLeafOrphan = `
INSERT OR REPLACE INTO leaf_orphan (version, sequence, at) VALUES (?, ?, ?)
`
