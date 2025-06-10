package tree

import (
	"errors"
	"fmt"
)

func (tree *Tree) VersionExists(version int64) (bool, error) {
	exists, err := tree.db.HasRoot(version)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (tree *Tree) Version() int64 {
	return tree.version.Load()
}

func (tree *Tree) LoadVersion(version int64) (err error) {
	if tree.db == nil {
		return errors.New("db is nil")
	}

	if version == 0 {
		return nil
	}

	tree.version.Store(version)
	if tree.immutable {
		exists, err := tree.db.HasRoot(version)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("root not found for version %d", version)
		}

		return nil
	}

	tree.rw.Lock()
	defer tree.rw.Unlock()

	tree.workingBytes = 0
	tree.workingSize = 0

	tree.root, err = tree.db.LoadRoot(tree.nodePool, version)
	if err != nil {
		return err
	}

	tree.hashedVersion = version
	tree.cache = make(map[string][]byte)
	tree.deleted = make(map[string]bool)

	return nil
}

func (tree *Tree) SetInitialVersion(version int64) error {
	if tree.immutable {
		panic("set initial version on immutable tree")
	}

	var err error

	tree.version.Store(version - 1)

	return err
}

func (tree *Tree) SaveVersion() ([]byte, int64, error) {
	tree.rw.Lock()
	defer tree.rw.Unlock()

	// if err := tree.sql.closeHangingIterators(); err != nil {
	// 	return nil, 0, err
	// }

	dirtyTreeVersion := tree.version.Load()
	savedTreeVersion := dirtyTreeVersion + 1

	rootHash := tree.computeHash()

	tree.version.Add(1)
	tree.resetSequences()
	tree.dirtyNodes.Version = savedTreeVersion

	err := tree.db.SaveTree(savedTreeVersion, tree.root, tree.dirtyNodes)
	if err != nil {
		return nil, dirtyTreeVersion, err
	}

	if tree.heightFilter > 0 {
		for i, leaf := range tree.dirtyNodes.Leaves {
			if i != 0 {
				// evict leaf
				tree.returnNode(leaf)
			} else if leaf.NodeKey() != tree.root.NodeKey() {
				// never evict the root if it's a leaf
				tree.returnNode(leaf)
			}
		}
	}
	for _, branch := range tree.dirtyNodes.Branches {
		if branch.Evict() {
			tree.returnNode(branch)
		}
	}

	if err := tree.db.ResetRead(); err != nil {
		return nil, savedTreeVersion, err
	}

	tree.resetSequences()
	tree.dirtyNodes.Reset()
	tree.deleted = make(map[string]bool)
	tree.cache = make(map[string][]byte)
	tree.modificationCount = 0

	return rootHash, savedTreeVersion, nil
}

func (tree *Tree) Revert(version int64) error {
	return tree.db.Revert(version)
}
