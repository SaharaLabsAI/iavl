package tree

//	func (tree *Tree) LoadSnapshot(version int64, traverseOrder TraverseOrderType) (err error) {
//		tree.rw.Lock()
//		defer tree.rw.Unlock()
//
//		var v int64
//		tree.root, v, err = tree.sql.ImportMostRecentSnapshot(version, traverseOrder, true)
//		if err != nil {
//			return err
//		}
//		if v < version {
//			return fmt.Errorf("requested %d found snapshot %d, replay not yet supported", version, v)
//		}
//		tree.version.Store(v)
//		tree.hashedVersion = v
//		tree.cache = make(map[string][]byte)
//		tree.deleted = make(map[string]bool)
//		return nil
//	}
//
//	func (tree *Tree) SaveSnapshot() (err error) {
//		tree.rw.Lock()
//		defer tree.rw.Unlock()
//
//		ctx := context.Background()
//		return tree.sql.Snapshot(ctx, tree)
//	}
