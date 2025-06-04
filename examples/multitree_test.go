package examples

// func TestTree_Hash(t *testing.T) {
// 	var err error
// 	tmpDir := t.TempDir()
//
// 	require.NoError(t, err)
// 	opts := testutil.BigTreeOptions_100_000()
//
// 	// this hash was validated as correct (with this same dataset) in iavl-bench
// 	// with `go run . tree --seed 1234 --dataset std`
// 	// at this commit tree: https://github.com/cosmos/iavl-bench/blob/3a6a1ec0a8cbec305e46239454113687da18240d/iavl-v0/main.go#L136
// 	opts.Until = 100
// 	opts.UntilHash = "0101e1d6f3158dcb7221acd7ed36ce19f2ef26847ffea7ce69232e362539e5cf"
// 	treeOpts := TreeOptions{
// 		HeightFilter: 1, StateStorage: true, EvictionDepth: 14, MetricsProxy: metrics.NewStructMetrics(),
// 	}
//
// 	testStart := time.Now()
// 	testStart = time.Now()
// 	multiTree := NewMultiTree(NewTestLogger(), tmpDir, treeOpts)
// 	itrs, ok := opts.Iterator.(*bench.ChangesetIterators)
// 	require.True(t, ok)
// 	for _, sk := range itrs.StoreKeys() {
// 		require.NoError(t, multiTree.MountTree(sk))
// 	}
// 	leaves, err := multiTree.TestBuild(opts)
// 	require.NoError(t, err)
// 	treeDuration := time.Since(testStart)
// 	fmt.Printf("mean leaves/s: %s\n", humanize.Comma(int64(float64(leaves)/treeDuration.Seconds())))
//
// 	require.NoError(t, multiTree.Close())
// }
//
// func TestTree_Build_Load(t *testing.T) {
// 	tmpDir := t.TempDir()
// 	opts := testutil.NewTreeBuildOptions().With10_000()
// 	multiTree := NewMultiTree(NewTestLogger(), tmpDir, TreeOptions{
// 		HeightFilter: 0, StateStorage: true, EvictionDepth: 14, MetricsProxy: metrics.NewStructMetrics(),
// 	})
// 	itrs, ok := opts.Iterator.(*bench.ChangesetIterators)
// 	require.True(t, ok)
// 	for _, sk := range itrs.StoreKeys() {
// 		require.NoError(t, multiTree.MountTree(sk))
// 	}
// 	t.Log("building initial tree to version 10,000")
// 	_, err := multiTree.TestBuild(opts)
// 	require.NoError(t, err)
//
// 	t.Log("snapshot tree at version 10,000")
// 	// take a snapshot at version 10,000
// 	require.NoError(t, multiTree.SnapshotConcurrently())
// 	require.NoError(t, multiTree.Close())
//
// 	t.Log("import snapshot into new tree")
// 	mt, err := ImportMultiTree(NewTestLogger(), 10_000, tmpDir, DefaultTreeOptions())
// 	require.NoError(t, err)
//
// 	t.Log("build tree to version 12,000 and verify hash")
// 	require.NoError(t, opts.Iterator.Next())
// 	require.Equal(t, int64(10_001), opts.Iterator.Version())
// 	opts.Until = 12_000
// 	opts.UntilHash = "3a037f8dd67a5e1a9ef83a53b81c619c9ac0233abee6f34a400fb9b9dfbb4f8d"
// 	_, err = mt.TestBuild(opts)
// 	require.NoError(t, err)
// 	require.NoError(t, mt.Close())
// }
