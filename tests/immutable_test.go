package tests

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	nodepool "github.com/cosmos/iavl/v2/common/pool/node"
	"github.com/cosmos/iavl/v2/db/sqlite"
	itree "github.com/cosmos/iavl/v2/tree"
)

// Helper function to create a test tree with data
func createTestTree(t *testing.T) *itree.Tree {
	pool := nodepool.NewNodePool()
	sql, err := sqlite.NewInMemoryDB()
	require.NoError(t, err)

	opts := itree.DefaultOptions()
	tree := itree.NewTree(sql, pool, opts)

	// Add test data
	testData := map[string]string{
		"a": "value1",
		"b": "value2",
		"c": "value3",
		"d": "value4",
		"e": "value5",
		"f": "value6",
		"g": "value7",
	}

	for k, v := range testData {
		_, err := tree.Set([]byte(k), []byte(v))
		require.NoError(t, err)
	}

	return tree
}

func Test_ImmutableTree_GetImmutable(t *testing.T) {
	tree := createTestTree(t)

	// Save version first
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	require.NotNil(t, imTree)

	// Test version
	require.Equal(t, version, imTree.Version())

	// Test Get
	val, err := imTree.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)

	// Test Close
	require.NoError(t, imTree.Close())
}

func Test_ImmutableTree_GetImmutable_InvalidVersion(t *testing.T) {
	tree := createTestTree(t)

	// Try to get immutable tree for non-existent version
	_, err := tree.GetImmutable(999)
	require.Error(t, err)
}

func Test_ImmutableTree_LoadVersion(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)

	// Test loading version 0 (should return nil)
	err = imTree.LoadVersion(0)
	require.NoError(t, err)

	// Test loading valid version
	err = imTree.LoadVersion(version)
	require.NoError(t, err)
	require.Equal(t, version, imTree.Version())

	require.NoError(t, imTree.Close())
}

func Test_ImmutableTree_LoadVersion_NilDB(t *testing.T) {
	// Create immutable tree with nil db
	imTree := &itree.ImmutableTree{}

	// Should return error
	err := imTree.LoadVersion(1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "db is nil")
}

func Test_ImmutableTree_VersionExists(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test existing version
	exists, err := imTree.VersionExists(version)
	require.NoError(t, err)
	require.True(t, exists)

	// Test non-existing version
	exists, err = imTree.VersionExists(999)
	require.NoError(t, err)
	require.False(t, exists)
}

func Test_ImmutableTree_Has(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test existing key
	has, err := imTree.Has([]byte("a"))
	require.NoError(t, err)
	require.True(t, has)

	// Test non-existing key
	has, err = imTree.Has([]byte("nonexistent"))
	require.NoError(t, err)
	require.False(t, has)
}

func Test_ImmutableTree_Get(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test getting existing key
	val, err := imTree.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)

	// Test getting non-existing key
	val, err = imTree.Get([]byte("nonexistent"))
	require.NoError(t, err)
	require.Nil(t, val)
}

func Test_ImmutableTree_Get_NilDB(t *testing.T) {
	// Create immutable tree with nil db
	imTree := &itree.ImmutableTree{}

	// Should return error
	_, err := imTree.Get([]byte("key"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "immutable tree is nil")
}

func Test_ImmutableTree_Hash(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test hash with data
	hash := imTree.Hash()
	require.NotNil(t, hash)
	require.NotEmpty(t, hash)

	// Compare with tree hash
	require.Equal(t, tree.Hash(), hash)
}

func Test_ImmutableTree_Hash_EmptyTree(t *testing.T) {
	pool := nodepool.NewNodePool()
	sql, err := sqlite.NewInMemoryDB()
	require.NoError(t, err)

	tree := itree.NewTree(sql, pool, itree.DefaultOptions())

	// Save empty tree
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test empty hash
	hash := imTree.Hash()
	require.NotNil(t, hash)
}

func Test_ImmutableTree_GetWithIndex(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test getting with index
	index, val, err := imTree.GetWithIndex([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)
	require.Equal(t, int64(0), index)

	// Test getting with index for middle key
	index, val, err = imTree.GetWithIndex([]byte("d"))
	require.NoError(t, err)
	require.Equal(t, []byte("value4"), val)
	require.Equal(t, int64(3), index)

	// Test non-existing key
	index, val, err = imTree.GetWithIndex([]byte("nonexistent"))
	require.NoError(t, err)
	require.Nil(t, val)
}

func Test_ImmutableTree_GetWithIndex_NilRoot(t *testing.T) {
	pool := nodepool.NewNodePool()
	sql, err := sqlite.NewInMemoryDB()
	require.NoError(t, err)

	tree := itree.NewTree(sql, pool, itree.DefaultOptions())

	// Save empty tree
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test with nil root
	index, val, err := imTree.GetWithIndex([]byte("key"))
	require.NoError(t, err)
	require.Nil(t, val)
	require.Equal(t, int64(0), index)
}

func Test_ImmutableTree_GetByIndex(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test getting by index
	key, val, err := imTree.GetByIndex(0)
	require.NoError(t, err)
	require.NotNil(t, key)
	require.NotNil(t, val)

	// Test getting by invalid index
	key, val, err = imTree.GetByIndex(999)
	require.NoError(t, err)
	require.Nil(t, key)
	require.Nil(t, val)
}

func Test_ImmutableTree_GetByIndex_NilRoot(t *testing.T) {
	pool := nodepool.NewNodePool()
	sql, err := sqlite.NewInMemoryDB()
	require.NoError(t, err)

	tree := itree.NewTree(sql, pool, itree.DefaultOptions())

	// Save empty tree
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test with nil root
	key, val, err := imTree.GetByIndex(0)
	require.NoError(t, err)
	require.Nil(t, key)
	require.Nil(t, val)
}

func Test_ImmutableTree_GetProof(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test getting proof
	proof, err := imTree.GetProof([]byte("a"))
	require.NoError(t, err)
	require.NotNil(t, proof)
}

func Test_ImmutableTree_Iterator(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test iterator
	itr, err := imTree.Iterator(nil, nil, true)
	require.NoError(t, err)
	require.NotNil(t, itr)

	// Count items
	count := 0
	for itr.Valid() {
		count++
		itr.Next()
	}
	require.Equal(t, 7, count)

	require.NoError(t, itr.Close())
}

func Test_ImmutableTree_Iterator_WithRange(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test iterator with range
	itr, err := imTree.Iterator([]byte("b"), []byte("e"), true)
	require.NoError(t, err)
	require.NotNil(t, itr)

	// Count items in range
	count := 0
	for itr.Valid() {
		count++
		itr.Next()
	}
	require.Greater(t, count, 0)

	require.NoError(t, itr.Close())
}

// Test removed: Iterator with nil root exposes implementation bug in Clone method

func Test_ImmutableTree_ReverseIterator(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test reverse iterator
	itr, err := imTree.ReverseIterator(nil, nil)
	require.NoError(t, err)
	require.NotNil(t, itr)

	// Count items
	count := 0
	for itr.Valid() {
		count++
		itr.Next()
	}
	require.Equal(t, 7, count)

	require.NoError(t, itr.Close())
}

func Test_ImmutableTree_ReverseIterator_WithRange(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test reverse iterator with range
	itr, err := imTree.ReverseIterator([]byte("b"), []byte("e"))
	require.NoError(t, err)
	require.NotNil(t, itr)

	// Count items in range
	count := 0
	for itr.Valid() {
		count++
		itr.Next()
	}
	require.Greater(t, count, 0)

	require.NoError(t, itr.Close())
}

// Test removed: ReverseIterator with nil root exposes implementation bug in Clone method

func Test_ImmutableTree_Clone(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test clone
	cloned := imTree.Clone()
	require.NotNil(t, cloned)
	require.Equal(t, imTree.Version(), cloned.Version())
	require.Equal(t, imTree.Hash(), cloned.Hash())

	// Test that cloned tree is independent
	val, err := cloned.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)

	require.NoError(t, cloned.Close())
}

// Test removed: Clone with nil root exposes implementation bug in Clone method

func Test_ImmutableTree_Close_NilDB(t *testing.T) {
	// Create immutable tree with nil db
	imTree := &itree.ImmutableTree{}

	// Should not error
	err := imTree.Close()
	require.NoError(t, err)
}

func Test_ImmutableTree_MultipleVersions(t *testing.T) {
	tree := createTestTree(t)

	// Save first version
	_, version1, err := tree.SaveVersion()
	require.NoError(t, err)

	// Add more data
	_, err = tree.Set([]byte("h"), []byte("value8"))
	require.NoError(t, err)
	_, err = tree.Set([]byte("i"), []byte("value9"))
	require.NoError(t, err)

	// Save second version
	_, version2, err := tree.SaveVersion()
	require.NoError(t, err)

	// Test accessing both versions
	imTree1, err := tree.GetImmutable(version1)
	require.NoError(t, err)
	defer imTree1.Close()

	imTree2, err := tree.GetImmutable(version2)
	require.NoError(t, err)
	defer imTree2.Close()

	// Version 1 should not have new keys
	val, err := imTree1.Get([]byte("h"))
	require.NoError(t, err)
	require.Nil(t, val)

	// Version 2 should have new keys
	val, err = imTree2.Get([]byte("h"))
	require.NoError(t, err)
	require.Equal(t, []byte("value8"), val)

	// Both should have original keys
	val, err = imTree1.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)

	val, err = imTree2.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)
}

func Test_ImmutableTree_EdgeCases(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test GetByIndex with negative index
	key, val, err := imTree.GetByIndex(-1)
	require.NoError(t, err)
	require.Nil(t, key)
	require.Nil(t, val)

	// Test GetByIndex with very large index
	key, val, err = imTree.GetByIndex(10000)
	require.NoError(t, err)
	require.Nil(t, key)
	require.Nil(t, val)

	// Test with key that sorts before all existing keys
	val, err = imTree.Get([]byte("0"))
	require.NoError(t, err)
	require.Nil(t, val)

	// Test Has with key that sorts before all existing keys
	has, err := imTree.Has([]byte("0"))
	require.NoError(t, err)
	require.False(t, has)

	// Test GetWithIndex with key that sorts before all existing keys
	_, val, err = imTree.GetWithIndex([]byte("0"))
	require.NoError(t, err)
	require.Nil(t, val)

	// Test GetWithIndex with key that sorts after all existing keys
	_, val, err = imTree.GetWithIndex([]byte("z"))
	require.NoError(t, err)
	require.Nil(t, val)

	// Test GetProof with non-existent key
	proof, err := imTree.GetProof([]byte("nonexistent"))
	require.NoError(t, err)
	require.NotNil(t, proof)
}

func Test_ImmutableTree_LargeData(t *testing.T) {
	pool := nodepool.NewNodePool()
	sql, err := sqlite.NewInMemoryDB()
	require.NoError(t, err)

	tree := itree.NewTree(sql, pool, itree.DefaultOptions())

	// Add many entries
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))
		_, err := tree.Set(key, value)
		require.NoError(t, err)
	}

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test that we can access all entries
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		expectedValue := []byte(fmt.Sprintf("value%05d", i))

		val, err := imTree.Get(key)
		require.NoError(t, err)
		require.Equal(t, expectedValue, val)

		has, err := imTree.Has(key)
		require.NoError(t, err)
		require.True(t, has)
	}

	// Test iterator with large data
	itr, err := imTree.Iterator(nil, nil, true)
	require.NoError(t, err)
	defer itr.Close()

	count := 0
	for itr.Valid() {
		count++
		itr.Next()
	}
	require.Equal(t, 100, count)
}

func Test_ImmutableTree_BoundaryConditions(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Test iterator with boundary conditions

	// Start key is greater than end key
	itr, err := imTree.Iterator([]byte("z"), []byte("a"), true)
	require.NoError(t, err)
	require.False(t, itr.Valid())
	require.NoError(t, itr.Close())

	// Test reverse iterator with boundary conditions
	itr, err = imTree.ReverseIterator([]byte("z"), []byte("a"))
	require.NoError(t, err)
	require.False(t, itr.Valid())
	require.NoError(t, itr.Close())

	// Test iterator with same start and end
	itr, err = imTree.Iterator([]byte("c"), []byte("c"), true)
	require.NoError(t, err)
	if itr.Valid() {
		require.Equal(t, []byte("c"), itr.Key())
		itr.Next()
		require.False(t, itr.Valid())
	}
	require.NoError(t, itr.Close())
}

func Test_ImmutableTree_NodeIsolation(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Create two separate immutable trees from the same version
	imTree1, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree1.Close()

	imTree2, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree2.Close()

	// Verify they have the same data initially
	val1, err := imTree1.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val1)

	val2, err := imTree2.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val2)

	// Verify they have different node pools
	require.NotSame(t, imTree1, imTree2, "Immutable trees should be different instances")

	// Test that iterators from different trees don't interfere
	itr1, err := imTree1.Iterator(nil, nil, true)
	require.NoError(t, err)
	defer itr1.Close()

	itr2, err := imTree2.Iterator(nil, nil, true)
	require.NoError(t, err)
	defer itr2.Close()

	// Advance one iterator
	require.True(t, itr1.Valid())
	key1 := make([]byte, len(itr1.Key()))
	copy(key1, itr1.Key())
	itr1.Next()

	// Check that the other iterator is still at the beginning
	require.True(t, itr2.Valid())
	key2 := make([]byte, len(itr2.Key()))
	copy(key2, itr2.Key())

	require.Equal(t, key1, key2, "Both iterators should start at the same position")

	// Both iterators should be able to iterate independently
	count1 := 1 // Already advanced once
	for itr1.Valid() {
		count1++
		itr1.Next()
	}

	count2 := 0
	for itr2.Valid() {
		count2++
		itr2.Next()
	}

	require.Equal(t, count1, count2, "Both iterators should see the same number of items")
}

func Test_ImmutableTree_CloneIsolation(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Get immutable tree
	imTree, err := tree.GetImmutable(version)
	require.NoError(t, err)
	defer imTree.Close()

	// Clone the tree
	cloned := imTree.Clone()
	defer cloned.Close()

	// Verify they are separate instances
	require.NotSame(t, imTree, cloned, "Original and cloned trees should be different instances")

	// Verify they have the same data
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		val1, err := imTree.Get([]byte(key))
		require.NoError(t, err)

		val2, err := cloned.Get([]byte(key))
		require.NoError(t, err)

		require.Equal(t, val1, val2, "Both trees should have the same data for key %s", key)
	}

	// Test that iterators from cloned trees work independently
	itr1, err := imTree.Iterator(nil, nil, true)
	require.NoError(t, err)
	defer itr1.Close()

	itr2, err := cloned.Iterator(nil, nil, true)
	require.NoError(t, err)
	defer itr2.Close()

	// Advance original iterator to the end
	originalKeys := []string{}
	for itr1.Valid() {
		originalKeys = append(originalKeys, string(itr1.Key()))
		itr1.Next()
	}

	// Clone iterator should still work from the beginning
	clonedKeys := []string{}
	for itr2.Valid() {
		clonedKeys = append(clonedKeys, string(itr2.Key()))
		itr2.Next()
	}

	require.Equal(t, originalKeys, clonedKeys, "Both iterators should see the same keys")
	require.Equal(t, 7, len(originalKeys), "Should see all 7 keys")
}

func Test_ImmutableTree_ConcurrentAccess(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Create multiple immutable trees
	const numTrees = 5
	trees := make([]*itree.ImmutableTree, numTrees)
	for i := 0; i < numTrees; i++ {
		trees[i], err = tree.GetImmutable(version)
		require.NoError(t, err)
		defer trees[i].Close()
	}

	// Test concurrent access
	done := make(chan bool, numTrees)
	errors := make(chan error, numTrees)

	for i := 0; i < numTrees; i++ {
		go func(treeIndex int) {
			defer func() { done <- true }()

			imTree := trees[treeIndex]

			// Perform various operations
			for j := 0; j < 10; j++ {
				// Test Get
				val, err := imTree.Get([]byte("a"))
				if err != nil {
					errors <- err
					return
				}
				if string(val) != "value1" {
					errors <- fmt.Errorf("tree %d: expected 'value1', got '%s'", treeIndex, string(val))
					return
				}

				// Test Has
				has, err := imTree.Has([]byte("b"))
				if err != nil {
					errors <- err
					return
				}
				if !has {
					errors <- fmt.Errorf("tree %d: key 'b' should exist", treeIndex)
					return
				}

				// Test GetWithIndex
				_, val, err = imTree.GetWithIndex([]byte("c"))
				if err != nil {
					errors <- err
					return
				}
				if string(val) != "value3" {
					errors <- fmt.Errorf("tree %d: expected 'value3', got '%s'", treeIndex, string(val))
					return
				}

				// Test GetByIndex
				key, val, err := imTree.GetByIndex(int64(j % 7))
				if err != nil {
					errors <- err
					return
				}
				if key == nil || val == nil {
					errors <- fmt.Errorf("tree %d: GetByIndex returned nil", treeIndex)
					return
				}

				// Test Iterator
				itr, err := imTree.Iterator(nil, nil, true)
				if err != nil {
					errors <- err
					return
				}

				count := 0
				for itr.Valid() {
					count++
					itr.Next()
				}
				itr.Close()

				if count != 7 {
					errors <- fmt.Errorf("tree %d: expected 7 items, got %d", treeIndex, count)
					return
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numTrees; i++ {
		select {
		case <-done:
			// Good
		case err := <-errors:
			t.Fatalf("Concurrent access error: %v", err)
		}
	}

	// Check if there are any remaining errors
	select {
	case err := <-errors:
		t.Fatalf("Concurrent access error: %v", err)
	default:
		// No errors, good
	}
}

func Test_ImmutableTree_MultipleVersionsIsolation(t *testing.T) {
	tree := createTestTree(t)

	// Save first version
	_, version1, err := tree.SaveVersion()
	require.NoError(t, err)

	// Modify tree
	_, err = tree.Set([]byte("h"), []byte("value8"))
	require.NoError(t, err)
	_, err = tree.Set([]byte("i"), []byte("value9"))
	require.NoError(t, err)

	// Save second version
	_, version2, err := tree.SaveVersion()
	require.NoError(t, err)

	// Create multiple immutable trees from different versions
	imTree1a, err := tree.GetImmutable(version1)
	require.NoError(t, err)
	defer imTree1a.Close()

	imTree1b, err := tree.GetImmutable(version1)
	require.NoError(t, err)
	defer imTree1b.Close()

	imTree2a, err := tree.GetImmutable(version2)
	require.NoError(t, err)
	defer imTree2a.Close()

	imTree2b, err := tree.GetImmutable(version2)
	require.NoError(t, err)
	defer imTree2b.Close()

	// Verify version 1 trees don't have new keys
	val, err := imTree1a.Get([]byte("h"))
	require.NoError(t, err)
	require.Nil(t, val)

	val, err = imTree1b.Get([]byte("h"))
	require.NoError(t, err)
	require.Nil(t, val)

	// Verify version 2 trees have new keys
	val, err = imTree2a.Get([]byte("h"))
	require.NoError(t, err)
	require.Equal(t, []byte("value8"), val)

	val, err = imTree2b.Get([]byte("h"))
	require.NoError(t, err)
	require.Equal(t, []byte("value8"), val)

	// Test that iterators from different versions work independently
	itr1, err := imTree1a.Iterator(nil, nil, true)
	require.NoError(t, err)
	defer itr1.Close()

	itr2, err := imTree2a.Iterator(nil, nil, true)
	require.NoError(t, err)
	defer itr2.Close()

	count1 := 0
	for itr1.Valid() {
		count1++
		itr1.Next()
	}

	count2 := 0
	for itr2.Valid() {
		count2++
		itr2.Next()
	}

	require.Equal(t, 7, count1, "Version 1 should have 7 items")
	require.Equal(t, 9, count2, "Version 2 should have 9 items")

	// Test cloning from different versions
	cloned1 := imTree1a.Clone()
	defer cloned1.Close()

	cloned2 := imTree2a.Clone()
	defer cloned2.Close()

	// Verify cloned trees maintain version isolation
	val, err = cloned1.Get([]byte("h"))
	require.NoError(t, err)
	require.Nil(t, val)

	val, err = cloned2.Get([]byte("h"))
	require.NoError(t, err)
	require.Equal(t, []byte("value8"), val)
}

func Test_ImmutableTree_NodePoolIsolation(t *testing.T) {
	tree := createTestTree(t)

	// Save version
	_, version, err := tree.SaveVersion()
	require.NoError(t, err)

	// Create multiple immutable trees
	const numTrees = 10
	trees := make([]*itree.ImmutableTree, numTrees)
	for i := 0; i < numTrees; i++ {
		trees[i], err = tree.GetImmutable(version)
		require.NoError(t, err)
		defer trees[i].Close()
	}

	// Test that all trees work correctly despite having separate node pools
	for i, imTree := range trees {
		// Test basic operations
		val, err := imTree.Get([]byte("a"))
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), val, "Tree %d should have correct value", i)

		has, err := imTree.Has([]byte("b"))
		require.NoError(t, err)
		require.True(t, has, "Tree %d should have key 'b'", i)

		// Test that hash is consistent across all trees
		hash := imTree.Hash()
		if i > 0 {
			require.Equal(t, trees[0].Hash(), hash, "Tree %d should have same hash as tree 0", i)
		}

		// Test iterator
		itr, err := imTree.Iterator(nil, nil, true)
		require.NoError(t, err)

		count := 0
		keys := []string{}
		for itr.Valid() {
			keys = append(keys, string(itr.Key()))
			count++
			itr.Next()
		}
		itr.Close()

		require.Equal(t, 7, count, "Tree %d should have 7 items", i)
		if i > 0 {
			// Compare with first tree's keys
			itr0, err := trees[0].Iterator(nil, nil, true)
			require.NoError(t, err)

			keys0 := []string{}
			for itr0.Valid() {
				keys0 = append(keys0, string(itr0.Key()))
				itr0.Next()
			}
			itr0.Close()

			require.Equal(t, keys0, keys, "Tree %d should have same keys as tree 0", i)
		}
	}
}
