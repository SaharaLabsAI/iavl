package tree

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"runtime"
	"sort"
	"sync"

	"github.com/SaharaLabsAI/iavl/v2/common/pool"
	hashpool "github.com/SaharaLabsAI/iavl/v2/common/pool/hash"
	"github.com/SaharaLabsAI/iavl/v2/db"
	inode "github.com/SaharaLabsAI/iavl/v2/node"
)

func (tree *Tree) Hash() []byte {
	tree.rw.RLock()
	defer tree.rw.RUnlock()

	if tree.root == nil {
		return inode.EmptyHash
	}
	return tree.root.Hash()
}

func (tree *Tree) WorkingHash() []byte {
	tree.rw.Lock()
	defer tree.rw.Unlock()

	if tree.root == nil {
		return inode.EmptyHash
	}

	if tree.root.Hash() != nil {
		return tree.root.Hash()
	}

	// if err := tree.sql.closeHangingIterators(); err != nil {
	// 	panic(err)
	// }

	hash := tree.computeHash()

	return hash
}

// ComputeHash the node and its descendants recursively. This usually mutates all
// descendant nodes. Returns the tree root node hash.
// If the tree is empty (i.e. the node is nil), returns the hash of an empty input,
// to conform with RFC-6962.
func (tree *Tree) computeHash() []byte {
	if tree.root == nil {
		return sha256.New().Sum(nil)
	}

	currentVersion := tree.version.Load()
	if tree.hashedVersion == currentVersion && tree.root.Hash() != nil {
		if tree.modificationCount != 0 {
			panic("unexpected tree modification, hash root twice")
		}
		return tree.root.Hash()
	}

	tree.deepHashParallel(tree.root, 0)
	// tree.deepHash(tree.root, 0)

	tree.hashedVersion = currentVersion
	tree.modificationCount = 0
	return tree.root.Hash()
}

func (tree *Tree) deepHashParallel(node *inode.Node, depth int8) {
	if node == nil {
		return
	}

	// Only use sequential for extremely small trees (under 10 nodes)
	estimatedNodes, _ := tree.estimateCapacity(node)
	if estimatedNodes < 10 {
		tree.deepHash(node, depth)
		return
	}

	// Get pooled resources to reduce allocations
	allBranches := nodeSlicePool.Get().([]*inode.Node)
	allLeaves := nodeSlicePool.Get().([]*inode.Node)
	nodesToLoad := loadTaskPool.Get().([]nodeLoadTask)

	defer func() {
		// Return resources to pools
		allBranches = allBranches[:0]
		allLeaves = allLeaves[:0]
		nodesToLoad = nodesToLoad[:0]

		//nolint:staticcheck
		nodeSlicePool.Put(allBranches)
		//nolint:staticcheck
		nodeSlicePool.Put(allLeaves)
		//nolint:staticcheck
		loadTaskPool.Put(nodesToLoad)
	}()

	// Initialize with estimated capacity
	tree.dirtyNodes.Branches = make([]*inode.Node, 0, estimatedNodes)
	tree.dirtyNodes.Leaves = make([]*inode.Node, 0, estimatedNodes/2)

	type nodeWithDepth struct {
		node  *inode.Node
		depth int8
	}

	toProcess := []nodeWithDepth{{node: node, depth: depth}}
	nextTreeVersion := tree.version.Load() + 1

	// Phase 1: Collect nodes and identify what needs loading (match original logic)
	for len(toProcess) > 0 {
		current := toProcess[0]
		toProcess = toProcess[1:]

		if current.node == nil {
			continue
		}

		// Handle leaf nodes first
		if current.node.IsLeaf() {
			if current.node.Version() == nextTreeVersion {
				allLeaves = append(allLeaves, current.node)
			}
			continue
		}

		// Skip non-dirty or non-current version branch nodes
		if !current.node.Dirty() || current.node.Version() != nextTreeVersion {
			continue
		}

		// This is a dirty branch node that needs processing
		allBranches = append(allBranches, current.node)

		// Check if children need loading
		needsLeftLoad := current.node.LeftNode() == nil && !current.node.LeftNodeKey().IsEmpty()
		needsRightLoad := current.node.RightNode() == nil && !current.node.RightNodeKey().IsEmpty()

		if needsLeftLoad {
			nodesToLoad = append(nodesToLoad, nodeLoadTask{
				parentNode: current.node,
				isLeft:     true,
				nodeKey:    current.node.LeftNodeKey(),
			})
		}

		if needsRightLoad {
			nodesToLoad = append(nodesToLoad, nodeLoadTask{
				parentNode: current.node,
				isLeft:     false,
				nodeKey:    current.node.RightNodeKey(),
			})
		}

		// Add children to processing queue if they're already loaded
		if current.node.LeftNode() != nil {
			toProcess = append(toProcess, nodeWithDepth{node: current.node.LeftNode(), depth: current.depth + 1})
		}
		if current.node.RightNode() != nil {
			toProcess = append(toProcess, nodeWithDepth{node: current.node.RightNode(), depth: current.depth + 1})
		}
	}

	// Phase 2: Always attempt parallel loading if there are nodes to load
	if len(nodesToLoad) > 0 {
		// Use fewer connections for smaller workloads to reduce overhead
		var maxConnections int
		if len(nodesToLoad) < 50 {
			maxConnections = 2
		} else if len(nodesToLoad) < 200 {
			maxConnections = 3
		} else {
			maxConnections = 4
		}

		actualConnections := min(maxConnections, tree.optimizedWorkerCount(len(nodesToLoad)))
		actualConnections = max(1, actualConnections) // Ensure at least 1

		connPool := make([]db.ReadConn, 0, actualConnections)
		defer func() {
			for _, conn := range connPool {
				if err := conn.Release(); err != nil {
					panic(err)
				}
			}
		}()

		// Create connections - always try parallel, fallback on error
		parallelLoadSuccessful := false
		if actualConnections > 1 {
			for i := 0; i < actualConnections; i++ {
				conn, err := tree.db.GetConn()
				if err != nil {
					for _, conn := range connPool {
						if err := conn.Release(); err != nil {
							panic(err)
						}
					}
					connPool = connPool[:0]
					break
				}
				connPool = append(connPool, conn)
			}

			if len(connPool) > 1 {
				tree.loadNodesParallel(nodesToLoad, connPool)
				parallelLoadSuccessful = true
			}
		}

		// Fallback to sequential loading if parallel failed or not attempted
		if !parallelLoadSuccessful {
			tree.loadNodesSequentially(nodesToLoad)
		}
	}

	// Phase 3: Continue tree traversal for newly loaded nodes
	toProcess = []nodeWithDepth{{node: node, depth: depth}}
	seen := make(map[*inode.Node]bool, len(allBranches)+len(allLeaves))

	for len(toProcess) > 0 {
		current := toProcess[0]
		toProcess = toProcess[1:]

		if current.node == nil || seen[current.node] {
			continue
		}
		seen[current.node] = true

		if current.node.IsLeaf() {
			// Already handled in Phase 1
			continue
		}

		// Skip nodes that don't meet the processing criteria
		if !current.node.Dirty() || current.node.Version() != nextTreeVersion {
			continue
		}

		// Add children to process queue (they should be loaded now)
		if current.node.LeftNode() != nil {
			toProcess = append(toProcess, nodeWithDepth{
				node:  current.node.LeftNode(),
				depth: current.depth + 1,
			})
		}
		if current.node.RightNode() != nil {
			toProcess = append(toProcess, nodeWithDepth{
				node:  current.node.RightNode(),
				depth: current.depth + 1,
			})
		}
	}

	// Phase 4: Always parallel process leaf nodes (if any exist)
	if len(allLeaves) > 0 {
		leafWorkers := tree.optimizedWorkerCount(len(allLeaves))
		leafWorkers = max(1, min(leafWorkers, 6)) // Cap at 6 workers, min 1

		if leafWorkers > 1 && len(allLeaves) > 2 { // Much lower threshold
			leafChan := make(chan *inode.Node, min(len(allLeaves), 50))

			go func() {
				defer close(leafChan)
				for _, leaf := range allLeaves {
					leafChan <- leaf
				}
			}()

			var leafWg sync.WaitGroup
			for i := 0; i < leafWorkers; i++ {
				leafWg.Add(1)
				go func() {
					defer leafWg.Done()

					h := hashpool.Sha256Pool.Get().(hash.Hash)
					defer hashpool.Sha256Pool.Put(h)

					buf := pool.BufPool.Get().(*bytes.Buffer)
					defer pool.BufPool.Put(buf)

					for leaf := range leafChan {
						h.Reset()
						buf.Reset()
						leaf.HashWith(h, buf)
					}
				}()
			}
			leafWg.Wait()
		} else {
			// Sequential for very small workloads
			h := hashpool.Sha256Pool.Get().(hash.Hash)
			defer hashpool.Sha256Pool.Put(h)

			buf := pool.BufPool.Get().(*bytes.Buffer)
			defer pool.BufPool.Put(buf)

			for _, leaf := range allLeaves {
				h.Reset()
				buf.Reset()
				leaf.HashWith(h, buf)
			}
		}
	}

	// Phase 5: Always parallel process branch nodes by height (if any exist)
	if len(allBranches) > 0 {
		heightMap := make(map[int8][]*inode.Node)
		var heights []int8

		// Group by height
		for _, branch := range allBranches {
			h := branch.SubTreeHeight()
			if _, exists := heightMap[h]; !exists {
				heights = append(heights, h)
			}
			heightMap[h] = append(heightMap[h], branch)
		}

		// Sort heights (process from lowest to highest)
		sort.Slice(heights, func(i, j int) bool {
			return heights[i] < heights[j]
		})

		// Process by height with optimized parallelism
		for _, height := range heights {
			branches := heightMap[height]
			branchWorkers := tree.optimizedWorkerCount(len(branches))
			branchWorkers = max(1, min(branchWorkers, 4)) // Cap at 4 workers, min 1

			if branchWorkers > 1 && len(branches) > 1 { // Much lower threshold - parallel for 2+ nodes
				branchChan := make(chan *inode.Node, min(len(branches), 20))

				go func() {
					defer close(branchChan)
					for _, branch := range branches {
						branchChan <- branch
					}
				}()

				var branchWg sync.WaitGroup
				for i := 0; i < branchWorkers; i++ {
					branchWg.Add(1)
					go func() {
						defer branchWg.Done()

						h := hashpool.Sha256Pool.Get().(hash.Hash)
						defer hashpool.Sha256Pool.Put(h)

						buf := pool.BufPool.Get().(*bytes.Buffer)
						defer pool.BufPool.Put(buf)

						for branch := range branchChan {
							if branch.LeftNode() != nil && branch.LeftNode().Hash() == nil {
								h.Reset()
								buf.Reset()
								branch.LeftNode().HashWith(h, buf)
							}
							if branch.RightNode() != nil && branch.RightNode().Hash() == nil {
								h.Reset()
								buf.Reset()
								branch.RightNode().HashWith(h, buf)
							}

							h.Reset()
							buf.Reset()
							branch.HashWith(h, buf)
						}
					}()
				}
				branchWg.Wait()
			} else {
				h := hashpool.Sha256Pool.Get().(hash.Hash)
				defer hashpool.Sha256Pool.Put(h)

				buf := pool.BufPool.Get().(*bytes.Buffer)
				defer pool.BufPool.Put(buf)

				for _, branch := range branches {
					if branch.LeftNode() != nil && branch.LeftNode().Hash() == nil {
						h.Reset()
						buf.Reset()
						branch.LeftNode().HashWith(h, buf)
					}
					if branch.RightNode() != nil && branch.RightNode().Hash() == nil {
						h.Reset()
						buf.Reset()
						branch.RightNode().HashWith(h, buf)
					}

					h.Reset()
					buf.Reset()
					branch.HashWith(h, buf)
				}
			}
		}
	}

	// Copy results to tree's collections
	tree.dirtyNodes.Branches = make([]*inode.Node, len(allBranches))
	copy(tree.dirtyNodes.Branches, allBranches)
	tree.dirtyNodes.Leaves = make([]*inode.Node, len(allLeaves))
	copy(tree.dirtyNodes.Leaves, allLeaves)

	// Phase 6: Post-processing (same as original)
	nodesToEvict := make(map[*inode.Node]bool)
	nodesToReturn := make(map[*inode.Node]bool)

	for _, branch := range tree.dirtyNodes.Branches {
		if tree.heightFilter > 0 {
			leftNode := branch.LeftNode()
			rightNode := branch.RightNode()

			if leftNode != nil && leftNode.IsLeaf() {
				if !leftNode.Dirty() {
					nodesToReturn[leftNode] = true
				}
				branch.SetLeft(nil)
			}

			if rightNode != nil && rightNode.IsLeaf() {
				if !rightNode.Dirty() {
					nodesToReturn[rightNode] = true
				}
				branch.SetRight(nil)
			}
		}

		if branch.SubTreeHeight() < 2 {
			nodesToEvict[branch] = true
		}
	}

	for node := range nodesToEvict {
		node.EvictChildren()
	}

	for node := range nodesToReturn {
		tree.returnNode(node)
	}
}

// loadNodesParallel loads nodes in parallel using multiple database connections
func (tree *Tree) loadNodesParallel(tasks []nodeLoadTask, connPool []db.ReadConn) {
	if len(tasks) == 0 {
		return
	}

	type loadResult struct {
		task *nodeLoadTask
		node *inode.Node
		err  error
	}

	taskChan := make(chan *nodeLoadTask, len(tasks))
	resultChan := make(chan loadResult, len(tasks))

	// Send all tasks to channel
	for i := range tasks {
		taskChan <- &tasks[i]
	}
	close(taskChan)

	// Start worker goroutines
	var wg sync.WaitGroup
	maxWorkers := min(len(connPool), len(tasks))

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(connIdx int) {
			defer wg.Done()
			conn := connPool[connIdx]

			for task := range taskChan {
				// Load node from database using the dedicated connection
				loadedNode, err := conn.GetNode(tree.nodePool, task.nodeKey)

				resultChan <- loadResult{
					task: task,
					node: loadedNode,
					err:  err,
				}
			}
		}(i)
	}

	// Collect results
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Process results and update parent nodes
	for result := range resultChan {
		if result.err != nil {
			panic(fmt.Sprintf("Error loading node %s: %v", result.task.nodeKey, result.err))
		}

		if result.node == nil {
			panic(fmt.Sprintf("Node not found: %s", result.task.nodeKey))
		}

		// Update parent node with loaded child
		if result.task.isLeft {
			result.task.parentNode.SetLeft(result.node)
		} else {
			result.task.parentNode.SetRight(result.node)
		}
	}
}

// loadNodesSequentially is a fallback method for sequential loading
func (tree *Tree) loadNodesSequentially(tasks []nodeLoadTask) {
	for _, task := range tasks {
		var err error

		if task.isLeft {
			_, err = tree.getLeftNode(task.parentNode)
		} else {
			_, err = tree.getRightNode(task.parentNode)
		}

		if err != nil {
			panic(fmt.Sprintf("Error loading node sequentially: %v", err))
		}
	}
}

// Original deepHash function kept for reference
func (tree *Tree) deepHash(node *inode.Node, depth int8) {
	type nodeWithDepth struct {
		node    *inode.Node
		depth   int8
		visited bool // Flag to track if children have been visited
	}

	nextTreeVersion := tree.version.Load() + 1

	// Estimate stack capacity based on tree height to avoid reallocations
	// 2^height is roughly the maximum number of nodes
	estimatedCapacity := min(1<<(node.SubTreeHeight()+1), 1024)
	stack := make([]nodeWithDepth, 0, estimatedCapacity)
	stack = append(stack, nodeWithDepth{node: node, depth: depth, visited: false})

	// Pre-allocate slices for append operations to avoid reallocations
	tree.dirtyNodes.Branches = make([]*inode.Node, 0, estimatedCapacity/2)
	tree.dirtyNodes.Leaves = make([]*inode.Node, 0, estimatedCapacity/2)

	// Track nodes that should be evicted but not returned to pool yet
	nodesToEvict := make(map[*inode.Node]bool)
	// Track nodes that should be returned to pool after hash calculation
	nodesToReturn := make(map[*inode.Node]bool)

	// Process nodes in a depth-first manner using a stack
	for len(stack) > 0 {
		// Pop from stack instead of peeking - reduces slice operations
		lastIdx := len(stack) - 1
		current := stack[lastIdx]
		stack = stack[:lastIdx]

		if current.node == nil {
			panic(fmt.Sprintf("node is nil; sql.path=%s", tree.db.Path()))
		}

		// Check if this is a leaf node
		if current.node.IsLeaf() {
			// new leaves are written every version
			if current.node.Version() == nextTreeVersion {
				tree.dirtyNodes.AddLeaf(current.node)
			}
			continue // No further processing for leaf nodes
		}

		// Skip non-dirty or non-current version branch nodes
		if !current.node.Dirty() || current.node.Version() != nextTreeVersion {
			continue
		}

		// If it's the first visit, process children
		if !current.visited {
			// Clear cached hash
			current.node.SetHash(nil)

			// Push back the current node with visited flag set
			current.visited = true
			stack = append(stack, current)

			// Fetch both children at once to reduce function calls
			leftNode := tree.EnsureLeftNode(current.node)
			rightNode := tree.EnsureRightNode(current.node)

			// Push children to process first (post-order traversal)
			// Add right then left, so left is processed first due to LIFO stack
			stack = append(stack, nodeWithDepth{node: rightNode, depth: current.depth + 1, visited: false})
			stack = append(stack, nodeWithDepth{node: leftNode, depth: current.depth + 1, visited: false})
			continue
		}

		// Process the node after its children have been visited
		tree.dirtyNodes.AddBranch(current.node)
		current.node.HashSelf()

		// Apply height filter if enabled - combined conditional checks
		if tree.heightFilter > 0 {
			leftNode := current.node.LeftNode()
			rightNode := current.node.RightNode()

			if leftNode != nil && leftNode.IsLeaf() {
				if !leftNode.Dirty() {
					nodesToReturn[leftNode] = true
				}
				current.node.SetLeft(nil)
			}

			if rightNode != nil && rightNode.IsLeaf() {
				if !rightNode.Dirty() {
					nodesToReturn[rightNode] = true
				}
				current.node.SetRight(nil)
			}
		}

		// Apply eviction if at or beyond the eviction depth
		if current.node.SubTreeHeight() < 2 {
			nodesToEvict[current.node] = true
		}
	}

	for node := range nodesToEvict {
		node.EvictChildren()
	}

	for node := range nodesToReturn {
		tree.returnNode(node)
	}
}

// estimateCapacity uses subtree height for better memory pre-allocation
func (tree *Tree) estimateCapacity(rootNode *inode.Node) (int, int) {
	if rootNode == nil {
		return 64, 32
	}

	height := rootNode.SubTreeHeight()
	if height <= 0 {
		return 64, 32
	}

	// Estimate based on height, but cap at reasonable limits
	estimatedNodes := min(1<<height, 10000)
	estimatedLeaves := estimatedNodes / 2

	// Ensure minimums for small trees
	if estimatedNodes < 64 {
		estimatedNodes = 64
	}
	if estimatedLeaves < 32 {
		estimatedLeaves = 32
	}

	return estimatedNodes, estimatedLeaves
}

// optimizedWorkerCount dynamically sizes worker pool based on workload
func (tree *Tree) optimizedWorkerCount(workload int) int {
	cpuCount := runtime.NumCPU()

	if workload <= 1 {
		return 1
	} else if workload <= 5 {
		return min(2, cpuCount)
	} else if workload <= 20 {
		return min(3, cpuCount)
	} else if workload <= 100 {
		return min(4, cpuCount)
	} else if workload <= 1000 {
		return min(8, cpuCount)
	}
	return min(16, cpuCount*2) // For large workloads
}

type nodeLoadTask struct {
	parentNode *inode.Node
	isLeft     bool // true for left child, false for right child
	nodeKey    inode.NodeKey
}

var loadTaskPool = &sync.Pool{
	New: func() any {
		return make([]nodeLoadTask, 0, 64) // Pre-allocate with reasonable capacity
	},
}

var nodeSlicePool = &sync.Pool{
	New: func() any {
		return make([]*inode.Node, 0, 1024)
	},
}
