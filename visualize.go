package iavl

import (
	"fmt"

	"github.com/emicklei/dot"

	nodetypes "github.com/cosmos/iavl/v2/types/node"
)

func writeDotGraph(root *nodetypes.Node, lastGraph *dot.Graph) *dot.Graph {
	graph := dot.NewGraph(dot.Directed)

	var traverse func(node *nodetypes.Node) dot.Node
	var i int
	traverse = func(node *nodetypes.Node) dot.Node {
		if node == nil {
			return dot.Node{}
		}
		i++
		nodeKey := fmt.Sprintf("%s-%d", node.Key(), node.SubTreeHeight())
		nodeLabel := fmt.Sprintf("%s - %d", string(node.Key()), node.SubTreeHeight())
		n := graph.Node(nodeKey).Label(nodeLabel)
		if _, found := lastGraph.FindNodeById(nodeKey); !found {
			n.Attr("color", "red")
		}
		if node.IsLeaf() {
			return n
		}
		leftNode := traverse(node.LeftNode())
		rightNode := traverse(node.RightNode())

		leftEdge := n.Edge(leftNode, "l")
		rightEdge := n.Edge(rightNode, "r")
		if edges := lastGraph.FindEdges(n, leftNode); len(edges) == 0 {
			leftEdge.Attr("color", "red")
		}
		if edges := lastGraph.FindEdges(n, rightNode); len(edges) == 0 {
			rightEdge.Attr("color", "red")
		}

		return n
	}

	traverse(root)
	return graph
}
