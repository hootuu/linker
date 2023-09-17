package linker

import (
	"github.com/hootuu/domain/chain"
	"github.com/hootuu/domain/scope"
	"log/slog"
)

const HEAD int64 = 0
const HeadNodePre chain.Cid = "LING"

type Node struct {
	VN    chain.Cid  `bson:"vn" json:"vn"`
	Scope chain.Cid  `bson:"scope" json:"scope"`
	Type  chain.Type `bson:"t" json:"t"`
	Block int64      `bson:"block" json:"block"`
	Data  chain.Cid  `bson:"data" json:"data"`
	Pre   chain.Cid  `bson:"pre" json:"pre"`
}

func HeadNode(lead scope.Lead, dataCid chain.Cid) *Node {
	return &Node{
		VN:    lead.VN,
		Scope: lead.Scope,
		Type:  chain.Types.Link,
		Block: HEAD,
		Data:  dataCid,
		Pre:   HeadNodePre,
	}
}

func (n Node) GetType() chain.Type {
	return n.Type
}

func (n Node) GetVn() chain.Cid {
	return n.VN
}

func (n Node) GetScope() chain.Cid {
	return n.Scope
}

func (n Node) IsHead() bool {
	return n.Block == 0 && n.Pre == HeadNodePre
}

type NodePack struct {
	Cid  chain.Cid `bson:"cid" json:"cid"`
	Node *Node     `bson:"node" json:"node"`
}

func NewNodePack(node *Node) (*NodePack, error) {
	nodeCid, err := chain.GetStone().Inscribe(node)
	if err != nil {
		slog.Error("stone.Inscribe headNode error", err)
		return nil, err
	}
	return &NodePack{
		Cid:  nodeCid,
		Node: node,
	}, nil
}

func (pack NodePack) Next(dataCid chain.Cid) (*NodePack, error) {
	nxtNode := &Node{
		VN:    pack.Node.VN,
		Scope: pack.Node.Scope,
		Type:  chain.Types.Link,
		Block: pack.Node.Block + 1,
		Data:  dataCid,
		Pre:   pack.Cid,
	}
	return NewNodePack(nxtNode)
}
