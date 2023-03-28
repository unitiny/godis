package cluster

import (
	"Go-Redis/config"
	database2 "Go-Redis/database"
	"Go-Redis/interface/database"
	"Go-Redis/interface/resp"
	"Go-Redis/lib/consistanthash"
	"Go-Redis/lib/logger"
	"Go-Redis/resp/reply"
	"context"
	pool "github.com/jolestar/go-commons-pool"
	"strings"
)

type ClusterDatabase struct {
	self           string
	nodes          []string
	peerPick       *consistanthash.NodeMap
	peerConnection map[string]*pool.ObjectPool
	db             database.Database
}

type CmdFunc func(cluster *ClusterDatabase, conn resp.Connection, args [][]byte) resp.Reply

var router = MakeRouter()

func NewClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self:           config.Properties.Self,
		peerPick:       consistanthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
		db:             database2.NewStandaloneDatabase(),
	}

	// init nodes
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	nodes = append(nodes, config.Properties.Peers...)
	nodes = append(nodes, config.Properties.Self)
	cluster.nodes = nodes

	// addNodes
	cluster.peerPick.AddNode(nodes...)

	// init connection pool
	ctx := context.Background()
	for _, p := range config.Properties.Peers {
		cluster.peerConnection[p] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: p,
		})
	}
	return cluster
}

func (c *ClusterDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	var result resp.Reply
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			result = &reply.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(args[0]))
	execFunc, ok := router[cmdName]
	if !ok {
		logger.Error("ERR unRegister " + cmdName + " cmdFunc")
	}
	result = execFunc(c, client, args)
	return result
}

func (c *ClusterDatabase) Close() {
	c.db.Close()
}

func (c *ClusterDatabase) AfterClientClose(r resp.Connection) {
	c.db.AfterClientClose(r)
}
