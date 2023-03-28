package cluster

import (
	"Go-Redis/interface/resp"
	"Go-Redis/lib/utils"
	"Go-Redis/resp/client"
	"Go-Redis/resp/reply"
	"context"
	"errors"
	"strconv"
)

func (c *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	pool, ok := c.peerConnection[peer]
	if !ok {
		return nil, errors.New("pool not found")
	}

	cluster, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}

	result, ok := cluster.(*client.Client)
	if !ok {
		return nil, errors.New("wrong type")
	}
	return result, nil
}

func (c *ClusterDatabase) returnPeerClient(peer string, client *client.Client) error {
	pool, ok := c.peerConnection[peer]
	if !ok {
		return errors.New("pool not found")
	}

	return pool.ReturnObject(context.Background(), client)
}

func (c *ClusterDatabase) relay(peer string, conn resp.Connection, args [][]byte) resp.Reply {
	if peer == c.self {
		return c.db.Exec(conn, args)
	}

	peerClient, err := c.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = c.returnPeerClient(peer, peerClient)
	}()

	peerClient.Send(utils.ToCmdLine("select", strconv.Itoa(conn.GetDBIndex())))
	return peerClient.Send(args)
}

func (c *ClusterDatabase) broadcast(conn resp.Connection, args [][]byte) map[string]resp.Reply {
	results := make(map[string]resp.Reply)
	for _, node := range c.nodes {
		result := c.relay(node, conn, args)
		results[node] = result
	}
	return results
}
