package cluster

import (
	"Go-Redis/interface/resp"
	"Go-Redis/resp/reply"
)

func MakeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["type"] = defaultFunc
	routerMap["exists"] = defaultFunc
	routerMap["get"] = defaultFunc
	routerMap["set"] = defaultFunc
	routerMap["setnx"] = defaultFunc
	routerMap["getset"] = defaultFunc
	routerMap["ping"] = ping
	routerMap["rename"] = rename
	routerMap["renamenx"] = rename
	routerMap["flushdb"] = flushdb
	routerMap["del"] = del
	routerMap["select"] = execSelect

	return routerMap
}

func defaultFunc(cluster *ClusterDatabase, conn resp.Connection, args [][]byte) resp.Reply {
	key := string(args[1])
	peer := cluster.peerPick.PickNode(key)
	return cluster.relay(peer, conn, args)
}

func ping(cluster *ClusterDatabase, conn resp.Connection, args [][]byte) resp.Reply {
	return cluster.relay(cluster.self, conn, args)
}

func rename(cluster *ClusterDatabase, conn resp.Connection, args [][]byte) resp.Reply {
	if len(args) != 3 {
		return reply.MakeArgNumErrReply("rename")
	}

	src := string(args[1])
	dest := string(args[2])

	peerSrc := cluster.peerPick.PickNode(src)
	peerDest := cluster.peerPick.PickNode(dest)

	if peerSrc != peerDest {
		return reply.MakeErrReply("ERR rename must within on peer")
	}
	return cluster.relay(peerSrc, conn, args)
}

func flushdb(cluster *ClusterDatabase, conn resp.Connection, args [][]byte) resp.Reply {
	replies := cluster.broadcast(conn, args)
	var errReply reply.ErrorReply

	for _, r := range replies {
		if reply.IsErrorReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
	}

	if errReply == nil {
		return reply.MakeOkReply()
	}
	return reply.MakeErrReply("error: " + errReply.Error())
}

func del(cluster *ClusterDatabase, conn resp.Connection, args [][]byte) resp.Reply {
	replies := cluster.broadcast(conn, args)
	var errReply reply.ErrorReply
	deleted := int64(0)

	for _, r := range replies {
		if reply.IsErrorReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}

		intReply, ok := r.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error")
		}
		deleted += intReply.Code
	}

	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}
	return reply.MakeErrReply("error: " + errReply.Error())
}

func execSelect(cluster *ClusterDatabase, conn resp.Connection, args [][]byte) resp.Reply {
	return cluster.db.Exec(conn, args)
}
