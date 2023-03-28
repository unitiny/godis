package handler

import (
	"Go-Redis/cluster"
	"Go-Redis/config"
	"Go-Redis/database"
	databaseface "Go-Redis/interface/database"
	"Go-Redis/lib/logger"
	"Go-Redis/lib/sync/atomic"
	"Go-Redis/resp/connection"
	"Go-Redis/resp/parser"
	"Go-Redis/resp/reply"
	"context"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR\r\n")
)

type RespHandler struct {
	activeConn sync.Map
	db         databaseface.Database
	closing    atomic.Boolen
}

func MakeRespHandler() *RespHandler {
	var db databaseface.Database
	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 {
		db = cluster.NewClusterDatabase()
	} else {
		db = database.NewStandaloneDatabase()
	}

	return &RespHandler{
		db: db,
	}
}

func (r *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	r.db.AfterClientClose(client)
	r.activeConn.Delete(client)
}

func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if r.closing.Get() {
		_ = conn.Close()
	}
	client := connection.NewConnection(conn)
	r.activeConn.Store(client, struct{}{})
	ch := parser.ParseStream(conn)

	for payload := range ch {
		// 异常处理
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				r.closeClient(client)
				logger.Info("connection close: " + client.RemoteAddr().String())
				return
			}

			errReply := reply.MakeSyntaxErrReply()
			err := client.Write(errReply.ToBytes())
			if err != nil {
				r.closeClient(client)
				logger.Info("connection close: " + client.RemoteAddr().String())
			}
			continue
		}

		// exec处理
		if payload.Data == nil {
			continue
		}

		theReply, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Info("require multi bulk")
			continue
		}

		//logger.Info(fmt.Sprintf("Handle theReply %+v", theReply))
		result := r.db.Exec(client, theReply.Args)
		if result == nil {
			_ = client.Write(unknownErrReplyBytes)
		} else {
			_ = client.Write(result.ToBytes())
		}
	}
	//logger.Info("RespHandler Handle end...")
}

func (r *RespHandler) Close() error {
	logger.Info("handler shutting down")
	r.closing.Set(true)
	r.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	r.db.Close()
	return nil
}
