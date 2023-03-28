package database

import (
	"Go-Redis/interface/resp"
)

var CmdLine [][]byte

type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply
	Close()
	AfterClientClose(r resp.Connection)
}

type DataEntity struct {
	Data interface{}
}
