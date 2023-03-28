package database

import (
	"Go-Redis/datastruct/dict"
	"Go-Redis/interface/database"
	"Go-Redis/interface/resp"
	"Go-Redis/resp/reply"
	"strings"
)

type ExecFunc func(db *DB, args [][]byte) resp.Reply

type CmdLine = [][]byte

type DB struct {
	index  int
	data   dict.Dict
	addAof func(line CmdLine)
}

func MakeDB() *DB {
	return &DB{
		data:   dict.MakeSyncDict(),
		addAof: func(line CmdLine) {},
	}
}

func (db *DB) Exec(c resp.Connection, cmdline CmdLine) resp.Reply {
	cmdName := strings.ToLower(string(cmdline[0]))
	cmd, ok := cmdTable[cmdName]

	if !ok {
		return reply.MakeErrReply("ERR unknown command " + cmdName)
	}

	if !validateArity(cmd.arity, cmdline) {
		return reply.MakeArgNumErrReply(cmdName)
	}

	fun := cmd.exector
	return fun(db, cmdline[1:])
}

// SET K V  arity = 3
// EXISTS k1 k2 ... arity = -2 代表至少得有2个参数
func validateArity(arity int, cmdArgs [][]byte) bool {
	argsLen := len(cmdArgs)
	if arity >= 0 {
		return arity == argsLen
	}
	return argsLen >= -arity
}

func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	val, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	return val.(*database.DataEntity), true
}

func (db *DB) PutEntity(key string, val *database.DataEntity) int {
	return db.data.Put(key, val)
}

func (db *DB) PutIfExists(key string, val *database.DataEntity) int {
	return db.data.PutIfExists(key, val)
}

func (db *DB) PutIfAbsent(key string, val *database.DataEntity) int {
	return db.data.PutIfAbsent(key, val)
}

func (db *DB) Remove(key string) int {
	return db.data.Remove(key)
}

func (db *DB) Removes(keys ...string) int {
	deleted := 0
	for _, key := range keys {
		deleted += db.data.Remove(key)
	}
	return deleted
}

func (db *DB) Flush() {
	db.data.Clear()
}
