package database

import (
	"Go-Redis/interface/database"
	"Go-Redis/interface/resp"
	"Go-Redis/lib/utils"
	"Go-Redis/resp/reply"
)

// GET k1
func execGet(db *DB, args [][]byte) resp.Reply {
	//logger.Info("execGet...", key)
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	return reply.MakeBulkReply(entity.Data.([]byte))
}

// SET k1 v1
func execSet(db *DB, args [][]byte) resp.Reply {
	//logger.Info("execSet...")
	key := string(args[0])
	value := args[1]

	entity := &database.DataEntity{
		Data: value,
	}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine2("set", args))
	return reply.MakeOkReply()
}

// SETNX k1 v1
func execSetnx(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity := &database.DataEntity{
		Data: args[1],
	}
	result := db.PutIfAbsent(key, entity)
	db.addAof(utils.ToCmdLine2("setnx", args))
	return reply.MakeIntReply(int64(result))
}

// GETSET k1 v1
func execGetSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity := &database.DataEntity{
		Data: args[1],
	}

	value, exists := db.GetEntity(key)
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine2("getset", args))
	if !exists {
		return reply.MakeNullBulkReply()
	}

	return reply.MakeBulkReply(value.Data.([]byte))
}

// STRLEN k1
func execStrLen(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	result := len(entity.Data.([]byte))
	return reply.MakeIntReply(int64(result))
}

func init() {
	RegisterCommand("get", execGet, 2)
	RegisterCommand("set", execSet, 3)
	RegisterCommand("setnx", execSetnx, 3)
	RegisterCommand("getset", execGetSet, 3)
	RegisterCommand("strlen", execStrLen, 2)
}
