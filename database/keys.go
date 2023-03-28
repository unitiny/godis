package database

import (
	"Go-Redis/interface/resp"
	"Go-Redis/lib/utils"
	"Go-Redis/lib/wildcard"
	"Go-Redis/resp/reply"
)

// DEL k1 k2 ...
func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	deleted := db.Removes(keys...)
	db.addAof(utils.ToCmdLine2("del", args))
	return reply.MakeIntReply(int64(deleted))
}

// EXISTS k1 k2 ...
func execExists(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		_, exists := db.GetEntity(string(arg))
		if exists {
			result++
		}
	}
	return reply.MakeIntReply(result)
}

// FLUSHDB
func execFlushDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLine2("flushdb", args))
	return reply.MakeOkReply()
}

// TYPE k1
func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)

	if !exists {
		return reply.MakeStatusReply("none") // :none\r\n
	}

	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string")
	}
	// TODO
	return &reply.UnknownErrReply{}
}

// RENAME k1 k2
func execRename(db *DB, args [][]byte) resp.Reply {
	key1 := string(args[0])
	key2 := string(args[1])

	entity, exists := db.GetEntity(key1)
	if !exists {
		return reply.MakeIntReply(0)
	}

	db.PutEntity(key2, entity)
	db.Remove(key1)
	db.addAof(utils.ToCmdLine2("rename", args))
	return reply.MakeIntReply(1)
}

// RENAMENX k1 k2
func execRenamenx(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dest := string(args[1])

	_, exist := db.GetEntity(dest)
	if exist {
		return reply.MakeIntReply(0)
	}

	entity, exists := db.GetEntity(src)
	if !exists {
		return reply.MakeIntReply(0)
	}

	db.PutEntity(dest, entity)
	db.Removes(src)
	db.addAof(utils.ToCmdLine2("renamenx", args))
	return reply.MakeIntReply(1)
}

// KEYS *
func execKeys(db *DB, args [][]byte) resp.Reply {
	pattern, err := wildcard.CompilePattern(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("Err pattern " + string(args[0]))
	}

	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		match := pattern.IsMatch(key)
		if match {
			result = append(result, []byte(key))
		}
		return true
	})

	return reply.MakeMultiBulkReply(result)
}

func init() {
	RegisterCommand("del", execDel, -2)
	RegisterCommand("exists", execExists, -2)
	RegisterCommand("flushDb", execFlushDB, -1)
	RegisterCommand("type", execType, 2)
	RegisterCommand("rename", execRename, 3)
	RegisterCommand("renamenx", execRenamenx, 3)
	RegisterCommand("keys", execKeys, 2)
}
