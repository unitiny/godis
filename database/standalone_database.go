package database

import (
	"Go-Redis/aof"
	"Go-Redis/config"
	"Go-Redis/interface/resp"
	"Go-Redis/lib/logger"
	"Go-Redis/lib/utils"
	"Go-Redis/resp/reply"
	"strconv"
	"strings"
)

type StandaloneDatabase struct {
	setDB      []*DB
	aofHandler *aof.AofHandler
}

func NewStandaloneDatabase() *StandaloneDatabase {
	database := &StandaloneDatabase{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	database.setDB = make([]*DB, config.Properties.Databases)
	for i := range database.setDB {
		db := MakeDB()
		db.index = i
		database.setDB[i] = db
	}

	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAofHandler(database)
		if err != nil {
			panic(err)
		}
		database.aofHandler = aofHandler
		for _, db := range database.setDB {
			sdb := db
			sdb.addAof = func(line CmdLine) {
				aofHandler.AddAof(sdb.index, line)
			}
		}
	}
	return database
}

func (database *StandaloneDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()

	cmd := strings.ToLower(string(args[0]))
	if cmd == "select" {
		if len(args) != 2 {
			return reply.MakeArgNumErrReply(cmd)
		}
		return execSelect(client, database, args[1:])
	}

	//logger.Info("Exec cmd:" + cmd + " " + string(args[1]))

	dbIndex := client.GetDBIndex()
	db := database.setDB[dbIndex]
	return db.Exec(client, args)
}

func (database *StandaloneDatabase) Close() {

}

func (database *StandaloneDatabase) AfterClientClose(client resp.Connection) {

}

// SELECT k
func execSelect(client resp.Connection, database *StandaloneDatabase, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid selectDB index")
	}

	if dbIndex >= len(database.setDB) {
		return reply.MakeErrReply("ERR index out of range")
	}

	client.SelectDB(dbIndex)

	// 记录
	database.setDB[dbIndex].addAof(utils.ToCmdLine2("select", args))
	return reply.MakeOkReply()
}
