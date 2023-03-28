package aof

import (
	"Go-Redis/config"
	"Go-Redis/interface/database"
	"Go-Redis/lib/logger"
	"Go-Redis/lib/utils"
	"Go-Redis/resp/connection"
	"Go-Redis/resp/parser"
	"Go-Redis/resp/reply"
	"io"
	"os"
	"strconv"
)

const aofBufferSize = 1 << 16

type CmdLine = [][]byte

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

type AofHandler struct {
	database    database.Database
	aofChan     chan *payload
	aofFile     *os.File
	aofFileName string
	currentDB   int
}

func NewAofHandler(database database.Database) (*AofHandler, error) {
	aofHandler := &AofHandler{}
	aofHandler.database = database
	aofHandler.aofFileName = config.Properties.AppendFilename
	aofHandler.LoadAof() // 持久化存储

	aofFile, err := os.OpenFile(aofHandler.aofFileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	aofHandler.aofFile = aofFile

	aofHandler.aofChan = make(chan *payload, aofBufferSize)

	// handle
	go func() {
		aofHandler.handlerAof()
	}()
	return aofHandler, nil
}

func (handler *AofHandler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil {
		handler.aofChan <- &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

func (handler *AofHandler) handlerAof() {
	handler.currentDB = 0
	for p := range handler.aofChan {
		if handler.currentDB != p.dbIndex {
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Error(err)
			}
			handler.currentDB = p.dbIndex
			continue
		}

		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Error(err)
		}
	}
}

func (handler *AofHandler) LoadAof() {
	file, err := os.Open(handler.aofFileName)
	if err != nil {
		logger.Error(err)
		return
	}
	defer file.Close()

	fackConn := &connection.Connection{}
	ch := parser.ParseStream(file)
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error(p.Err)
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}

		rep, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("need multi bulk")
			continue
		}
		r := handler.database.Exec(fackConn, rep.Args)
		if reply.IsErrorReply(r) {
			logger.Error("exec err " + string(r.ToBytes()))
		}
	}
}
