package tcp

import (
	"Go-Redis/lib/logger"
	"Go-Redis/lib/sync/atomic"
	"Go-Redis/lib/sync/wait"
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"time"
)

// EchoClient 单个客户端
type EchoClient struct {
	conn    net.Conn
	waiting wait.Wait
}

func (e *EchoClient) Close() error {
	e.waiting.WaitWithTimeout(10 * time.Second)
	_ = e.conn.Close()
	return nil
}

type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolen
}

func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}

func (e *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if e.closing.Get() {
		_ = conn.Close()
	}

	echoClient := &EchoClient{
		conn: conn,
	}

	e.activeConn.Store(echoClient, struct{}{})
	reader := bufio.NewReader(conn)
	for true {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connecting shutdown")
			} else {
				logger.Error(err)
			}
			return
		}

		echoClient.waiting.Add(1)
		b := []byte(msg)
		_, _ = conn.Write(b)
		echoClient.waiting.Done()
	}
}

func (e *EchoHandler) Close() error {
	e.closing.Set(true)
	logger.Info("EchoHandler shutting")
	e.activeConn.Range(func(key, value any) bool {
		client := key.(*EchoClient)
		_ = client.conn.Close()
		return true
	})
	return nil
}
