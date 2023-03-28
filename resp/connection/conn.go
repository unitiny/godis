package connection

import (
	"Go-Redis/lib/sync/wait"
	"context"
	"net"
	"sync"
	"time"
)

type Connection struct {
	conn     net.Conn
	waiting  wait.Wait
	mu       sync.Mutex
	selectDB int
}

func NewConnection(conn net.Conn) *Connection {
	return &Connection{conn: conn}
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Handle(ctx context.Context, conn net.Conn) {

}

func (c *Connection) Close() error {
	c.waiting.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

func (c *Connection) Write(bytes []byte) error {
	if len(bytes) == 0 {
		return nil
	}

	c.mu.Lock()
	c.waiting.Add(1)
	defer func() {
		c.waiting.Done()
		c.mu.Unlock()
	}()

	_, err := c.conn.Write(bytes)
	return err
}

func (c *Connection) GetDBIndex() int {
	return c.selectDB
}

func (c *Connection) SelectDB(dbNum int) {
	c.selectDB = dbNum
}
