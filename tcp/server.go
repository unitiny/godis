package tcp

import (
	"Go-Redis/interface/tcp"
	"Go-Redis/lib/logger"
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Config struct {
	Address string
}

func ListenAndServeWithSignal(
	cfg *Config,
	handle tcp.Handler,
) error {
	closeChan := make(chan struct{})
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	go func() {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT:
			closeChan <- struct{}{}
		}
		return
	}()

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info("start listen")
	ListenAndServe(listener, handle, closeChan)
	return nil
}

func ListenAndServe(
	ln net.Listener,
	handle tcp.Handler,
	closeChan <-chan struct{},
) {
	go func() {
		<-closeChan
		logger.Info("shutting down")
		_ = ln.Close()
		_ = handle.Close()
		return
	}()

	defer func() {
		_ = ln.Close()
		_ = handle.Close()
	}()

	ctx := context.Background()
	wg := sync.WaitGroup{}
	for {
		accept, err := ln.Accept()
		if err != nil {
			logger.Error(err.Error())
			break
		}

		wg.Add(1)
		logger.Info("accepted link")
		go func() {
			defer func() {
				wg.Done()
			}()
			handle.Handle(ctx, accept)
		}()
	}
	wg.Wait()
}
