package parser

import (
	"Go-Redis/interface/resp"
	"Go-Redis/lib/logger"
	"Go-Redis/resp/reply"
	"bufio"
	"errors"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data resp.Reply
	Err  error
}

type readState struct {
	readingMultiline  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		// logger.Info("parse0 exit!!!")
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()

	var state readState
	var msg []byte
	var err error

	bufio := bufio.NewReader(reader)
	for true {
		var ioErr bool
		msg, ioErr, err = readLine(bufio, &state)
		if err != nil {
			if ioErr {
				state = readState{}
				close(ch)
				return
			}
			state = readState{}
			ch <- &Payload{
				Err: err,
			}
			continue
		}

		// 判断是不是多行解析模式
		if !state.readingMultiline { // 一开始解析和简单命令会走这里
			if msg[0] == '*' {
				err := parseMultiBulkHeader(msg, &state)
				if err != nil {
					state = readState{}
					ch <- &Payload{
						Err: errors.New("protocol error " + string(msg)),
					}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' {
				err := parseBulkHeader(msg, &state)
				if err != nil {
					state = readState{}
					ch <- &Payload{
						Err: err,
					}
					continue
				}

				if state.bulkLen == -1 {
					state = readState{}
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					continue
				}
			} else {
				result, err := parseSingleLineHeader(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}
		} else {
			err := readBody(msg, &state)
			if err != nil {
				state = readState{}
				ch <- &Payload{
					Err: errors.New("protocol error " + string(msg)),
				}
				continue
			}

			if state.finished() {
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}

				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error

	if state.bulkLen == 0 { // 按\r\n切分
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error " + string(msg))
		}
	} else {
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, false, err
		}
		if len(msg) == 0 || msg[len(msg)-1] != '\n' || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error " + string(msg))
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

// *3\r\n$3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedArgsCount int
	expectedArgsCount, err = strconv.Atoi(string(msg[1 : len(msg)-2]))
	if err != nil {
		return errors.New("protocol error " + string(msg))
	}

	if expectedArgsCount == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedArgsCount > 0 {
		state.msgType = msg[0]
		state.readingMultiline = true
		state.expectedArgsCount = expectedArgsCount
		state.args = make([][]byte, 0, expectedArgsCount)
		return nil
	} else {
		return errors.New("protocol error " + string(msg))
	}
}

// $4\r\nBULK\r\n
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.Atoi(string(msg[1 : len(msg)-2]))
	if err != nil {
		return errors.New("protocol error " + string(msg))
	}

	if state.bulkLen == -1 {
		state.expectedArgsCount = 0
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error " + string(msg))
	}
}

// +OK\r\n -err\r\n :5\r\n
func parseSingleLineHeader(msg []byte) (resp.Reply, error) {
	var result resp.Reply
	str := strings.TrimPrefix(string(msg), "\r\n")

	switch msg[0] {
	case '+':
		result = reply.MakeStatusReply(str[1:])
	case '-':
		result = reply.MakeSyntaxErrReply()
	case ':':
		val, err := strconv.ParseInt(string(str[1]), 10, 64)
		if err != nil {
			return nil, errors.New("protocol error " + string(msg))
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}

// $3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n
// PING\r\n
func readBody(msg []byte, state *readState) error {
	var err error
	line := msg[:len(msg)-2]

	if line[0] == '$' {
		state.bulkLen, err = strconv.Atoi(string(line[1:]))
		if err != nil {
			return errors.New("protocol error " + string(msg))
		}

		if state.bulkLen <= 0 {
			state.bulkLen = 0
			state.args = append(state.args, []byte{})
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
