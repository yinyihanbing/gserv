package network

import (
	"errors"
	"net"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/yinyihanbing/gutils/logs"
)

type WebsocketConnSet map[*websocket.Conn]struct{}

// WSConn represents a websocket connection with additional control mechanisms.
type WSConn struct {
	sync.Mutex
	conn           *websocket.Conn
	writeChan      chan []byte
	maxMsgLen      uint32
	closeFlag      bool
	remoteOriginIP net.Addr
}

// newWSConn creates a new WSConn instance.
func newWSConn(conn *websocket.Conn, pendingWriteNum int, maxMsgLen uint32) *WSConn {
	wsConn := new(WSConn)
	wsConn.conn = conn
	wsConn.writeChan = make(chan []byte, pendingWriteNum)
	wsConn.maxMsgLen = maxMsgLen

	// Start a goroutine to handle write operations.
	go func() {
		for b := range wsConn.writeChan {
			if b == nil {
				break
			}

			err := conn.WriteMessage(websocket.BinaryMessage, b)
			if err != nil {
				break
			}
		}

		conn.Close()
		wsConn.Lock()
		wsConn.closeFlag = true
		wsConn.Unlock()
	}()

	return wsConn
}

// SetOriginIP sets the remote origin IP address.
func (wsConn *WSConn) SetOriginIP(ip net.Addr) {
	wsConn.remoteOriginIP = ip
}

// doDestroy forcibly closes the connection and cleans up resources.
func (wsConn *WSConn) doDestroy() {
	wsConn.conn.UnderlyingConn().(*net.TCPConn).SetLinger(0)
	wsConn.conn.Close()

	if !wsConn.closeFlag {
		close(wsConn.writeChan)
		wsConn.closeFlag = true
	}
}

// Destroy closes the connection and ensures cleanup.
func (wsConn *WSConn) Destroy() {
	wsConn.Lock()
	defer wsConn.Unlock()

	wsConn.doDestroy()
}

// Close gracefully closes the connection.
func (wsConn *WSConn) Close() {
	wsConn.Lock()
	defer wsConn.Unlock()
	if wsConn.closeFlag {
		return
	}

	wsConn.doWrite(nil)
	wsConn.closeFlag = true
}

// doWrite writes data to the write channel or destroys the connection if the channel is full.
func (wsConn *WSConn) doWrite(b []byte) {
	if len(wsConn.writeChan) == cap(wsConn.writeChan) {
		logs.Debug("close conn: channel full")
		wsConn.doDestroy()
		return
	}

	wsConn.writeChan <- b
}

// LocalAddr returns the local address of the connection.
func (wsConn *WSConn) LocalAddr() net.Addr {
	return wsConn.conn.LocalAddr()
}

// RemoteAddr returns the remote address of the connection, prioritizing the origin IP if set.
func (wsConn *WSConn) RemoteAddr() net.Addr {
	if wsConn.remoteOriginIP != nil {
		return wsConn.remoteOriginIP
	}
	return wsConn.conn.RemoteAddr()
}

// ReadMsg reads a message from the websocket connection.
// goroutine not safe
func (wsConn *WSConn) ReadMsg() ([]byte, error) {
	_, b, err := wsConn.conn.ReadMessage()
	return b, err
}

// WriteMsg writes a message to the websocket connection.
// args must not be modified by other goroutines
func (wsConn *WSConn) WriteMsg(args ...[]byte) error {
	wsConn.Lock()
	defer wsConn.Unlock()
	if wsConn.closeFlag {
		return nil
	}

	// calculate total message length
	var msgLen uint32
	for _, arg := range args {
		msgLen += uint32(len(arg))
	}

	// validate message length
	if msgLen > wsConn.maxMsgLen {
		return errors.New("message too long")
	} else if msgLen < 1 {
		return errors.New("message too short")
	}

	// write directly if there's only one argument
	if len(args) == 1 {
		wsConn.doWrite(args[0])
		return nil
	}

	// merge all arguments into a single message
	msg := make([]byte, msgLen)
	l := 0
	for _, arg := range args {
		copy(msg[l:], arg)
		l += len(arg)
	}

	wsConn.doWrite(msg)

	return nil
}
