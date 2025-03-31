package network

import (
	"net"
	"sync"

	"github.com/yinyihanbing/gutils/logs"
)

// ConnSet represents a set of connections.
type ConnSet map[net.Conn]struct{}

// TCPConn wraps a net.Conn with additional features like write buffering and message parsing.
type TCPConn struct {
	sync.Mutex
	conn      net.Conn
	writeChan chan []byte
	closeFlag bool
	msgParser *MsgParser
}

// newTCPConn creates a new TCPConn instance.
func newTCPConn(conn net.Conn, pendingWriteNum int, msgParser *MsgParser) *TCPConn {
	tcpConn := new(TCPConn)
	tcpConn.conn = conn
	tcpConn.writeChan = make(chan []byte, pendingWriteNum)
	tcpConn.msgParser = msgParser

	// goroutine to handle writing to the connection
	go func() {
		for b := range tcpConn.writeChan {
			if b == nil {
				break
			}

			_, err := conn.Write(b)
			if err != nil {
				logs.Debug("error writing to connection: ", err)
				break
			}
		}

		conn.Close()
		tcpConn.Lock()
		tcpConn.closeFlag = true
		tcpConn.Unlock()
	}()

	return tcpConn
}

// doDestroy forcibly closes the connection and cleans up resources.
func (tcpConn *TCPConn) doDestroy() {
	tcpConn.conn.(*net.TCPConn).SetLinger(0)
	tcpConn.conn.Close()

	if !tcpConn.closeFlag {
		close(tcpConn.writeChan)
		tcpConn.closeFlag = true
	}
}

// Destroy closes the connection and releases resources.
func (tcpConn *TCPConn) Destroy() {
	tcpConn.Lock()
	defer tcpConn.Unlock()

	tcpConn.doDestroy()
}

// Close signals the connection to close gracefully.
func (tcpConn *TCPConn) Close() {
	tcpConn.Lock()
	defer tcpConn.Unlock()
	if tcpConn.closeFlag {
		return
	}

	tcpConn.doWrite(nil) // signal to close
	tcpConn.closeFlag = true
}

// doWrite writes data to the write channel or destroys the connection if the channel is full.
func (tcpConn *TCPConn) doWrite(b []byte) {
	if len(tcpConn.writeChan) == cap(tcpConn.writeChan) {
		logs.Debug("close connection: write channel full")
		tcpConn.doDestroy()
		return
	}

	tcpConn.writeChan <- b
}

// Write sends data to the connection. The data must not be modified by other goroutines.
func (tcpConn *TCPConn) Write(b []byte) {
	tcpConn.Lock()
	defer tcpConn.Unlock()
	if tcpConn.closeFlag || b == nil {
		logs.Debug("write failed: connection closed or nil data")
		return
	}

	tcpConn.doWrite(b)
}

// Read reads data from the connection into the provided buffer.
func (tcpConn *TCPConn) Read(b []byte) (int, error) {
	return tcpConn.conn.Read(b)
}

// LocalAddr returns the local network address of the connection.
func (tcpConn *TCPConn) LocalAddr() net.Addr {
	return tcpConn.conn.LocalAddr()
}

// RemoteAddr returns the remote network address of the connection.
func (tcpConn *TCPConn) RemoteAddr() net.Addr {
	return tcpConn.conn.RemoteAddr()
}

// ReadMsg reads a complete message from the connection using the message parser.
func (tcpConn *TCPConn) ReadMsg() ([]byte, error) {
	return tcpConn.msgParser.Read(tcpConn)
}

// WriteMsg writes one or more messages to the connection using the message parser.
func (tcpConn *TCPConn) WriteMsg(args ...[]byte) error {
	return tcpConn.msgParser.Write(tcpConn, args...)
}
