package network

import (
	"net"
	"sync"
	"time"

	"github.com/yinyihanbing/gutils/logs"
)

// TCPClient represents a TCP client configuration and runtime state.
type TCPClient struct {
	sync.Mutex
	Addr            string
	ConnNum         int
	ConnectInterval time.Duration
	PendingWriteNum int
	AutoReconnect   bool
	NewAgent        func(*TCPConn) Agent
	conns           ConnSet
	wg              sync.WaitGroup
	closeFlag       bool

	// msg parser
	LenMsgLen    int
	MinMsgLen    uint32
	MaxMsgLen    uint32
	LittleEndian bool
	msgParser    *MsgParser
}

// Start initializes the client and starts the connection goroutines.
func (client *TCPClient) Start() {
	client.init()

	for i := 0; i < client.ConnNum; i++ {
		client.wg.Add(1)
		go client.connect()
	}
}

// init initializes the client configuration and message parser.
func (client *TCPClient) init() {
	client.Lock()
	defer client.Unlock()

	client.validateConfig()

	if client.conns != nil {
		logs.Fatal("tcpclient is already running. duplicate start() calls are not allowed.")
	}

	client.conns = make(ConnSet)
	client.closeFlag = false

	client.initMsgParser()
}

// validateConfig validates and adjusts the client configuration.
func (client *TCPClient) validateConfig() {
	if client.ConnNum <= 0 {
		client.ConnNum = 1
		logs.Info("invalid connnum. resetting to default value: %v", client.ConnNum)
	}
	if client.ConnectInterval <= 0 {
		client.ConnectInterval = 3 * time.Second
		logs.Info("invalid connectinterval. resetting to default value: %v", client.ConnectInterval)
	}
	if client.PendingWriteNum <= 0 {
		client.PendingWriteNum = 100
		logs.Info("invalid pendingwritenum. resetting to default value: %v", client.PendingWriteNum)
	}
	if client.NewAgent == nil {
		logs.Fatal("newagent callback must not be nil. please provide a valid function.")
	}
}

// initMsgParser initializes the message parser with the configured parameters.
func (client *TCPClient) initMsgParser() {
	msgParser := NewMsgParser()
	msgParser.SetMsgLen(client.LenMsgLen, client.MinMsgLen, client.MaxMsgLen)
	msgParser.SetByteOrder(client.LittleEndian)
	client.msgParser = msgParser
}

// dial attempts to establish a TCP connection to the configured address.
func (client *TCPClient) dial() net.Conn {
	for {
		conn, err := net.Dial("tcp", client.Addr)
		if err == nil || client.closeFlag {
			return conn
		}

		logs.Info("failed to connect to %v. error: %v. retrying in %v...", client.Addr, err, client.ConnectInterval)
		time.Sleep(client.ConnectInterval)
	}
}

// connect handles the connection lifecycle, including reconnection logic.
func (client *TCPClient) connect() {
	defer client.wg.Done()

	for {
		conn := client.dial()
		if conn == nil {
			return
		}

		if !client.handleConnection(conn) {
			return
		}

		if !client.AutoReconnect {
			break
		}
		time.Sleep(client.ConnectInterval)
	}
}

// handleConnection manages a single connection, including agent lifecycle and cleanup.
func (client *TCPClient) handleConnection(conn net.Conn) bool {
	client.Lock()
	if client.closeFlag {
		client.Unlock()
		conn.Close()
		return false
	}
	client.conns[conn] = struct{}{}
	client.Unlock()

	tcpConn := newTCPConn(conn, client.PendingWriteNum, client.msgParser)
	agent := client.NewAgent(tcpConn)
	agent.Run()

	// Cleanup after connection is closed
	tcpConn.Close()
	client.Lock()
	delete(client.conns, conn)
	client.Unlock()
	agent.OnClose()

	return true
}

// Close gracefully shuts down the client, closing all active connections.
func (client *TCPClient) Close() {
	client.Lock()
	client.closeFlag = true
	for conn := range client.conns {
		conn.Close()
	}
	client.conns = nil
	client.Unlock()

	client.wg.Wait()
}
