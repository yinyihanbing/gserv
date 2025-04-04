package network

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yinyihanbing/gutils/logs"
)

// WSClient represents a WebSocket client with configurable options.
type WSClient struct {
	sync.Mutex
	Addr             string
	ConnNum          int
	ConnectInterval  time.Duration
	PendingWriteNum  int
	MaxMsgLen        uint32
	HandshakeTimeout time.Duration
	AutoReconnect    bool
	NewAgent         func(*WSConn) Agent
	dialer           websocket.Dialer
	conns            WebsocketConnSet
	wg               sync.WaitGroup
	closeFlag        bool
}

// Start initializes the client and starts the connection process.
func (client *WSClient) Start() {
	client.init()

	for range make([]struct{}, client.ConnNum) {
		client.wg.Add(1)
		go client.connect()
	}
}

// init validates and initializes the client configuration.
func (client *WSClient) init() {
	client.Lock()
	defer client.Unlock()

	if client.ConnNum <= 0 {
		client.ConnNum = 1
		logs.Info("invalid connnum, reset to %v", client.ConnNum)
	}
	if client.ConnectInterval <= 0 {
		client.ConnectInterval = 3 * time.Second
		logs.Info("invalid connectinterval, reset to %v", client.ConnectInterval)
	}
	if client.PendingWriteNum <= 0 {
		client.PendingWriteNum = 100
		logs.Info("invalid pendingwritenum, reset to %v", client.PendingWriteNum)
	}
	if client.MaxMsgLen <= 0 {
		client.MaxMsgLen = 4096
		logs.Info("invalid maxmsglen, reset to %v", client.MaxMsgLen)
	}
	if client.HandshakeTimeout <= 0 {
		client.HandshakeTimeout = 10 * time.Second
		logs.Info("invalid handshaketimeout, reset to %v", client.HandshakeTimeout)
	}
	if client.NewAgent == nil {
		logs.Fatal("newagent must not be nil")
	}
	if client.conns != nil {
		logs.Fatal("client is already running")
	}

	client.conns = make(WebsocketConnSet)
	client.closeFlag = false
	client.dialer = websocket.Dialer{
		HandshakeTimeout: client.HandshakeTimeout,
	}
}

// dial establishes a WebSocket connection to the server.
func (client *WSClient) dial() *websocket.Conn {
	for {
		conn, _, err := client.dialer.Dial(client.Addr, nil)
		if err == nil || client.closeFlag {
			return conn
		}
		logs.Info("failed to connect to %v: %v", client.Addr, err)
		time.Sleep(client.ConnectInterval)
		continue
	}
}

// connect handles the connection lifecycle and reconnection logic.
func (client *WSClient) connect() {
	defer client.wg.Done()

reconnect:
	conn := client.dial()
	if conn == nil {
		return
	}
	conn.SetReadLimit(int64(client.MaxMsgLen))

	client.Lock()
	if client.closeFlag {
		client.Unlock()
		conn.Close()
		return
	}
	client.conns[conn] = struct{}{}
	client.Unlock()

	wsConn := newWSConn(conn, client.PendingWriteNum, client.MaxMsgLen)
	agent := client.NewAgent(wsConn)
	agent.Run()

	// cleanup
	wsConn.Close()
	client.Lock()
	delete(client.conns, conn)
	client.Unlock()
	agent.OnClose()

	if client.AutoReconnect {
		time.Sleep(client.ConnectInterval)
		goto reconnect
	}
}

// Close gracefully shuts down the client and closes all connections.
func (client *WSClient) Close() {
	client.Lock()
	client.closeFlag = true
	for conn := range client.conns {
		conn.Close()
	}
	client.conns = nil
	client.Unlock()

	client.wg.Wait()
}
