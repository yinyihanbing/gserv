package network

import (
	"net"
	"sync"
	"time"

	"github.com/yinyihanbing/gutils/logs"
)

// TCPServer represents a TCP server configuration and runtime state.
type TCPServer struct {
	// Address to listen on
	Addr string
	// Maximum number of concurrent connections
	MaxConnNum int
	// Maximum number of pending writes per connection
	PendingWriteNum int
	// Callback to create a new agent for each connection
	NewAgent func(*TCPConn) Agent
	// Listener for incoming connections
	ln net.Listener
	// Set of active connections
	conns ConnSet
	// Mutex to protect access to the connection set
	mutexConns sync.Mutex
	// WaitGroup for listener goroutine
	wgLn sync.WaitGroup
	// WaitGroup for connection handling goroutines
	wgConns sync.WaitGroup

	// Message parser configuration
	LenMsgLen    int
	MinMsgLen    uint32
	MaxMsgLen    uint32
	LittleEndian bool
	msgParser    *MsgParser
}

// Start initializes the server and starts accepting connections.
func (server *TCPServer) Start() {
	// Initialize server configuration and message parser
	server.init()
	// Start the server in a separate goroutine
	go server.run()
}

// init initializes the server configuration and message parser.
func (server *TCPServer) init() {
	// Start listening on the specified address
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		logs.Fatal("failed to start listener: %v", err)
	}

	// Validate and set default values for configuration
	if server.MaxConnNum <= 0 {
		server.MaxConnNum = 100
		logs.Info("invalid maxconnnum. resetting to default value: %v", server.MaxConnNum)
	}
	if server.PendingWriteNum <= 0 {
		server.PendingWriteNum = 100
		logs.Info("invalid pendingwritenum. resetting to default value: %v", server.PendingWriteNum)
	}
	if server.NewAgent == nil {
		logs.Fatal("newagent callback must not be nil. please provide a valid function.")
	}

	// Assign listener and initialize connection set
	server.ln = ln
	server.conns = make(ConnSet)

	// Initialize message parser
	msgParser := NewMsgParser()
	msgParser.SetMsgLen(server.LenMsgLen, server.MinMsgLen, server.MaxMsgLen)
	msgParser.SetByteOrder(server.LittleEndian)
	server.msgParser = msgParser
}

// run starts accepting connections and handles them.
func (server *TCPServer) run() {
	server.wgLn.Add(1)
	defer server.wgLn.Done()

	var tempDelay time.Duration
	for {
		// Accept a new connection
		conn, err := server.ln.Accept()
		if err != nil {
			// Handle temporary errors with exponential backoff
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				logs.Error("accept error: %v; retrying in %v", err, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			// Log and exit on non-recoverable errors
			logs.Error("accept failed: %v", err)
			return
		}
		tempDelay = 0

		// Check if the connection limit is reached
		server.mutexConns.Lock()
		if len(server.conns) >= server.MaxConnNum {
			server.mutexConns.Unlock()
			conn.Close()
			logs.Error("too many connections. conn num=%v, limit=%v", len(server.conns), server.MaxConnNum)
			continue
		}
		// Add the new connection to the connection set
		server.conns[conn] = struct{}{}
		server.mutexConns.Unlock()

		// Increment the connection WaitGroup
		server.wgConns.Add(1)

		// Create a new TCP connection and agent
		tcpConn := newTCPConn(conn, server.PendingWriteNum, server.msgParser)
		agent := server.NewAgent(tcpConn)
		go func() {
			// Run the agent
			agent.Run()

			// Cleanup after connection is closed
			tcpConn.Close()
			server.mutexConns.Lock()
			delete(server.conns, conn)
			server.mutexConns.Unlock()
			agent.OnClose()

			// Decrement the connection WaitGroup
			server.wgConns.Done()
		}()
	}
}

// Close gracefully shuts down the server and closes all active connections.
func (server *TCPServer) Close() {
	// Close the listener and wait for the listener goroutine to finish
	server.ln.Close()
	server.wgLn.Wait()

	// Close all active connections
	server.mutexConns.Lock()
	for conn := range server.conns {
		conn.Close()
	}
	server.conns = nil
	server.mutexConns.Unlock()

	// Wait for all connection handling goroutines to finish
	server.wgConns.Wait()
}
