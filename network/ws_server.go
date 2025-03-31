package network

import (
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yinyihanbing/gutils/logs"
)

// WSServer represents a WebSocket server configuration and runtime state.
type WSServer struct {
	Addr            string              // server address
	MaxConnNum      int                 // maximum number of connections
	PendingWriteNum int                 // pending write queue length per connection
	MaxMsgLen       uint32              // maximum message length
	HTTPTimeout     time.Duration       // HTTP handshake timeout
	CertFile        string              // TLS certificate file
	KeyFile         string              // TLS key file
	NewAgent        func(*WSConn) Agent // callback to create a new agent
	ln              net.Listener        // network listener
	handler         *WSHandler          // WebSocket handler
}

// WSHandler handles WebSocket connections and manages their lifecycle.
type WSHandler struct {
	maxConnNum      int                 // maximum number of connections
	pendingWriteNum int                 // pending write queue length per connection
	maxMsgLen       uint32              // maximum message length
	newAgent        func(*WSConn) Agent // callback to create a new agent
	upgrader        websocket.Upgrader  // WebSocket upgrader
	conns           WebsocketConnSet    // set of active connections
	mutexConns      sync.Mutex          // mutex for connection set
	wg              sync.WaitGroup      // wait group for active connections
}

// getRealIP extracts the real IP address from the HTTP request headers.
func getRealIP(req *http.Request) net.Addr {
	ip := req.Header.Get("x-forwarded-for")
	if ip == "" {
		ip = req.Header.Get("x-real-ip")
	}
	if ip != "" {
		ip = strings.Split(ip, ",")[0]
	} else {
		ip, _, _ = net.SplitHostPort(req.RemoteAddr)
	}
	q := net.ParseIP(ip)
	return &net.IPAddr{IP: q}
}

// ServeHTTP handles incoming WebSocket upgrade requests.
func (handler *WSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	conn, err := handler.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logs.Error("upgrade error: %v", err)
		return
	}
	conn.SetReadLimit(int64(handler.maxMsgLen))

	handler.wg.Add(1)
	defer handler.wg.Done()

	handler.mutexConns.Lock()
	if handler.conns == nil {
		handler.mutexConns.Unlock()
		conn.Close()
		return
	}
	if len(handler.conns) >= handler.maxConnNum {
		handler.mutexConns.Unlock()
		conn.Close()
		logs.Error("too many connections. conn num=%v, limit=%v", len(handler.conns), handler.maxConnNum)
		return
	}
	handler.conns[conn] = struct{}{}
	handler.mutexConns.Unlock()

	wsConn := newWSConn(conn, handler.pendingWriteNum, handler.maxMsgLen)
	wsConn.SetOriginIP(getRealIP(r))
	agent := handler.newAgent(wsConn)
	agent.Run()

	// cleanup
	wsConn.Close()
	handler.mutexConns.Lock()
	delete(handler.conns, conn)
	handler.mutexConns.Unlock()
	agent.OnClose()
}

// Start initializes the WebSocket server and starts listening for connections.
func (server *WSServer) Start() {
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		logs.Fatal("failed to start listener: %v", err)
	}

	if server.MaxConnNum <= 0 {
		server.MaxConnNum = 100
		logs.Info("invalid maxconnnum. resetting to default value: %v", server.MaxConnNum)
	}
	if server.PendingWriteNum <= 0 {
		server.PendingWriteNum = 100
		logs.Info("invalid pendingwritenum. resetting to default value: %v", server.PendingWriteNum)
	}
	if server.MaxMsgLen <= 0 {
		server.MaxMsgLen = 4096
		logs.Info("invalid maxmsglen. resetting to default value: %v", server.MaxMsgLen)
	}
	if server.HTTPTimeout <= 0 {
		server.HTTPTimeout = 10 * time.Second
		logs.Info("invalid httptimeout. resetting to default value: %v", server.HTTPTimeout)
	}
	if server.NewAgent == nil {
		logs.Fatal("newagent callback must not be nil. please provide a valid function.")
	}

	if server.CertFile != "" || server.KeyFile != "" {
		config := &tls.Config{NextProtos: []string{"http/1.1"}}
		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0], err = tls.LoadX509KeyPair(server.CertFile, server.KeyFile)
		if err != nil {
			logs.Fatal("failed to load certificates: %v", err)
		}
		ln = tls.NewListener(ln, config)
	}

	server.ln = ln
	server.handler = &WSHandler{
		maxConnNum:      server.MaxConnNum,
		pendingWriteNum: server.PendingWriteNum,
		maxMsgLen:       server.MaxMsgLen,
		newAgent:        server.NewAgent,
		conns:           make(WebsocketConnSet),
		upgrader: websocket.Upgrader{
			HandshakeTimeout: server.HTTPTimeout,
			CheckOrigin:      func(_ *http.Request) bool { return true },
		},
	}

	httpServer := &http.Server{
		Addr:           server.Addr,
		Handler:        server.handler,
		ReadTimeout:    server.HTTPTimeout,
		WriteTimeout:   server.HTTPTimeout,
		MaxHeaderBytes: 1024,
	}

	go httpServer.Serve(ln)
}

// Close gracefully shuts down the WebSocket server and closes all active connections.
func (server *WSServer) Close() {
	server.ln.Close()

	server.handler.mutexConns.Lock()
	for conn := range server.handler.conns {
		conn.Close()
	}
	server.handler.conns = nil
	server.handler.mutexConns.Unlock()

	server.handler.wg.Wait()
}
