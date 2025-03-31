package gate

import (
	"net"
	"reflect"
	"time"

	"github.com/yinyihanbing/gserv/chanrpc"
	"github.com/yinyihanbing/gserv/network"
	"github.com/yinyihanbing/gutils/logs"
)

type Gate struct {
	MaxConnNum      int
	PendingWriteNum int
	MaxMsgLen       uint32
	Processor       network.Processor
	AgentChanRPC    *chanrpc.Server

	// websocket
	WSAddr      string
	HTTPTimeout time.Duration
	CertFile    string
	KeyFile     string

	// tcp
	TCPAddr      string
	LenMsgLen    int
	LittleEndian bool
}

// Run starts the websocket and TCP servers if configured, and waits for a close signal.
func (gate *Gate) Run(closeSig chan bool) {
	var wsServer *network.WSServer
	if gate.WSAddr != "" {
		// initialize websocket server
		wsServer = new(network.WSServer)
		wsServer.Addr = gate.WSAddr
		wsServer.MaxConnNum = gate.MaxConnNum
		wsServer.PendingWriteNum = gate.PendingWriteNum
		wsServer.MaxMsgLen = gate.MaxMsgLen
		wsServer.HTTPTimeout = gate.HTTPTimeout
		wsServer.CertFile = gate.CertFile
		wsServer.KeyFile = gate.KeyFile
		wsServer.NewAgent = func(conn *network.WSConn) network.Agent {
			a := &agent{conn: conn, gate: gate}
			if gate.AgentChanRPC != nil {
				gate.AgentChanRPC.Go("NewAgent", a)
			}
			return a
		}
	}

	var tcpServer *network.TCPServer
	if gate.TCPAddr != "" {
		// initialize tcp server
		tcpServer = new(network.TCPServer)
		tcpServer.Addr = gate.TCPAddr
		tcpServer.MaxConnNum = gate.MaxConnNum
		tcpServer.PendingWriteNum = gate.PendingWriteNum
		tcpServer.LenMsgLen = gate.LenMsgLen
		tcpServer.MaxMsgLen = gate.MaxMsgLen
		tcpServer.LittleEndian = gate.LittleEndian
		tcpServer.NewAgent = func(conn *network.TCPConn) network.Agent {
			a := &agent{conn: conn, gate: gate}
			if gate.AgentChanRPC != nil {
				gate.AgentChanRPC.Go("NewAgent", a)
			}
			return a
		}
	}

	// start websocket server if configured
	if wsServer != nil {
		wsServer.Start()
		logs.Info("game ws service startup: %v", wsServer.Addr)
	}
	// start tcp server if configured
	if tcpServer != nil {
		tcpServer.Start()
		logs.Info("game tcp service startup: %v", tcpServer.Addr)
	}
	// wait for close signal
	<-closeSig
	// stop websocket server if running
	if wsServer != nil {
		wsServer.Close()
		logs.Info("game ws service stopped: %v", wsServer.Addr)
	}
	// stop tcp server if running
	if tcpServer != nil {
		tcpServer.Close()
		logs.Info("game tcp service stopped: %v", tcpServer.Addr)
	}
}

// OnDestroy is a placeholder for cleanup logic when the gate is destroyed.
func (gate *Gate) OnDestroy() {}

type agent struct {
	conn     network.Conn
	gate     *Gate
	userData any
}

// Run is the main loop for reading and processing messages from the connection.
func (a *agent) Run() {
	for {
		data, err := a.conn.ReadMsg()
		if err != nil {
			break
		}
		if a.gate.Processor != nil {
			// unmarshal and route the message
			msg, err := a.gate.Processor.Unmarshal(data)
			if err != nil {
				logs.Debug("unmarshal message error: %v", err)
				break
			}
			err = a.gate.Processor.Route(msg, a)
			if err != nil {
				logs.Debug("route message error: %v", err)
				break
			}
		}
	}
}

// OnClose handles the closure of the agent and notifies the RPC server if configured.
func (a *agent) OnClose() {
	if a.gate.AgentChanRPC != nil {
		err := a.gate.AgentChanRPC.Call0("CloseAgent", a)
		if err != nil {
			logs.Error("chanrpc error: %v", err)
		}
	}
}

// WriteMsg marshals the message and writes it to the connection.
func (a *agent) WriteMsg(msg any) {
	if a.gate.Processor != nil {
		data, err := a.gate.Processor.Marshal(msg)
		if err != nil {
			logs.Error("marshal message %v error: %v", reflect.TypeOf(msg), err)
			return
		}
		err = a.conn.WriteMsg(data...)
		if err != nil {
			logs.Error("write message %v error: %v", reflect.TypeOf(msg), err)
		}
	}
}

// LocalAddr returns the local address of the connection.
func (a *agent) LocalAddr() net.Addr {
	return a.conn.LocalAddr()
}

// RemoteAddr returns the remote address of the connection.
func (a *agent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

// Close closes the connection.
func (a *agent) Close() {
	a.conn.Close()
}

// Destroy destroys the connection.
func (a *agent) Destroy() {
	a.conn.Destroy()
}

// UserData returns the user data associated with the agent.
func (a *agent) UserData() any {
	return a.userData
}

// SetUserData sets the user data associated with the agent.
func (a *agent) SetUserData(data any) {
	a.userData = data
}
