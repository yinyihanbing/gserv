package cluster

import (
	"math"
	"net"
	"reflect"
	"time"

	"github.com/yinyihanbing/gserv/chanrpc"
	"github.com/yinyihanbing/gserv/conf"
	"github.com/yinyihanbing/gserv/network"
	"github.com/yinyihanbing/gserv/network/protobuf"
	"github.com/yinyihanbing/gutils/logs"
)

var (
	server  *network.TCPServer
	clients []*network.TCPClient

	AgentChanRPC *chanrpc.Server
	Processor    *protobuf.Processor
)

// Init initializes the cluster by starting the server and connecting clients.
func Init() {
	if conf.ListenAddr != "" {
		server = new(network.TCPServer)
		// configure server settings
		server.Addr = conf.ListenAddr
		server.MaxConnNum = int(math.MaxInt32)
		server.PendingWriteNum = conf.PendingWriteNum
		server.LenMsgLen = 2
		server.MaxMsgLen = math.MaxUint32
		server.NewAgent = newAgent

		server.Start()

		logs.Info("game cluster service startup: %v", conf.ListenAddr)
	}

	for _, addr := range conf.ConnAddrs {
		client := new(network.TCPClient)
		// configure client settings
		client.Addr = addr
		client.ConnNum = 1
		client.ConnectInterval = 3 * time.Second
		client.PendingWriteNum = conf.PendingWriteNum
		client.LenMsgLen = 2
		client.MaxMsgLen = math.MaxUint32
		client.NewAgent = newAgent
		client.AutoReconnect = true

		client.Start()
		clients = append(clients, client)

		logs.Info("game client service startup: %v", addr)
	}
}

// Destroy stops the server and closes all client connections.
func Destroy() {
	if server != nil {
		server.Close()
	}

	for _, client := range clients {
		client.Close()
	}
}

// Agent represents a network connection agent.
type Agent struct {
	conn     *network.TCPConn // underlying TCP connection
	userData interface{}      // user-specific data
}

// newAgent creates a new Agent instance.
func newAgent(conn *network.TCPConn) network.Agent {
	a := new(Agent)
	a.conn = conn
	AgentChanRPC.Go("NewAgent", a)
	return a
}

// Run processes incoming messages for the agent.
func (a *Agent) Run() {
	for {
		data, err := a.conn.ReadMsg()
		if err != nil {
			logs.Error("read message error: %v", err)
			break
		}
		if Processor != nil {
			msg, err := Processor.Unmarshal(data)
			if err != nil {
				logs.Error("unmarshal message error: %v", err)
				break
			}

			err = Processor.Route(msg, a)
			if err != nil {
				logs.Error("route message error: %v", err)
				break
			}
		}
	}
}

// OnClose handles cleanup when the agent's connection is closed.
func (a *Agent) OnClose() {
	if AgentChanRPC != nil {
		err := AgentChanRPC.Call0("CloseAgent", a)
		if err != nil {
			logs.Error("chanrpc error: %v", err)
		}
	}
}

// WriteMsg sends a message to the agent's connection.
func (a *Agent) WriteMsg(msg interface{}) {
	if Processor != nil {
		data, err := Processor.Marshal(msg)
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

// LocalAddr returns the local address of the agent's connection.
func (a *Agent) LocalAddr() net.Addr {
	return a.conn.LocalAddr()
}

// RemoteAddr returns the remote address of the agent's connection.
func (a *Agent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

// Close closes the agent's connection.
func (a *Agent) Close() {
	a.conn.Close()
}

// Destroy forcibly destroys the agent's connection.
func (a *Agent) Destroy() {
	a.conn.Destroy()
}

// UserData retrieves the user-specific data associated with the agent.
func (a *Agent) UserData() interface{} {
	return a.userData
}

// SetUserData sets the user-specific data for the agent.
func (a *Agent) SetUserData(data interface{}) {
	a.userData = data
}
