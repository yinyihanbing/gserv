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

func Init() {
	if conf.ListenAddr != "" {
		server = new(network.TCPServer)
		server.Addr = conf.ListenAddr
		server.MaxConnNum = int(math.MaxInt32)
		server.PendingWriteNum = conf.PendingWriteNum
		server.LenMsgLen = 2
		server.MaxMsgLen = math.MaxUint32
		server.NewAgent = newAgent

		server.Start()

		logs.Info("Game cluster Service startup: %v", conf.ListenAddr)
	}

	for _, addr := range conf.ConnAddrs {
		client := new(network.TCPClient)
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

		logs.Info("Game client Service startup: %v", addr)
	}
}

func Destroy() {
	if server != nil {
		server.Close()
	}

	for _, client := range clients {
		client.Close()
	}
}

type Agent struct {
	conn     *network.TCPConn
	userData interface{}
}

func newAgent(conn *network.TCPConn) network.Agent {
	a := new(Agent)
	a.conn = conn
	AgentChanRPC.Go("NewAgent", a)
	return a
}

func (a *Agent) Run() {
	for {
		data, err := a.conn.ReadMsg()
		if err != nil {
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

func (a *Agent) OnClose() {
	if AgentChanRPC != nil {
		err := AgentChanRPC.Call0("CloseAgent", a)
		if err != nil {
			logs.Error("chanrpc error: %v", err)
		}
	}
}

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

func (a *Agent) LocalAddr() net.Addr {
	return a.conn.LocalAddr()
}

func (a *Agent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

func (a *Agent) Close() {
	a.conn.Close()
}

func (a *Agent) Destroy() {
	a.conn.Destroy()
}

func (a *Agent) UserData() interface{} {
	return a.userData
}

func (a *Agent) SetUserData(data interface{}) {
	a.userData = data
}
