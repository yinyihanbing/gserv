package cluster

import (
	"math"
	"time"

	"github.com/yinyihanbing/gserv/conf"
	"github.com/yinyihanbing/gserv/network"
	"github.com/yinyihanbing/gutils/logs"
	"github.com/yinyihanbing/gserv/network/protobuf"
	"github.com/yinyihanbing/gserv/chanrpc"
)

var (
	server  *network.TCPServer
	clients []*network.TCPClient

	AgentChanRPC *chanrpc.Server
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
	conn      *network.TCPConn
	Processor network.Processor
}

func newAgent(conn *network.TCPConn) network.Agent {
	a := new(Agent)
	a.conn = conn
	a.Processor = protobuf.NewProcessor()
	AgentChanRPC.Go("NewAgent", a)
	return a
}

func (a *Agent) Run() {
	for {
		data, err := a.conn.ReadMsg()
		if err != nil {
			break
		}
		if a.Processor != nil {
			msg, err := a.Processor.Unmarshal(data)
			if err != nil {
				logs.Error("unmarshal message error: %v", err)
				break
			}

			err = a.Processor.Route(msg, a)
			if err != nil {
				logs.Error("route message error: %v", err)
				break
			}
		}
	}
}

func (a *Agent) OnClose() {

}
