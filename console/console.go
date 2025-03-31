package console

import (
	"bufio"
	"math"
	"strconv"
	"strings"

	"github.com/yinyihanbing/gserv/conf"
	"github.com/yinyihanbing/gserv/network"
	"github.com/yinyihanbing/gutils/logs"
)

var server *network.TCPServer

// Init initializes the console service if the console port is configured.
func Init() {
	if conf.ConsolePort == 0 {
		return
	}
	server = new(network.TCPServer)
	server.Addr = "localhost:" + strconv.Itoa(conf.ConsolePort)
	server.MaxConnNum = int(math.MaxInt32)
	server.PendingWriteNum = 100
	server.NewAgent = newAgent

	server.Start()

	logs.Info("game console service startup: %v", server.Addr)
}

// Destroy stops the console service and releases resources.
func Destroy() {
	if server != nil {
		server.Close()
		logs.Info("game console service stopped: %v", server.Addr)
	}
}

type Agent struct {
	conn   *network.TCPConn
	reader *bufio.Reader
}

// newAgent creates a new Agent instance for handling console connections.
func newAgent(conn *network.TCPConn) network.Agent {
	a := new(Agent)
	a.conn = conn
	a.reader = bufio.NewReader(conn)
	return a
}

// Run handles incoming commands from the console connection.
func (a *Agent) Run() {
	for {
		// Display the console prompt if configured.
		if conf.ConsolePrompt != "" {
			a.conn.Write([]byte(conf.ConsolePrompt))
		}

		// Read a line of input from the console.
		line, err := a.reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSuffix(line[:len(line)-1], "\r")

		// Parse the input into command arguments.
		args := strings.Fields(line)
		if len(args) == 0 {
			continue
		}
		if args[0] == "quit" {
			break
		}

		// Find and execute the corresponding command.
		var c Command
		for _, _c := range commands {
			if _c.name() == args[0] {
				c = _c
				break
			}
		}
		if c == nil {
			a.conn.Write([]byte("command not found, try `help` for help\r\n"))
			continue
		}
		output := c.run(args[1:])
		if output != "" {
			a.conn.Write([]byte(output + "\r\n"))
		}
	}
}

// OnClose is called when the connection is closed.
func (a *Agent) OnClose() {
	// No specific cleanup required for now.
}
