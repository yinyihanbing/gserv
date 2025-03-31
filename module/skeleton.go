package module

import (
	"time"

	"github.com/yinyihanbing/gserv/chanrpc"
	g "github.com/yinyihanbing/gserv/go"
	"github.com/yinyihanbing/gutils/timer"
)

type Skeleton struct {
	GoLen              int
	TimerDispatcherLen int
	AsynCallLen        int
	ChanRPCServer      *chanrpc.Server
	g                  *g.Go
	dispatcher         *timer.Dispatcher
	client             *chanrpc.Client
	server             *chanrpc.Server
	commandServer      *chanrpc.Server
}

// Init initializes the Skeleton with default values and creates necessary components.
func (s *Skeleton) Init() {
	s.GoLen = max(s.GoLen, 0)
	s.TimerDispatcherLen = max(s.TimerDispatcherLen, 0)
	s.AsynCallLen = max(s.AsynCallLen, 0)

	s.g = g.New(s.GoLen)
	s.dispatcher = timer.NewDispatcher(s.TimerDispatcherLen)
	s.client = chanrpc.NewClient(s.AsynCallLen)
	s.server = s.ChanRPCServer
	if s.server == nil {
		s.server = chanrpc.NewServer(0)
	}
	s.commandServer = chanrpc.NewServer(0)
}

// Run starts the main loop of the Skeleton, handling various events until a close signal is received.
func (s *Skeleton) Run(closeSig chan bool) {
	for {
		select {
		case <-closeSig:
			s.shutdown()
			return
		case ri := <-s.client.ChanAsynRet:
			s.client.Cb(ri)
		case ci := <-s.server.ChanCall:
			s.server.Exec(ci)
		case ci := <-s.commandServer.ChanCall:
			s.commandServer.Exec(ci)
		case cb := <-s.g.ChanCb:
			s.g.Cb(cb)
		case t := <-s.dispatcher.ChanTimer:
			t.Cb()
		}
	}
}

// AfterFunc schedules a function to be executed after a specified duration.
func (s *Skeleton) AfterFunc(d time.Duration, cb func()) *timer.Timer {
	s.ensureValidDispatcher()
	return s.dispatcher.AfterFunc(d, cb)
}

// CronFunc schedules a function to be executed based on a Cron expression.
func (s *Skeleton) CronFunc(cronExpr *timer.CronExpr, cb func()) *timer.Cron {
	s.ensureValidDispatcher()
	return s.dispatcher.CronFunc(cronExpr, cb)
}

// CronFuncExt parses a Cron expression string and schedules a function to be executed accordingly.
func (s *Skeleton) CronFuncExt(expr string, cb func()) *timer.Cron {
	s.ensureValidDispatcher()
	cronExpr, err := timer.NewCronExpr(expr)
	if err != nil {
		panic("invalid CronExpr")
	}
	return s.dispatcher.CronFunc(cronExpr, cb)
}

// Go executes a function asynchronously and invokes a callback upon completion.
func (s *Skeleton) Go(f func(), cb func()) {
	s.ensureValidGo()
	s.g.Go(f, cb)
}

// NewLinearContext creates a new linear context for sequential asynchronous execution.
func (s *Skeleton) NewLinearContext() *g.LinearContext {
	s.ensureValidGo()
	return s.g.NewLinearContext()
}

// AsynCall performs an asynchronous call to a ChanRPC server.
func (s *Skeleton) AsynCall(server *chanrpc.Server, id interface{}, args ...interface{}) {
	s.ensureValidClient()
	s.client.Attach(server)
	s.client.AsynCall(id, args...)
}

// RegisterChanRPC registers a function with a ChanRPC server for remote procedure calls.
func (s *Skeleton) RegisterChanRPC(id interface{}, f interface{}) {
	if s.ChanRPCServer == nil {
		panic("invalid ChanRPCServer")
	}
	s.server.Register(id, f)
}

// ensureValidDispatcher checks if the TimerDispatcherLen is valid.
func (s *Skeleton) ensureValidDispatcher() {
	if s.TimerDispatcherLen == 0 {
		panic("invalid TimerDispatcherLen")
	}
}

// ensureValidGo checks if the GoLen is valid.
func (s *Skeleton) ensureValidGo() {
	if s.GoLen == 0 {
		panic("invalid GoLen")
	}
}

// ensureValidClient checks if the AsynCallLen is valid.
func (s *Skeleton) ensureValidClient() {
	if s.AsynCallLen == 0 {
		panic("invalid AsynCallLen")
	}
}

// shutdown gracefully shuts down the Skeleton, ensuring all resources are released.
func (s *Skeleton) shutdown() {
	s.commandServer.Close()
	s.server.Close()
	for !s.g.Idle() || !s.client.Idle() {
		s.g.Close()
		s.client.Close()
	}
}

// max returns the maximum of two integers.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
