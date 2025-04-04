package chanrpc

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"

	"github.com/yinyihanbing/gserv/conf"
	"github.com/yinyihanbing/gutils/logs"
)

// one server per goroutine (goroutine not safe)
// one client per goroutine (goroutine not safe)
type Server struct {
	// id -> function
	//
	// function:
	// func(args []any)
	// func(args []any) any
	// func(args []any) []any
	functions map[any]any
	ChanCall  chan *CallInfo
}

type CallInfo struct {
	f       any
	args    []any
	chanRet chan *RetInfo
	cb      any
}

type RetInfo struct {
	// nil
	// any
	// []any
	ret any
	err error
	// callback:
	// func(err error)
	// func(ret any, err error)
	// func(ret []any, err error)
	cb any
}

type Client struct {
	s               *Server
	chanSyncRet     chan *RetInfo
	ChanAsynRet     chan *RetInfo
	pendingAsynCall int
}

func NewServer(l int) *Server {
	s := new(Server)
	s.functions = make(map[any]any)
	s.ChanCall = make(chan *CallInfo, l)
	return s
}

func assert(i any) []any {
	if i == nil {
		return nil
	} else {
		return i.([]any)
	}
}

// you must call the function before calling Open and Go
func (s *Server) Register(id any, f any) {
	if _, ok := s.functions[id]; ok {
		panic(fmt.Sprintf("function id %v: already registered", id))
	}
	s.functions[id] = f
}

func (s *Server) ret(ci *CallInfo, ri *RetInfo) (err error) {
	if ci.chanRet == nil {
		return
	}

	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	ri.cb = ci.cb
	ci.chanRet <- ri
	return
}

func (s *Server) exec(ci *CallInfo) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if conf.LenStackBuf > 0 {
				buf := make([]byte, conf.LenStackBuf)
				l := runtime.Stack(buf, false)
				err = fmt.Errorf("%v: %s", r, buf[:l])
			} else {
				err = fmt.Errorf("%v", r)
			}

			s.ret(ci, &RetInfo{err: fmt.Errorf("%v", r)})
		}
	}()

	// execute
	switch ci.f.(type) {
	case func([]any):
		ci.f.(func([]any))(ci.args)
		return s.ret(ci, &RetInfo{})
	case func([]any) any:
		ret := ci.f.(func([]any) any)(ci.args)
		return s.ret(ci, &RetInfo{ret: ret})
	case func([]any) []any:
		ret := ci.f.(func([]any) []any)(ci.args)
		return s.ret(ci, &RetInfo{ret: ret})
	default:
		vs := make([]reflect.Value, len(ci.args))
		for k, v := range ci.args {
			vs[k] = reflect.ValueOf(v)
		}
		reflect.ValueOf(ci.f).Call(vs)
		return s.ret(ci, &RetInfo{})
	}
}

func (s *Server) Exec(ci *CallInfo) {
	err := s.exec(ci)
	if err != nil {
		logs.Error("%v", err)
	}
}

// goroutine safe
func (s *Server) Go(id any, args ...any) {
	f := s.functions[id]
	if f == nil {
		logs.Debug("not exists callback func,  key=%v", id)
		for _, v := range s.functions {
			logs.Debug("register callback func, key=%v", v)
		}
		return
	}
	defer func() {
		recover()
	}()

	s.ChanCall <- &CallInfo{
		f:    f,
		args: args,
	}
}

// goroutine safe
func (s *Server) Call0(id any, args ...any) error {
	return s.Open(0).Call0(id, args...)
}

// goroutine safe
func (s *Server) Call1(id any, args ...any) (any, error) {
	return s.Open(0).Call1(id, args...)
}

// goroutine safe
func (s *Server) CallN(id any, args ...any) ([]any, error) {
	return s.Open(0).CallN(id, args...)
}

func (s *Server) Close() {
	close(s.ChanCall)

	for ci := range s.ChanCall {
		s.ret(ci, &RetInfo{
			err: errors.New("chanrpc server closed"),
		})
	}
}

// goroutine safe
func (s *Server) Open(l int) *Client {
	c := NewClient(l)
	c.Attach(s)
	return c
}

func NewClient(l int) *Client {
	c := new(Client)
	c.chanSyncRet = make(chan *RetInfo, 1)
	c.ChanAsynRet = make(chan *RetInfo, l)
	return c
}

func (c *Client) Attach(s *Server) {
	c.s = s
}

func (c *Client) call(ci *CallInfo, block bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	if block {
		c.s.ChanCall <- ci
	} else {
		select {
		case c.s.ChanCall <- ci:
		default:
			err = errors.New("chanrpc channel full")
		}
	}
	return
}

func (c *Client) f(id any, n int) (f any, err error) {
	if c.s == nil {
		err = errors.New("server not attached")
		return
	}

	f = c.s.functions[id]
	if f == nil {
		err = fmt.Errorf("function id %v: function not registered", id)
		return
	}

	var ok bool
	switch n {
	case 0:
		_, ok = f.(func([]any))
	case 1:
		_, ok = f.(func([]any) any)
	case 2:
		_, ok = f.(func([]any) []any)
	default:
		panic("bug")
	}

	if !ok {
		err = fmt.Errorf("function id %v: return type mismatch", id)
	}
	return
}

func (c *Client) Call0(id any, args ...any) error {
	f, err := c.f(id, 0)
	if err != nil {
		return err
	}

	err = c.call(&CallInfo{
		f:       f,
		args:    args,
		chanRet: c.chanSyncRet,
	}, true)
	if err != nil {
		return err
	}

	ri := <-c.chanSyncRet
	return ri.err
}

func (c *Client) Call1(id any, args ...any) (any, error) {
	f, err := c.f(id, 1)
	if err != nil {
		return nil, err
	}

	err = c.call(&CallInfo{
		f:       f,
		args:    args,
		chanRet: c.chanSyncRet,
	}, true)
	if err != nil {
		return nil, err
	}

	ri := <-c.chanSyncRet
	return ri.ret, ri.err
}

func (c *Client) CallN(id any, args ...any) ([]any, error) {
	f, err := c.f(id, 2)
	if err != nil {
		return nil, err
	}

	err = c.call(&CallInfo{
		f:       f,
		args:    args,
		chanRet: c.chanSyncRet,
	}, true)
	if err != nil {
		return nil, err
	}

	ri := <-c.chanSyncRet
	return assert(ri.ret), ri.err
}

func (c *Client) asynCall(id any, args []any, cb any, n int) {
	f, err := c.f(id, n)
	if err != nil {
		c.ChanAsynRet <- &RetInfo{err: err, cb: cb}
		return
	}
	err = c.call(&CallInfo{
		f:       f,
		args:    args,
		chanRet: c.ChanAsynRet,
		cb:      cb,
	}, false)
	if err != nil {
		c.ChanAsynRet <- &RetInfo{err: err, cb: cb}
		return
	}
}

func (c *Client) AsynCall(id any, _args ...any) {
	if len(_args) < 1 {
		panic("callback function not found")
	}

	args := _args[:len(_args)-1]
	cb := _args[len(_args)-1]

	var n int
	switch cb.(type) {
	case func(error):
		n = 0
	case func(any, error):
		n = 1
	case func([]any, error):
		n = 2
	default:
		panic("definition of callback function is invalid")
	}

	// too many calls
	if c.pendingAsynCall >= cap(c.ChanAsynRet) {
		execCb(&RetInfo{err: errors.New("too many calls"), cb: cb})
		return
	}

	c.asynCall(id, args, cb, n)
	c.pendingAsynCall++
}

func execCb(ri *RetInfo) {
	defer func() {
		if r := recover(); r != nil {
			if conf.LenStackBuf > 0 {
				buf := make([]byte, conf.LenStackBuf)
				l := runtime.Stack(buf, false)
				logs.Error("%v: %s", r, buf[:l])
			} else {
				logs.Error("%v", r)
			}
		}
	}()

	// execute
	switch ri.cb.(type) {
	case func(error):
		ri.cb.(func(error))(ri.err)
	case func(any, error):
		ri.cb.(func(any, error))(ri.ret, ri.err)
	case func([]any, error):
		ri.cb.(func([]any, error))(assert(ri.ret), ri.err)
	default:
		panic("bug")
	}
}

func (c *Client) Cb(ri *RetInfo) {
	c.pendingAsynCall--
	execCb(ri)
}

func (c *Client) Close() {
	for c.pendingAsynCall > 0 {
		c.Cb(<-c.ChanAsynRet)
	}
}

func (c *Client) Idle() bool {
	return c.pendingAsynCall == 0
}
