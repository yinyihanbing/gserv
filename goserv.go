package gserv

import (
	"os"
	"os/signal"

	"github.com/yinyihanbing/gserv/cluster"
	"github.com/yinyihanbing/gserv/module"
	"github.com/yinyihanbing/gserv/storage"
	"github.com/yinyihanbing/gserv/console"
	"github.com/yinyihanbing/gutils/logs"
)

func Run(mods ...module.Module) {
	logs.Info("Gserv starting up ")

	// module
	for i := 0; i < len(mods); i++ {
		module.Register(mods[i])
	}
	module.Init()

	// cluster
	cluster.Init()

	// console
	console.Init()

	// close
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	sig := <-c
	logs.Info("Gserv closing down (signal: %v)", sig)
	Stop()
}

func Stop() {
	cluster.Destroy()
	module.Destroy()
	storage.Destroy()
}
