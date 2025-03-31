package gserv

import (
	"os"
	"os/signal"
	"syscall" // added syscall package

	"github.com/yinyihanbing/gserv/cluster"
	"github.com/yinyihanbing/gserv/console"
	"github.com/yinyihanbing/gserv/module"
	"github.com/yinyihanbing/gserv/storage"
	"github.com/yinyihanbing/gutils/logs"
)

// Run initializes and starts the gserv application.
// it registers modules, initializes the cluster and console, and waits for termination signals.
func Run(mods ...module.Module) {
	logs.Info("gserv starting up") // log startup message

	// register and initialize modules
	for i := range len(mods) {
		module.Register(mods[i]) // register each module
	}
	module.Init() // initialize all registered modules

	// initialize cluster
	cluster.Init()

	// initialize console
	console.Init()

	// wait for termination signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	sig := <-c
	logs.Info("gserv closing down (signal: %v)", sig) // log shutdown message
	Stop()                                            // call stop to clean up resources
}

// Stop gracefully shuts down the gserv application.
// it destroys the cluster, modules, and storage resources.
func Stop() {
	cluster.Destroy() // destroy cluster resources
	module.Destroy()  // destroy module resources
	storage.Destroy() // destroy storage resources
}
