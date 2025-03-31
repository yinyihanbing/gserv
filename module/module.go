package module

import (
	"runtime"
	"sync"

	"github.com/yinyihanbing/gserv/conf"
	"github.com/yinyihanbing/gutils/logs"
)

// Module defines the interface for a module with lifecycle methods.
type Module interface {
	OnInit()                // Called to initialize the module.
	OnDestroy()             // Called to clean up resources when the module is destroyed.
	Run(closeSig chan bool) // Called to run the module, listens for a close signal.
}

type module struct {
	mi       Module         // The module instance.
	closeSig chan bool      // Channel to signal the module to stop.
	wg       sync.WaitGroup // WaitGroup to manage module's goroutines.
}

var mods []*module // List of registered modules.

// Register adds a new module to the list of registered modules.
// Parameter: mi - the module instance to register.
func Register(mi Module) {
	mods = append(mods, &module{
		mi:       mi,
		closeSig: make(chan bool, 1),
	})
}

// Init initializes all registered modules and starts their execution.
func Init() {
	for _, m := range mods {
		m.mi.OnInit() // Call the module's initialization method.
		m.wg.Add(1)   // Increment the WaitGroup counter.
		go run(m)     // Start the module's Run method in a goroutine.
	}
}

// Destroy stops and cleans up all registered modules in reverse order.
func Destroy() {
	for i := len(mods) - 1; i >= 0; i-- {
		m := mods[i]
		m.closeSig <- true // Send a signal to stop the module.
		m.wg.Wait()        // Wait for the module's goroutine to finish.
		safeDestroy(m)     // Safely destroy the module.
	}
}

// run executes the module's Run method.
// Parameter: m - the module instance.
func run(m *module) {
	defer m.wg.Done() // Decrement the WaitGroup counter when done.
	m.mi.Run(m.closeSig)
}

// safeDestroy safely calls the module's OnDestroy method, recovering from panics.
// Parameter: m - the module instance.
func safeDestroy(m *module) {
	defer func() {
		if r := recover(); r != nil {
			logError(r) // Log any panic that occurs.
		}
	}()
	m.mi.OnDestroy() // Call the module's destruction method.
}

// logError logs an error message and stack trace if available.
// Parameter: r - the error or panic value.
func logError(r any) {
	if conf.LenStackBuf > 0 {
		buf := make([]byte, conf.LenStackBuf)
		l := runtime.Stack(buf, false)
		logs.Error("%v: %s", r, buf[:l]) // Log the error and stack trace.
	} else {
		logs.Error("%v", r) // Log only the error.
	}
}
