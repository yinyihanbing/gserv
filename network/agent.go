package network

// Agent defines an interface for managing agent behavior.
type Agent interface {
	// Run starts the agent's main logic.
	Run()

	// OnClose is called when the agent is closed.
	// Used for cleanup or releasing resources.
	OnClose()
}
