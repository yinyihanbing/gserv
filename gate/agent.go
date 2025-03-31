package gate

import (
	"net"
)

// Agent defines the interface for a network agent.
// it provides methods for message handling, connection management, and user data storage.
type Agent interface {
	// WriteMsg sends a message to the remote connection.
	// msg: the message to be sent.
	WriteMsg(msg any)

	// LocalAddr returns the local network address of the agent.
	LocalAddr() net.Addr

	// RemoteAddr returns the remote network address of the agent.
	RemoteAddr() net.Addr

	// Close gracefully closes the connection.
	Close()

	// Destroy forcefully terminates the connection and cleans up resources.
	Destroy()

	// UserData retrieves the user-defined data associated with the agent.
	UserData() any

	// SetUserData sets user-defined data for the agent.
	// data: the data to associate with the agent.
	SetUserData(data any)
}
