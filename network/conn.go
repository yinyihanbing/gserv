package network

import (
	"net"
)

// Conn defines an interface for managing network connections.
type Conn interface {
	// ReadMsg reads a complete message from the connection.
	// Returns the message data or an error if the read fails.
	ReadMsg() ([]byte, error)

	// WriteMsg writes one or more message parts to the connection.
	// Returns an error if the write fails.
	WriteMsg(args ...[]byte) error

	// LocalAddr returns the local network address of the connection.
	LocalAddr() net.Addr

	// RemoteAddr returns the remote network address of the connection.
	RemoteAddr() net.Addr

	// Close closes the connection gracefully.
	Close()

	// Destroy forcefully closes the connection and releases resources.
	Destroy()
}
