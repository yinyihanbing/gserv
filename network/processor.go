package network

// Processor defines an interface for processing messages.
type Processor interface {
	// Route processes the given message and user data.
	// Must be goroutine-safe.
	Route(msg any, userData any) error

	// Unmarshal decodes the given byte slice into a message.
	// Must be goroutine-safe.
	Unmarshal(data []byte) (any, error)

	// Marshal encodes the given message into a slice of byte slices.
	// Must be goroutine-safe.
	Marshal(msg any) ([][]byte, error)
}
