package json

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/yinyihanbing/gserv/chanrpc"
	"github.com/yinyihanbing/gutils/logs"
)

// Processor handles the registration, routing, and marshaling of JSON messages.
type Processor struct {
	msgInfo map[string]*MsgInfo // Stores metadata about registered messages
}

// MsgInfo contains metadata about a registered message type.
type MsgInfo struct {
	msgType       reflect.Type
	msgRouter     *chanrpc.Server
	msgHandler    MsgHandler
	msgRawHandler MsgHandler
}

// MsgHandler defines the function signature for message handlers.
type MsgHandler func([]any)

// MsgRaw represents a raw JSON message with its ID and raw data.
type MsgRaw struct {
	msgID      string
	msgRawData json.RawMessage
}

// NewProcessor creates a new Processor instance with an initialized msgInfo map.
// Returns: Pointer to the new Processor
func NewProcessor() *Processor {
	p := new(Processor)
	p.msgInfo = make(map[string]*MsgInfo)
	return p
}

// getMsgInfo retrieves message metadata and ID for a given message.
// Parameters: msg - the message object
// Returns: Pointer to MsgInfo and the message ID
// Panics if the message is not registered or is not a pointer
func (p *Processor) getMsgInfo(msg any) (*MsgInfo, string) {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		logs.Fatal("json message pointer is required")
	}
	msgID := msgType.Elem().Name()
	i, ok := p.msgInfo[msgID]
	if !ok {
		logs.Fatal("message %v is not registered", msgID)
	}
	return i, msgID
}

// Register registers a new message type with the processor.
// Parameters: msg - the message object (must be a pointer)
// Returns: The message ID
// Panics if the message is already registered or invalid
func (p *Processor) Register(msg any) string {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		logs.Fatal("json message pointer is required")
	}
	msgID := msgType.Elem().Name()
	if msgID == "" {
		logs.Fatal("json message must have a name")
	}
	if _, ok := p.msgInfo[msgID]; ok {
		logs.Fatal("message %v is already registered", msgID)
	}

	i := &MsgInfo{msgType: msgType}
	p.msgInfo[msgID] = i
	return msgID
}

// SetRouter sets a router for a specific message type.
// Parameters: msg - the message object, msgRouter - the router to handle the message
// Panics if the message is not registered
func (p *Processor) SetRouter(msg any, msgRouter *chanrpc.Server) {
	i, _ := p.getMsgInfo(msg)
	i.msgRouter = msgRouter
}

// SetHandler sets a handler function for a specific message type.
// Parameters: msg - the message object, msgHandler - the handler function
// Panics if the message is not registered
func (p *Processor) SetHandler(msg any, msgHandler MsgHandler) {
	i, _ := p.getMsgInfo(msg)
	i.msgHandler = msgHandler
}

// SetRawHandler sets a raw handler function for a specific message ID.
// Parameters: msgID - the message ID, msgRawHandler - the raw handler function
// Panics if the message is not registered
func (p *Processor) SetRawHandler(msgID string, msgRawHandler MsgHandler) {
	i, ok := p.msgInfo[msgID]
	if !ok {
		logs.Fatal("message %v is not registered", msgID)
	}
	i.msgRawHandler = msgRawHandler
}

// Route routes a message to the appropriate handler or router.
// Parameters: msg - the message object, userData - additional data for the handler
// Returns: An error if the message is not registered or invalid
func (p *Processor) Route(msg any, userData any) error {
	// raw
	if msgRaw, ok := msg.(MsgRaw); ok {
		i, ok := p.msgInfo[msgRaw.msgID]
		if !ok {
			return fmt.Errorf("message %v is not registered", msgRaw.msgID)
		}
		if i.msgRawHandler != nil {
			i.msgRawHandler([]any{msgRaw.msgID, msgRaw.msgRawData, userData})
		}
		return nil
	}

	// json
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		return errors.New("json message pointer is required")
	}
	msgID := msgType.Elem().Name()
	i, ok := p.msgInfo[msgID]
	if !ok {
		return fmt.Errorf("message %v is not registered", msgID)
	}
	if i.msgHandler != nil {
		i.msgHandler([]any{msg, userData})
	}
	if i.msgRouter != nil {
		i.msgRouter.Go(msgType, msg, userData)
	}
	return nil
}

// Unmarshal unmarshals JSON data into a message object.
// Parameters: data - the JSON data
// Returns: The message object and an error if unmarshaling fails
func (p *Processor) Unmarshal(data []byte) (any, error) {
	var m map[string]json.RawMessage
	err := json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	if len(m) != 1 {
		return nil, errors.New("invalid json data")
	}

	for msgID, data := range m {
		i, ok := p.msgInfo[msgID]
		if !ok {
			return nil, fmt.Errorf("message %v is not registered", msgID)
		}

		// msg
		if i.msgRawHandler != nil {
			return MsgRaw{msgID, data}, nil
		} else {
			msg := reflect.New(i.msgType.Elem()).Interface()
			return msg, json.Unmarshal(data, msg)
		}
	}

	panic("unreachable code")
}

// Marshal marshals a message object into JSON data.
// Parameters: msg - the message object
// Returns: A slice of byte slices containing the JSON data and an error if marshaling fails
func (p *Processor) Marshal(msg any) ([][]byte, error) {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		return nil, errors.New("json message pointer is required")
	}
	msgID := msgType.Elem().Name()
	if _, ok := p.msgInfo[msgID]; !ok {
		return nil, fmt.Errorf("message %v is not registered", msgID)
	}

	// data
	m := map[string]any{msgID: msg}
	data, err := json.Marshal(m)
	return [][]byte{data}, err
}
