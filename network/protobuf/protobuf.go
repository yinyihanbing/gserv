package protobuf

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/yinyihanbing/gserv/chanrpc"
	"github.com/yinyihanbing/gutils/logs"
	"google.golang.org/protobuf/proto"
)

// Processor handles the registration, routing, and marshaling of protobuf messages.
type Processor struct {
	littleEndian bool                    // Determines the byte order for encoding/decoding message IDs
	msgInfo      []*MsgInfo              // Stores metadata about registered messages
	msgID        map[reflect.Type]uint16 // Maps message types to their IDs
	mu           sync.RWMutex            // Ensures thread-safe access to msgInfo and msgID
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

// MsgRaw represents a raw protobuf message with its ID and raw data.
type MsgRaw struct {
	msgID      uint16
	msgRawData []byte
}

// NewProcessor creates a new Processor instance with default settings.
// Returns: Pointer to the new Processor
func NewProcessor() *Processor {
	return &Processor{
		littleEndian: false,
		msgID:        make(map[reflect.Type]uint16),
	}
}

// SetByteOrder sets the byte order for encoding/decoding message IDs.
// Parameters: littleEndian - true for little-endian, false for big-endian
func (p *Processor) SetByteOrder(littleEndian bool) {
	p.littleEndian = littleEndian
}

// Register registers a new message type with the processor.
// Parameters: msg - the protobuf message object
// Returns: The message ID
// Panics if the message is already registered or exceeds the maximum limit
func (p *Processor) Register(msg proto.Message) uint16 {
	msgType := reflect.TypeOf(msg)
	if err := p.validateMsgType(msgType); err != nil {
		logs.Error("invalid message type: %s", err.Error())
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.msgID[msgType]; ok {
		logs.Error("message type %s is already registered", msgType)
	}
	if len(p.msgInfo) >= math.MaxUint16 {
		logs.Error("exceeded maximum number of protobuf messages (max = %v)", math.MaxUint16)
	}

	i := &MsgInfo{msgType: msgType}
	p.msgInfo = append(p.msgInfo, i)
	id := uint16(len(p.msgInfo) - 1)
	p.msgID[msgType] = id
	return id
}

// SetRouter sets a router for a specific message type.
// Parameters: msg - the protobuf message object, msgRouter - the router to handle the message
// Panics if the message is not registered
func (p *Processor) SetRouter(msg proto.Message, msgRouter *chanrpc.Server) {
	msgType := reflect.TypeOf(msg)
	id := p.getMsgID(msgType)
	p.msgInfo[id].msgRouter = msgRouter
}

// SetHandler sets a handler function for a specific message type.
// Parameters: msg - the protobuf message object, msgHandler - the handler function
// Panics if the message is not registered
func (p *Processor) SetHandler(msg proto.Message, msgHandler MsgHandler) {
	msgType := reflect.TypeOf(msg)
	id := p.getMsgID(msgType)
	p.msgInfo[id].msgHandler = msgHandler
}

// SetRawHandler sets a raw handler function for a specific message ID.
// Parameters: id - the message ID, msgRawHandler - the raw handler function
// Panics if the message ID is not registered
func (p *Processor) SetRawHandler(id uint16, msgRawHandler MsgHandler) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if id >= uint16(len(p.msgInfo)) {
		logs.Fatal("message ID %v is not registered", id)
	}
	p.msgInfo[id].msgRawHandler = msgRawHandler
}

// Route routes a message to the appropriate handler or router.
// Parameters: msg - the message object, userData - additional data for the handler
// Returns: An error if the message is not registered or invalid
func (p *Processor) Route(msg any, userData any) error {
	// raw
	if msgRaw, ok := msg.(MsgRaw); ok {
		if msgRaw.msgID >= uint16(len(p.msgInfo)) {
			return fmt.Errorf("message ID %v is not registered", msgRaw.msgID)
		}
		i := p.msgInfo[msgRaw.msgID]
		if i.msgRawHandler != nil {
			i.msgRawHandler([]any{msgRaw.msgID, msgRaw.msgRawData, userData})
		}
		return nil
	}

	// protobuf
	msgType := reflect.TypeOf(msg)
	id, ok := p.msgID[msgType]
	if !ok {
		return fmt.Errorf("message type %s is not registered", msgType)
	}
	i := p.msgInfo[id]
	if i.msgHandler != nil {
		i.msgHandler([]any{msg, userData})
	}
	if i.msgRouter != nil {
		i.msgRouter.Go(msgType, msg, userData)
	}
	return nil
}

// Unmarshal unmarshals protobuf data into a message object.
// Parameters: data - the protobuf data
// Returns: The message object and an error if unmarshaling fails
func (p *Processor) Unmarshal(data []byte) (any, error) {
	if len(data) < 2 {
		return nil, errors.New("protobuf data is too short")
	}

	// id
	var id uint16
	if p.littleEndian {
		id = binary.LittleEndian.Uint16(data)
	} else {
		id = binary.BigEndian.Uint16(data)
	}
	if id >= uint16(len(p.msgInfo)) {
		return nil, fmt.Errorf("message ID %v is not registered", id)
	}

	// msgInfo
	i := p.msgInfo[id]
	if i.msgRawHandler != nil {
		return MsgRaw{id, data[2:]}, nil
	}

	// protobuf message
	msg := reflect.New(i.msgType.Elem()).Interface()
	if err := proto.Unmarshal(data[2:], msg.(proto.Message)); err != nil {
		return nil, err
	}

	return msg, nil
}

// Marshal marshals a message object into protobuf data.
// Parameters: msg - the protobuf message object
// Returns: A slice of byte slices containing the protobuf data and an error if marshaling fails
func (p *Processor) Marshal(msg any) ([][]byte, error) {
	msgType := reflect.TypeOf(msg)

	// id
	_id, ok := p.msgID[msgType]
	if !ok {
		err := fmt.Errorf("message type %s is not registered", msgType)
		return nil, err
	}

	id := make([]byte, 2)
	if p.littleEndian {
		binary.LittleEndian.PutUint16(id, _id)
	} else {
		binary.BigEndian.PutUint16(id, _id)
	}

	// data
	data, err := proto.Marshal(msg.(proto.Message))
	return [][]byte{id, data}, err
}

// Range iterates over all registered message types and their IDs.
// Parameters: f - a function to execute for each message type and ID
func (p *Processor) Range(f func(id uint16, t reflect.Type)) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for id, i := range p.msgInfo {
		f(uint16(id), i.msgType)
	}
}

// validateMsgType validates that the message type is a pointer.
// Parameters: msgType - the message type to validate
// Returns: An error if the message type is invalid
func (p *Processor) validateMsgType(msgType reflect.Type) error {
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		return errors.New("protobuf message pointer required")
	}
	return nil
}

// getMsgID retrieves the ID for a given message type.
// Parameters: msgType - the message type
// Returns: The message ID
// Panics if the message type is not registered
func (p *Processor) getMsgID(msgType reflect.Type) uint16 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	id, ok := p.msgID[msgType]
	if !ok {
		logs.Fatal("message type %s is not registered", msgType)
	}
	return id
}
