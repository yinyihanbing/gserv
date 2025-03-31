package conf

// LenStackBuf defines the length of the stack buffer.
var (
	LenStackBuf = 4096

	// console configuration
	ConsolePort   int                // port for console access
	ConsolePrompt string = "Gserv# " // default console prompt
	ProfilePath   string             // path for profile data

	// cluster configuration
	ListenAddr      string   // address to listen for incoming connections
	ConnAddrs       []string // list of connection addresses
	PendingWriteNum int      // number of pending writes allowed
)
