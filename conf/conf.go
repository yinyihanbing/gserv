package conf

var (
	LenStackBuf = 4096

	// console
	ConsolePort   int
	ConsolePrompt string = "Gserv# "
	ProfilePath   string

	// cluster
	ListenAddr      string
	ConnAddrs       []string
	PendingWriteNum int
)
