package chandy_lamport

import "log"

type SnapshotData struct {
	tokens    int
	openLinks map[string]bool              // key = src, true = still waiting for marker
	messages  map[string][]*SnapshotMessage // key = src, value = messages received
}

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	snapshots     map[int]*SnapshotData // key = snapshotId
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		make(map[int]*SnapshotData),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	switch msg := message.(type) {
	case TokenMessage:
		server.Tokens += msg.numTokens
		for snapshotId, data := range server.snapshots {
			if data.openLinks[src] {
				data.messages[src] = append(data.messages[src], &SnapshotMessage{src, server.Id, msg})
				_ = snapshotId
			}
		}
	case MarkerMessage:
		snapshotId := msg.snapshotId
		if _, exists := server.snapshots[snapshotId]; !exists {
			server.StartSnapshot(snapshotId)
		}
		data := server.snapshots[snapshotId]
		data.openLinks[src] = false
		allClosed := true
		for _, open := range data.openLinks {
			if open {
				allClosed = false
				break
			}
		}
		if allClosed {
			server.sim.NotifySnapshotComplete(server.Id, snapshotId)
		}
	default:
		log.Fatal("Unknown message type: ", message)
	}
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	openLinks := make(map[string]bool)
	for src := range server.inboundLinks {
		openLinks[src] = true
	}
	server.snapshots[snapshotId] = &SnapshotData{
		tokens:    server.Tokens,
		openLinks: openLinks,
		messages:  make(map[string][]*SnapshotMessage),
	}
	server.SendToNeighbors(MarkerMessage{snapshotId})
}
