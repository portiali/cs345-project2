package chandy_lamport

import "log"

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
	snaps         *SyncMap
}

type localState struct {
	tokens      	*SyncMap
	messages 		*SyncMap
	closedChannels  *SyncMap
	receivedMarkers *SyncMap
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
		NewSyncMap(),
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
	// if message is a token message, 
	if msg, isToken := message.(TokenMessage); isToken {
		server.Tokens += msg.numTokens

		// record the message in the snapshot state that has started but has not yet received a marker on this channel
        server.snaps.Range(func(key, value interface{}) bool {
            state := value.(*localState)
			_, closed := state.closedChannels.Load(src)
            if !closed {
            	list, _ := state.messages.LoadOrStore(src, []interface{}{})
                state.messages.Store(src, append(list.([]interface{}), msg))
            }
            return true
        })
		
	// if message is a marker message
	} else if msg, isMarker := message.(MarkerMessage); isMarker {
        val, ok := server.snaps.Load(msg.snapshotId)

		// first marker on snapshot
        if !ok {
            server.StartSnapshot(msg.snapshotId)
            val, _ = server.snaps.Load(msg.snapshotId)
        }

		// mark channel as closed and record that we've received a marker on this channel
        state := val.(*localState)
        state.closedChannels.Store(src, true)
        state.receivedMarkers.Store(src, true)

		// check if we've received markers on all inbound channels
    	count := 0
    	for inbound := range server.inboundLinks {
			done, ok := state.closedChannels.Load(inbound)
        	if ok && done.(bool) {
            	count++
        	} else {
				break
			}
    	}

		if count == len(server.inboundLinks) {
        	server.sim.NotifySnapshotComplete(server.Id, msg.snapshotId)
    	}
    }
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	localState := &localState{
		tokens: NewSyncMap(),
		messages: NewSyncMap(),
		closedChannels: NewSyncMap(),
		receivedMarkers: NewSyncMap(),
	}
	server.snaps.Store(snapshotId, localState)
	localState.tokens.Store("tokens", server.Tokens)
	server.SendToNeighbors(MarkerMessage{snapshotId})
}
