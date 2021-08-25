package metrics

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	pgossip "github.com/protolambda/rumor/p2p/gossip"
	"github.com/protolambda/rumor/p2p/gossip/database"
	log "github.com/sirupsen/logrus"
)

type PeerStore struct {
	PeerStore       sync.Map
	MessageDatabase *database.MessageDatabase // TODO: Discuss
	StartTime       time.Time
	MsgNotChannels  map[string](chan bool) // TODO: Unused?
}

// TODO: Remove from here?
type GossipState struct {
	GsNode  pgossip.GossipSub
	CloseGS context.CancelFunc
	// string -> *pubsub.Topic
	Topics sync.Map
	// Validation Filter Flag
	SeenFilter bool
}

func NewPeerStore() PeerStore {
	gm := PeerStore{
		StartTime:      time.Now(),
		MsgNotChannels: make(map[string](chan bool)),
	}
	return gm
}

func (c *PeerStore) ImportPeerStoreMetrics(importFolder string) error {
	// TODO: Load to memory an existing csv
	// Perhaps not needed since we are migrating to a database
	// See: https://github.com/migalabs/armiarma/pull/18

	return nil
}

// Function that resets to 0 the connections/disconnections, and message counters
// this way the Ram Usage gets limited (up to ~10k nodes for a 12h-24h )
// NOTE: Keep in mind that the peers that we ended up connected to, will experience a weid connection time
// TODO: Fix peers that stayed connected to the tool
func (c *PeerStore) ResetDynamicMetrics() {
	log.Info("Reseting Dynamic Metrics in Peer")

	// Iterate throught the peers in the metrics, restarting connection events and messages
	c.PeerStore.Range(func(key interface{}, value interface{}) bool {
		p := value.(Peer)
		p.ResetDynamicMetrics()
		c.PeerStore.Store(key, p)
		return true
	})
	log.Info("Finished Reseting Dynamic Metrics")
}

// Function that adds a notification channel to the message gossip topic
func (c *PeerStore) AddNotChannel(topicName string) {
	c.MsgNotChannels[topicName] = make(chan bool, 100)
}

// Adds or updates peer
func (c *PeerStore) AddPeer(peer Peer) {
	oldData, loaded := c.PeerStore.LoadOrStore(peer.PeerId, peer)

	// If already present
	if loaded {
		// TODO: We could also store the old data if there was a change. For example
		// if a given client upgrated it version. Use oldData
		// See: https://github.com/migalabs/armiarma/issues/17
		// Currently just overwritting what was before
		_ = oldData // TODO:
		c.PeerStore.Store(peer.PeerId, peer)
	}
}

// Add a connection Event to the given peer
func (c *PeerStore) ConnectionEvent(peerId string, direction string) error {
	pMetrics, ok := c.PeerStore.Load(peerId)
	if ok {
		peer := pMetrics.(Peer)
		peer.ConnectionEvent(direction, time.Now())
		c.PeerStore.Store(peerId, peer)
		return nil
	}
	return errors.New("could not add event, peer is not in the list")
}

// Add a connection Event to the given peer
func (c *PeerStore) DisconnectionEvent(peerId string) error {
	pMetrics, ok := c.PeerStore.Load(peerId)
	if ok {
		peer := pMetrics.(Peer)
		peer.DisconnectionEvent(time.Now())
		c.PeerStore.Store(peerId, peer)
		return nil
	}
	return errors.New("could not add connection event, peer is not in the list")
}

// Add a connection Event to the given peer
func (c *PeerStore) MetadataEvent(peerId string, success bool) error {
	pMetrics, ok := c.PeerStore.Load(peerId)
	if ok {
		Peer := pMetrics.(Peer)
		Peer.MetadataRequest = true
		if success {
			Peer.MetadataSucceed = true
		}
		c.PeerStore.Store(peerId, Peer)
		return nil
	}
	return errors.New("counld't add Event, Peer is not in the list: " + peerId)
}

// AddNewAttempts adds the resuts of a new attempt over an existing peer
// increasing the attempt counter and the respective fields
func (gm *PeerStore) ConnectionAttemptEvent(peerId string, succeed bool, err string) error {
	pMetrics, ok := gm.PeerStore.Load(peerId)
	if ok {
		peer := pMetrics.(Peer)
		peer.ConnectionAttemptEvent(succeed, err)
		gm.PeerStore.Store(peerId, peer)
		return nil
	}
	return errors.New("could not add connection attempt, peer is not in the list")
}

// Function that Manages the metrics updates for the incoming messages
// TODO: Rename to AddNewMessageEvent or something like that
func (c *PeerStore) MessageEvent(peerId string, topicName string) error {
	pMetrics, ok := c.PeerStore.Load(peerId)
	if ok {
		peer := pMetrics.(Peer)
		peer.MessageEvent(topicName, time.Now())
		c.PeerStore.Store(peerId, peer)
	} else {
		return errors.New("could not add incomming message to topics list")
	}
	return nil
}

// Get peer data
func (c *PeerStore) GetPeerData(peerId string) (Peer, error) {
	peerData, ok := c.PeerStore.Load(peerId)
	if !ok {
		return Peer{}, errors.New("could not find peer in peerstore: " + peerId)
	}
	return peerData.(Peer), nil
}

// Get a map with the errors we got when connecting and their amount
func (gm *PeerStore) GetErrorCounter() map[string]uint64 {
	errorsAndAmount := make(map[string]uint64)
	gm.PeerStore.Range(func(key interface{}, value interface{}) bool {
		peer := value.(Peer)
		errorsAndAmount[peer.Error]++
		return true
	})

	return errorsAndAmount
}

// Exports to a csv, useful for debug
func (c *PeerStore) ExportToCSV(filePath string) error {
	log.Info("Exporting metrics to csv: ", filePath)
	csvFile, err := os.Create(filePath)
	if err != nil {
		return errors.Wrap(err, "error opening the file "+filePath)
	}
	defer csvFile.Close()

	// First raw of the file will be the Titles of the columns
	_, err = csvFile.WriteString("Peer Id,Node Id,User Agent,Client,Version,Pubkey,Address,Ip,Country,City,Request Metadata,Success Metadata,Attempted,Succeed,Connected,Attempts,Error,Latency,Connections,Disconnections,Connected Time,Beacon Blocks,Beacon Aggregations,Voluntary Exits,Proposer Slashings,Attester Slashings,Total Messages\n")
	if err != nil {
		errors.Wrap(err, "error while writing the titles on the csv "+filePath)
	}

	err = nil
	c.PeerStore.Range(func(k, val interface{}) bool {
		v := val.(Peer)
		_, err = csvFile.WriteString(v.ToCsvLine())
		return true
	})

	if err != nil {
		return errors.Wrap(err, "could not export peer metrics")
	}

	return nil
}
