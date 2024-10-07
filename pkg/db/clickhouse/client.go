package clickhouse

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gnosischain/armiarma/pkg/db/models"
	"github.com/gnosischain/armiarma/pkg/gossipsub"
	eth "github.com/gnosischain/armiarma/pkg/networks/ethereum"
	"github.com/gnosischain/armiarma/pkg/utils"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/libp2p/go-libp2p/core/peer"
    ma "github.com/multiformats/go-multiaddr"
)

const (
	batchSize            = 1000
	batchFlushingTimeout = 5 * time.Second
	maxPersisters        = 2
)

type ClickHouseClient struct {
	ctx    context.Context
	conn   driver.Conn
	config *ClickHouseConfig

	persistC chan interface{}
	doneC    chan struct{}
	wg       *sync.WaitGroup
}

type ClickHouseConfig struct {
	Address  string
	Database string
	Username string
	Password string
}

func NewClickHouseClient(ctx context.Context, config *ClickHouseConfig) (*ClickHouseClient, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.Address},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:          time.Second * 30,
		MaxOpenConns:         5,
		MaxIdleConns:         5,
		ConnMaxLifetime:      time.Hour,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	})

	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to ClickHouse")
	}

	client := &ClickHouseClient{
		ctx:      ctx,
		conn:     conn,
		config:   config,
		persistC: make(chan interface{}, batchSize),
		doneC:    make(chan struct{}),
		wg:       &sync.WaitGroup{},
	}

	for i := 0; i < maxPersisters; i++ {
		go client.launchPersister()
	}

	return client, nil
}

func (c *ClickHouseClient) InitTables() error {
	err := c.initPeerInfoTable()
	if err != nil {
		return errors.Wrap(err, "failed to init peer_info table")
	}

	err = c.initConnEventTable()
	if err != nil {
		return errors.Wrap(err, "failed to init conn_events table")
	}

	err = c.initIpTable()
	if err != nil {
		return errors.Wrap(err, "failed to init ips table")
	}

	err = c.initActivePeersTable()
	if err != nil {
		return errors.Wrap(err, "failed to init active_peers table")
	}

	err = c.initEthNodesTable()
	if err != nil {
		return errors.Wrap(err, "failed to init eth_nodes table")
	}

	err = c.initEthereumNodeStatusTable()
	if err != nil {
		return errors.Wrap(err, "failed to init eth_status table")
	}

	err = c.initEthereumAttestationsTable()
	if err != nil {
		return errors.Wrap(err, "failed to init eth_attestations table")
	}

	err = c.initEthereumBeaconBlocksTable()
	if err != nil {
		return errors.Wrap(err, "failed to init eth_blocks table")
	}

	return nil
}

func (c *ClickHouseClient) launchPersister() {
	c.wg.Add(1)
	defer c.wg.Done()

	batch := make([]interface{}, 0, batchSize)
	ticker := time.NewTicker(batchFlushingTimeout)

	for {
		select {
		case <-c.ctx.Done():
			c.flush(batch)
			return
		case <-c.doneC:
			c.flush(batch)
			return
		case item := <-c.persistC:
			batch = append(batch, item)
			if len(batch) >= batchSize {
				c.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				c.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

func (c *ClickHouseClient) flush(batch []interface{}) {
	if len(batch) == 0 {
		return
	}

	err := c.persistBatch(batch)
	if err != nil {
		log.Errorf("Failed to persist batch: %v", err)
	}
}

func (c *ClickHouseClient) persistBatch(batch []interface{}) error {
	tx, err := c.conn.Begin()
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	for _, item := range batch {
		switch v := item.(type) {
		case *models.HostInfo:
			err = c.persistHostInfo(tx, v)
		case *models.PeerInfo:
			err = c.persistPeerInfo(tx, v)
		case *models.ConnectionAttempt:
			err = c.persistConnectionAttempt(tx, v)
		case *models.ConnEvent:
			err = c.persistConnEvent(tx, v)
		case models.IpInfo:
			err = c.persistIpInfo(tx, v)
		case gossipsub.PersistableMsg:
			err = c.persistGossipsubMsg(tx, v)
		default:
			err = fmt.Errorf("unknown type to persist: %T", v)
		}

		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (c *ClickHouseClient) PersistToDB(item interface{}) {
	c.persistC <- item
}

func (c *ClickHouseClient) Close() {
	close(c.doneC)
	c.wg.Wait()
	c.conn.Close()
}

func (c *ClickHouseClient) initPeerInfoTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS peer_info (
			id UInt64,
			peer_id String,
			network String,
			multi_addrs Array(String),
			ip String,
			port UInt16,
			user_agent String,
			client_name String,
			client_version String,
			client_os String,
			client_arch String,
			protocol_version String,
			sup_protocols Array(String),
			latency Int64,
			deprecated Bool,
			attempted Bool,
			last_activity DateTime64,
			last_conn_attempt DateTime64,
			last_error String
		) ENGINE = MergeTree()
		ORDER BY (peer_id, network)
	`
	return c.conn.Exec(c.ctx, query)
}

func (c *ClickHouseClient) initConnEventTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS conn_events (
			id UInt64,
			peer_id String,
			direction Enum8('inbound' = 1, 'outbound' = 2),
			conn_time DateTime64,
			latency Int64,
			disconn_time DateTime64,
			identified Bool,
			error String
		) ENGINE = MergeTree()
		ORDER BY (peer_id, conn_time)
	`
	return c.conn.Exec(c.ctx, query)
}

func (c *ClickHouseClient) initIpTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS ips (
			id UInt64,
			ip String,
			expiration_time DateTime64,
			continent String,
			continent_code String,
			country String,
			country_code String,
			region String,
			region_name String,
			city String,
			zip String,
			lat Float64,
			lon Float64,
			isp String,
			org String,
			as_raw String,
			asname String,
			mobile Bool,
			proxy Bool,
			hosting Bool
		) ENGINE = MergeTree()
		ORDER BY ip
	`
	return c.conn.Exec(c.ctx, query)
}

func (c *ClickHouseClient) initActivePeersTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS active_peers (
			id UInt64,
			timestamp DateTime64,
			peers Array(UInt64)
		) ENGINE = MergeTree()
		ORDER BY timestamp
	`
	return c.conn.Exec(c.ctx, query)
}

func (c *ClickHouseClient) initEthNodesTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS eth_nodes (
			id UInt64,
			timestamp DateTime64,
			peer_id String,
			node_id String,
			seq UInt64,
			ip String,
			tcp UInt16,
			udp UInt16,
			pubkey String,
			fork_digest String,
			next_fork_version String,
			attnets String,
			attnets_number UInt16
		) ENGINE = MergeTree()
		ORDER BY (node_id, timestamp)
	`
	return c.conn.Exec(c.ctx, query)
}

func (c *ClickHouseClient) initEthereumNodeStatusTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS eth_status (
			id UInt64,
			peer_id String,
			timestamp DateTime64,
			fork_digest String,
			finalized_root String,
			finalized_epoch UInt64,
			head_root String,
			head_slot UInt64,
			seq_number UInt64,
			attnets String,
			syncnets String
		) ENGINE = MergeTree()
		ORDER BY (peer_id, timestamp)
	`
	return c.conn.Exec(c.ctx, query)
}

func (c *ClickHouseClient) initEthereumAttestationsTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS eth_attestations (
			id UInt64,
			msg_id String,
			sender String,
			subnet UInt16,
			slot UInt64,
			arrival_time DateTime64,
			time_in_slot Float64,
			val_pubkey String
		) ENGINE = MergeTree()
		ORDER BY (msg_id, arrival_time)
	`
	return c.conn.Exec(c.ctx, query)
}

func (c *ClickHouseClient) initEthereumBeaconBlocksTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS eth_blocks (
			id UInt64,
			msg_id String,
			sender String,
			slot UInt64,
			arrival_time DateTime64,
			time_in_slot Float64,
			val_idx UInt64
		) ENGINE = MergeTree()
		ORDER BY (msg_id, arrival_time)
	`
	return c.conn.Exec(c.ctx, query)
}

func (c *ClickHouseClient) persistHostInfo(tx driver.Tx, hostInfo *models.HostInfo) error {
	query := `
		INSERT INTO peer_info (
			peer_id, network, multi_addrs, ip, port,
			user_agent, client_name, client_version, client_os, client_arch,
			protocol_version, sup_protocols, latency,
			deprecated, attempted, last_activity, last_conn_attempt, last_error
		) VALUES (
			?, ?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?, ?, ?
		)
	`

	_, err := tx.Exec(c.ctx, query,
		hostInfo.ID.String(),
		string(hostInfo.Network),
		hostInfo.MAddrs,
		hostInfo.IP,
		hostInfo.Port,
		hostInfo.PeerInfo.UserAgent,
		utils.ParseClientType(hostInfo.Network, hostInfo.PeerInfo.UserAgent),
		hostInfo.PeerInfo.ProtocolVersion,
		hostInfo.PeerInfo.Protocols,
		hostInfo.PeerInfo.Latency.Milliseconds(),
		hostInfo.ControlInfo.Deprecated,
		hostInfo.ControlInfo.Attempted,
		hostInfo.ControlInfo.LastActivity,
		hostInfo.ControlInfo.LastConnAttempt,
		hostInfo.ControlInfo.LastError,
	)

	return err
}

func (c *ClickHouseClient) persistPeerInfo(tx driver.Tx, peerInfo *models.PeerInfo) error {
	query := `
		UPDATE peer_info SET
			user_agent = ?,
			client_name = ?,
			client_version = ?,
			client_os = ?,
			client_arch = ?,
			protocol_version = ?,
			sup_protocols = ?,
			latency = ?
		WHERE peer_id = ?
	`

	cliName, cliVers, cliOS, cliArch := utils.ParseClientType(utils.EthereumNetwork, peerInfo.UserAgent)

	_, err := tx.Exec(c.ctx, query,
		peerInfo.UserAgent,
		cliName,
		cliVers,
		cliOS,
		cliArch,
		peerInfo.ProtocolVersion,
		peerInfo.Protocols,
		peerInfo.Latency.Milliseconds(),
		peerInfo.RemotePeer.String(),
	)

	return err
}

func (c *ClickHouseClient) persistConnectionAttempt(tx driver.Tx, connAttempt *models.ConnectionAttempt) error {
	query := `
		UPDATE peer_info SET
			deprecated = ?,
			attempted = ?,
			last_activity = ?,
			last_conn_attempt = ?,
			last_error = ?
		WHERE peer_id = ?
	`

	_, err := tx.Exec(c.ctx, query,
		connAttempt.Status == models.PossitiveAttempt,
		true,
		connAttempt.Timestamp,
		connAttempt.Timestamp,
		connAttempt.Error,
		connAttempt.RemotePeer.String(),
	)

	return err
}

func (c *ClickHouseClient) persistConnEvent(tx driver.Tx, connEvent *models.ConnEvent) error {
	query := `
		INSERT INTO conn_events (
			peer_id, direction, conn_time, latency, disconn_time, identified, error
		) VALUES (
			?, ?, ?, ?, ?, ?, ?
		)
	`

	_, err := tx.Exec(c.ctx, query,
		connEvent.PeerID.String(),
		models.DirectionIndexToString(connEvent.Direction),
		connEvent.ConnTime,
		connEvent.Latency.Milliseconds(),
		connEvent.DiscTime,
		connEvent.Identified,
		connEvent.Error,
	)

	return err
}

func (c *ClickHouseClient) persistIpInfo(tx driver.Tx, ipInfo models.IpInfo) error {
	query := `
		INSERT INTO ips (
			ip, expiration_time, continent, continent_code, country, country_code,
			region, region_name, city, zip, lat, lon, isp, org, as_raw, asname,
			mobile, proxy, hosting
		) VALUES (
			?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?,
			?, ?, ?,
			?, ?, ?
		)
	`

	_, err := tx.Exec(c.ctx, query,
		ipInfo.IP,
		ipInfo.ExpirationTime,
		ipInfo.Continent,
		ipInfo.ContinentCode,
		ipInfo.Country,
		ipInfo.CountryCode,
		ipInfo.Region,
		ipInfo.RegionName,
		ipInfo.City,
		ipInfo.Zip,
		ipInfo.Lat,
		ipInfo.Lon,
		ipInfo.Isp,
		ipInfo.Org,
		ipInfo.As,
		ipInfo.AsName,
		ipInfo.Mobile,
		ipInfo.Proxy,
		ipInfo.Hosting,
	)

	return err
}

func (c *ClickHouseClient) persistGossipsubMsg(tx driver.Tx, msg gossipsub.PersistableMsg) error {
	switch v := msg.(type) {
	case *eth.TrackedAttestation:
		return c.persistEthAttestation(tx, v)
	case *eth.TrackedBeaconBlock:
		return c.persistEthBeaconBlock(tx, v)
	default:
		return fmt.Errorf("unknown gossipsub message type: %T", v)
	}
}

func (c *ClickHouseClient) persistEthAttestation(tx driver.Tx, att *eth.TrackedAttestation) error {
	query := `
		INSERT INTO eth_attestations (
			msg_id, sender, subnet, slot, arrival_time, time_in_slot, val_pubkey
		) VALUES (
			?, ?, ?, ?, ?, ?, ?
		)
	`

	_, err := tx.Exec(c.ctx, query,
		att.MsgID,
		att.Sender.String(),
		att.Subnet,
		att.Slot,
		att.ArrivalTime,
		float64(att.TimeInSlot)/float64(time.Second),
		att.ValPubkey,
	)

	return err
}

func (c *ClickHouseClient) persistEthBeaconBlock(tx driver.Tx, block *eth.TrackedBeaconBlock) error {
	query := `
		INSERT INTO eth_blocks (
			msg_id, sender, slot, arrival_time, time_in_slot, val_idx
		) VALUES (
			?, ?, ?, ?, ?, ?
		)
	`

	_, err := tx.Exec(c.ctx, query,
		block.MsgID,
		block.Sender.String(),
		block.Slot,
		block.ArrivalTime,
		float64(block.TimeInSlot)/float64(time.Second),
		block.ValIndex,
	)

	return err
}

func (c *ClickHouseClient) GetNonDeprecatedPeers() ([]*models.RemoteConnectablePeer, error) {
	query := `
		SELECT peer_id, network, multi_addrs
		FROM peer_info
		WHERE deprecated = false
	`

	rows, err := c.conn.Query(c.ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query non-deprecated peers")
	}
	defer rows.Close()

	var peers []*models.RemoteConnectablePeer
	for rows.Next() {
		var peerID string
		var network string
		var multiAddrs []string

		err := rows.Scan(&peerID, &network, &multiAddrs)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		pid, err := peer.Decode(peerID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to decode peer ID")
		}

		maddrs := make([]ma.Multiaddr, 0, len(multiAddrs))
		for _, addrStr := range multiAddrs {
			maddr, err := ma.NewMultiaddr(addrStr)
			if err != nil {
				log.Warnf("Failed to parse multiaddr %s: %v", addrStr, err)
				continue
			}
			maddrs = append(maddrs, maddr)
		}

		peer := &models.RemoteConnectablePeer{
			ID:      pid,
			Addrs:   maddrs,
			Network: utils.NetworkType(network),
		}
		peers = append(peers, peer)
	}

	return peers, nil
}

// GetPersistable retrieves the persistable information for a given peer ID
func (c *ClickHouseClient) GetPersistable(peerID string) (models.RemoteConnectablePeer, error) {
    query := `
        SELECT peer_id, network, multi_addrs
        FROM peer_info
        WHERE peer_id = ?
    `

    var peerIDStr, networkStr string
    var multiAddrs []string

    err := c.conn.QueryRow(c.ctx, query, peerID).Scan(&peerIDStr, &networkStr, &multiAddrs)
    if err != nil {
        return models.RemoteConnectablePeer{}, errors.Wrap(err, "failed to get persistable peer info")
    }

    pid, err := peer.Decode(peerIDStr)
    if err != nil {
        return models.RemoteConnectablePeer{}, errors.Wrap(err, "failed to decode peer ID")
    }

    maddrs := make([]ma.Multiaddr, 0, len(multiAddrs))
    for _, addrStr := range multiAddrs {
        maddr, err := ma.NewMultiaddr(addrStr)
        if err != nil {
            log.Warnf("Failed to parse multiaddr %s: %v", addrStr, err)
            continue
        }
        maddrs = append(maddrs, maddr)
    }

    return models.RemoteConnectablePeer{
        ID:      pid,
        Addrs:   maddrs,
        Network: utils.NetworkType(networkStr),
    }, nil
}

// UpdateLastActivityTimestamp updates the last activity timestamp for a given peer
func (c *ClickHouseClient) UpdateLastActivityTimestamp(peerID peer.ID, t time.Time) error {
    query := `
        UPDATE peer_info
        SET last_activity = ?
        WHERE peer_id = ?
    `

    _, err := c.conn.Exec(c.ctx, query, t, peerID.String())
    return err
}

// PeerInfoExists checks if a peer exists in the database
func (c *ClickHouseClient) PeerInfoExists(peerID peer.ID) bool {
    var exists bool
    query := `
        SELECT 1
        FROM peer_info
        WHERE peer_id = ?
        LIMIT 1
    `

    err := c.conn.QueryRow(c.ctx, query, peerID.String()).Scan(&exists)
    if err != nil {
        return false
    }
    return exists
}

// GetFullHostInfo retrieves the full host information for a given peer ID
func (c *ClickHouseClient) GetFullHostInfo(peerID peer.ID) (*models.HostInfo, error) {
    query := `
        SELECT
            peer_id, network, multi_addrs, ip, port,
            user_agent, protocol_version, sup_protocols, latency,
            deprecated, attempted, last_activity, last_conn_attempt, last_error
        FROM peer_info
        WHERE peer_id = ?
    `

    var hostInfo models.HostInfo
    var peerIDStr, networkStr string
    var multiAddrs []string
    var lastActivity, lastConnAttempt int64

    err := c.conn.QueryRow(c.ctx, query, peerID.String()).Scan(
        &peerIDStr, &networkStr, &multiAddrs, &hostInfo.IP, &hostInfo.Port,
        &hostInfo.PeerInfo.UserAgent, &hostInfo.PeerInfo.ProtocolVersion, &hostInfo.PeerInfo.Protocols, &hostInfo.PeerInfo.Latency,
        &hostInfo.ControlInfo.Deprecated, &hostInfo.ControlInfo.Attempted, &lastActivity, &lastConnAttempt, &hostInfo.ControlInfo.LastError,
    )
    if err != nil {
        return nil, errors.Wrap(err, "failed to get full host info")
    }

    hostInfo.ID = peerID
    hostInfo.Network = utils.NetworkType(networkStr)
    hostInfo.MAddrs = make([]ma.Multiaddr, 0, len(multiAddrs))
    for _, addrStr := range multiAddrs {
        maddr, err := ma.NewMultiaddr(addrStr)
        if err != nil {
            log.Warnf("Failed to parse multiaddr %s: %v", addrStr, err)
            continue
        }
        hostInfo.MAddrs = append(hostInfo.MAddrs, maddr)
    }

    hostInfo.ControlInfo.LastActivity = time.Unix(lastActivity, 0)
    hostInfo.ControlInfo.LastConnAttempt = time.Unix(lastConnAttempt, 0)

    return &hostInfo, nil
}