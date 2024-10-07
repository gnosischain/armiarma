package clickhouse

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (c *ClickHouseClient) RunMigrations() error {
	migrations := []struct {
		name string
		fn   func() error
	}{
		{"Create initial tables", c.createInitialTables},
		{"Add indexes", c.addIndexes},
		{"Add materialized views", c.addMaterializedViews},
		{"Update schema versions", c.updateSchemaVersions},
		// Add more migrations as needed
	}

	for _, migration := range migrations {
		log.Infof("Running migration: %s", migration.name)
		if err := migration.fn(); err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to run migration: %s", migration.name))
		}
	}

	return nil
}

func (c *ClickHouseClient) createInitialTables() error {
	return c.InitTables()
}

func (c *ClickHouseClient) addIndexes() error {
	queries := []string{
		`ALTER TABLE peer_info ADD INDEX idx_peer_id (peer_id) TYPE minmax GRANULARITY 8192`,
		`ALTER TABLE peer_info ADD INDEX idx_network (network) TYPE set(0) GRANULARITY 8192`,
		`ALTER TABLE peer_info ADD INDEX idx_client_name (client_name) TYPE set(0) GRANULARITY 8192`,
		`ALTER TABLE conn_events ADD INDEX idx_peer_id (peer_id) TYPE minmax GRANULARITY 8192`,
		`ALTER TABLE ips ADD INDEX idx_ip (ip) TYPE minmax GRANULARITY 8192`,
		`ALTER TABLE eth_nodes ADD INDEX idx_node_id (node_id) TYPE minmax GRANULARITY 8192`,
		`ALTER TABLE eth_status ADD INDEX idx_peer_id (peer_id) TYPE minmax GRANULARITY 8192`,
		`ALTER TABLE eth_attestations ADD INDEX idx_msg_id (msg_id) TYPE minmax GRANULARITY 8192`,
		`ALTER TABLE eth_blocks ADD INDEX idx_msg_id (msg_id) TYPE minmax GRANULARITY 8192`,
	}

	for _, query := range queries {
		if err := c.conn.Exec(context.Background(), query); err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to execute query: %s", query))
		}
	}

	return nil
}

func (c *ClickHouseClient) addMaterializedViews() error {
	queries := []string{
		`
		CREATE MATERIALIZED VIEW IF NOT EXISTS mv_client_distribution
		ENGINE = SummingMergeTree()
		ORDER BY (client_name, date)
		POPULATE
		AS SELECT
			client_name,
			toDate(last_activity) AS date,
			count(*) AS count
		FROM peer_info
		WHERE deprecated = false AND attempted = true AND client_name != ''
		GROUP BY client_name, date
		`,
		`
		CREATE MATERIALIZED VIEW IF NOT EXISTS mv_geo_distribution
		ENGINE = SummingMergeTree()
		ORDER BY (country_code, date)
		POPULATE
		AS SELECT
			ips.country_code,
			toDate(peer_info.last_activity) AS date,
			count(*) AS count
		FROM peer_info
		JOIN ips ON peer_info.ip = ips.ip
		WHERE peer_info.deprecated = false AND peer_info.attempted = true AND peer_info.client_name != ''
		GROUP BY ips.country_code, date
		`,
	}

	for _, query := range queries {
		if err := c.conn.Exec(context.Background(), query); err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to execute query: %s", query))
		}
	}

	return nil
}

func (c *ClickHouseClient) updateSchemaVersions() error {
	query := `
	CREATE TABLE IF NOT EXISTS schema_versions (
		version UInt32,
		applied_at DateTime DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY version
	`

	if err := c.conn.Exec(context.Background(), query); err != nil {
		return errors.Wrap(err, "failed to create schema_versions table")
	}

	query = `INSERT INTO schema_versions (version) VALUES (1)`
	if err := c.conn.Exec(context.Background(), query); err != nil {
		return errors.Wrap(err, "failed to insert initial schema version")
	}

	return nil
}

// Add more migration functions as needed

func (c *ClickHouseClient) GetCurrentSchemaVersion() (int, error) {
	var version int
	query := `SELECT max(version) FROM schema_versions`
	
	err := c.conn.QueryRow(context.Background(), query).Scan(&version)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get current schema version")
	}

	return version, nil
}