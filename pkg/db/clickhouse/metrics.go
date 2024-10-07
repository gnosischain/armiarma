package clickhouse

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

func (c *ClickHouseClient) GetClientDistribution() (map[string]interface{}, error) {
	query := `
		SELECT 
			client_name, 
			count(*) as count
		FROM peer_info
		WHERE 
			deprecated = false AND 
			attempted = true AND 
			client_name != '' AND 
			last_activity > subtractDays(now(), 180)
		GROUP BY client_name
		ORDER BY count DESC
	`

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute client distribution query")
	}
	defer rows.Close()

	distribution := make(map[string]interface{})
	for rows.Next() {
		var clientName string
		var count uint64
		if err := rows.Scan(&clientName, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan client distribution row")
		}
		distribution[clientName] = count
	}

	return distribution, nil
}

func (c *ClickHouseClient) GetVersionDistribution() (map[string]interface{}, error) {
	query := `
		SELECT 
			client_name || '_' || client_version as client_version, 
			count(*) as count
		FROM peer_info
		WHERE 
			deprecated = false AND 
			attempted = true AND 
			client_name != '' AND 
			client_version != '' AND
			last_activity > subtractDays(now(), 180)
		GROUP BY client_name, client_version
		ORDER BY client_name DESC, count DESC
	`

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute version distribution query")
	}
	defer rows.Close()

	distribution := make(map[string]interface{})
	for rows.Next() {
		var clientVersion string
		var count uint64
		if err := rows.Scan(&clientVersion, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan version distribution row")
		}
		distribution[clientVersion] = count
	}

	return distribution, nil
}

func (c *ClickHouseClient) GetGeoDistribution() (map[string]interface{}, error) {
	query := `
		SELECT 
			ips.country_code as country_code,
			count(*) as count
		FROM peer_info
		JOIN ips ON peer_info.ip = ips.ip
		WHERE 
			peer_info.deprecated = false AND 
			peer_info.attempted = true AND 
			peer_info.client_name != '' AND 
			peer_info.last_activity > subtractDays(now(), 180)
		GROUP BY country_code
		ORDER BY count DESC
	`

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute geo distribution query")
	}
	defer rows.Close()

	distribution := make(map[string]interface{})
	for rows.Next() {
		var countryCode string
		var count uint64
		if err := rows.Scan(&countryCode, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan geo distribution row")
		}
		distribution[countryCode] = count
	}

	return distribution, nil
}

func (c *ClickHouseClient) GetOsDistribution() (map[string]interface{}, error) {
	query := `
		SELECT
			client_os,
			count(*) as count
		FROM peer_info
		WHERE
			deprecated = false AND
			attempted = true AND
			client_name != '' AND
			last_activity > subtractDays(now(), 180)
		GROUP BY client_os
		ORDER BY count DESC
	`

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute OS distribution query")
	}
	defer rows.Close()

	distribution := make(map[string]interface{})
	for rows.Next() {
		var os string
		var count uint64
		if err := rows.Scan(&os, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan OS distribution row")
		}
		distribution[os] = count
	}

	return distribution, nil
}

func (c *ClickHouseClient) GetArchDistribution() (map[string]interface{}, error) {
	query := `
		SELECT
			client_arch,
			count(*) as count
		FROM peer_info
		WHERE
			deprecated = false AND
			attempted = true AND
			client_name != '' AND
			last_activity > subtractDays(now(), 180)
		GROUP BY client_arch
		ORDER BY count DESC
	`

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute architecture distribution query")
	}
	defer rows.Close()

	distribution := make(map[string]interface{})
	for rows.Next() {
		var arch string
		var count uint64
		if err := rows.Scan(&arch, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan architecture distribution row")
		}
		distribution[arch] = count
	}

	return distribution, nil
}

func (c *ClickHouseClient) GetHostingDistribution() (map[string]interface{}, error) {
	query := `
		SELECT
			if(ips.mobile, 'mobile_ips', if(ips.proxy, 'under_proxy', if(ips.hosting, 'hosted_ips', 'other'))) as hosting_type,
			count(*) as count
		FROM peer_info
		JOIN ips ON peer_info.ip = ips.ip
		WHERE
			peer_info.deprecated = false AND
			peer_info.attempted = true AND
			peer_info.client_name != '' AND
			peer_info.last_activity > subtractDays(now(), 180)
		GROUP BY hosting_type
		ORDER BY count DESC
	`

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute hosting distribution query")
	}
	defer rows.Close()

	distribution := make(map[string]interface{})
	for rows.Next() {
		var hostingType string
		var count uint64
		if err := rows.Scan(&hostingType, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan hosting distribution row")
		}
		distribution[hostingType] = count
	}

	return distribution, nil
}

func (c *ClickHouseClient) GetRTTDistribution() (map[string]interface{}, error) {
	query := `
		SELECT
			multiIf(
				latency between 0 and 100, '0-100ms',
				latency between 101 and 200, '101-200ms',
				latency between 201 and 300, '201-300ms',
				latency between 301 and 400, '301-400ms',
				latency between 401 and 500, '401-500ms',
				latency between 501 and 600, '501-600ms',
				latency between 601 and 700, '601-700ms',
				latency between 701 and 800, '701-800ms',
				latency between 801 and 900, '801-900ms',
				latency between 901 and 1000, '901-1000ms',
				'+1s'
			) as latency_range,
			count(*) as count
		FROM peer_info
		WHERE
			deprecated = false AND
			attempted = true AND
			client_name != '' AND
			last_activity > subtractDays(now(), 180)
		GROUP BY latency_range
		ORDER BY count DESC
	`

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute RTT distribution query")
	}
	defer rows.Close()

	distribution := make(map[string]interface{})
	for rows.Next() {
		var latencyRange string
		var count uint64
		if err := rows.Scan(&latencyRange, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan RTT distribution row")
		}
		distribution[latencyRange] = count
	}

	return distribution, nil
}

func (c *ClickHouseClient) GetIPDistribution() (map[string]interface{}, error) {
	query := `
		WITH ip_counts AS (
			SELECT
				ip,
				count(*) as nodes
			FROM peer_info
			WHERE
				deprecated = false AND
				attempted = true AND
				client_name != '' AND
				last_activity > subtractDays(now(), 180)
			GROUP BY ip
		)
		SELECT
			toString(nodes) as nodes_per_ip,
			count(*) as number_of_ips
		FROM ip_counts
		GROUP BY nodes
		ORDER BY number_of_ips DESC
	`

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute IP distribution query")
	}
	defer rows.Close()

	distribution := make(map[string]interface{})
	for rows.Next() {
		var nodesPerIP string
		var numberOfIPs uint64
		if err := rows.Scan(&nodesPerIP, &numberOfIPs); err != nil {
			return nil, errors.Wrap(err, "failed to scan IP distribution row")
		}
		distribution[nodesPerIP] = numberOfIPs
	}

	return distribution, nil
}

func (c *ClickHouseClient) GetNodePerForkDistribution() (map[string]interface{}, error) {
	query := `
		SELECT
			fork_digest,
			count(*) as count
		FROM eth_nodes
		WHERE
			timestamp > subtractDays(now(), 1)
		GROUP BY fork_digest
		ORDER BY count DESC
	`

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute node per fork distribution query")
	}
	defer rows.Close()

	distribution := make(map[string]interface{})
	for rows.Next() {
		var forkDigest string
		var count uint64
		if err := rows.Scan(&forkDigest, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan node per fork distribution row")
		}
		distribution[forkDigest] = count
	}

	return distribution, nil
}

func (c *ClickHouseClient) GetAttnetsDistribution() (map[string]interface{}, error) {
	query := `
		SELECT
			attnets_number,
			count(*) as count
		FROM eth_nodes
		WHERE
			timestamp > subtractDays(now(), 1)
		GROUP BY attnets_number
		ORDER BY count DESC
	`

	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute attnets distribution query")
	}
	defer rows.Close()

	distribution := make(map[string]interface{})
	for rows.Next() {
		var attnetsNumber int
		var count uint64
		if err := rows.Scan(&attnetsNumber, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan attnets distribution row")
		}
		distribution[string(attnetsNumber)] = count
	}

	return distribution, nil
}

func (c *ClickHouseClient) GetDeprecatedNodes() (int, error) {
	var count int
	err := c.conn.QueryRow(context.Background(), `
		SELECT count(*) 
		FROM peer_info 
		WHERE deprecated = true
	`).Scan(&count)

	if err != nil {
		return 0, errors.Wrap(err, "failed to get deprecated node count")
	}

	return count, nil
}