package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gnosischain/armiarma/pkg/config"
	"github.com/gnosischain/armiarma/pkg/crawler"
	"github.com/gnosischain/armiarma/pkg/db/clickhouse"
	"github.com/gnosischain/armiarma/pkg/utils"
	log "github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v2"
)

var (
	Version = "v2.0.0\n"
	log     = logrus.WithField("module", "ARMIARMA")
)

func main() {
	PrintVersion()

	logrus.SetFormatter(utils.ParseLogFormatter("text"))
	logrus.SetOutput(utils.ParseLogOutput("terminal"))

	app := &cli.App{
		Name:      "armiarma",
		Usage:     "Distributed libp2p crawler that monitors, measures, and exposes the gathered information about libp2p network's overlays.",
		UsageText: "armiarma [commands] [arguments...]",
		Authors: []*cli.Author{
			{
				Name:  "Miga Labs",
				Email: "migalabs@protonmail.com",
			},
		},
		EnableBashCompletion: true,
		Commands: []*cli.Command{
			Eth2CrawlerCommand,
		},
	}

	if err := app.RunContext(context.Background(), os.Args); err != nil {
		log.Errorf("error: %v\n", err)
		os.Exit(1)
	}
}

var Eth2CrawlerCommand = &cli.Command{
	Name:   "eth2",
	Usage:  "crawl the given Ethereum CL network (selected by fork_digest)",
	Action: LaunchEth2Crawler,
	Flags:  getEth2CrawlerFlags(),
}

func LaunchEth2Crawler(c *cli.Context) error {
	log.Infoln("Starting Ethereum Crawler...")

	conf := config.NewEthereumCrawlerConfig()
	conf.Apply(c)

	// Initialize ClickHouse client
	clickhouseConfig, err := clickhouse.LoadClickHouseConfig()
	if err != nil {
		return fmt.Errorf("failed to load ClickHouse config: %v", err)
	}

	clickhouseClient, err := clickhouse.NewClickHouseClient(c.Context, clickhouseConfig)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse client: %v", err)
	}
	defer clickhouseClient.Close()

	// Run migrations
	if err := clickhouseClient.RunMigrations(); err != nil {
		return fmt.Errorf("failed to run ClickHouse migrations: %v", err)
	}

	// Generate the Eth2 crawler struct
	ethCrawler, err := crawler.NewEthereumCrawler(c, *conf, clickhouseClient)
	if err != nil {
		return err
	}

	// Launch the subroutines
	ethCrawler.Run()

	// Handle shutdown signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, syscall.SIGTERM)

	// Keep the app running until a termination signal is received
	sig := <-sigs
	log.Printf("Received %s signal - Stopping...\n", sig.String())
	signal.Stop(sigs)
	ethCrawler.Close()

	return nil
}

func getEth2CrawlerFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "log-level",
			Usage:       "Verbosity level for the Crawler's logs",
			EnvVars:     []string{"ARMIARMA_LOG_LEVEL"},
			DefaultText: config.DefaultLogLevel,
		},
		&cli.StringFlag{
			Name:    "priv-key",
			Usage:   "String representation of the PrivateKey to be used by the crawler",
			EnvVars: []string{"ARMIARMA_PRIV_KEY"},
		},
		&cli.StringFlag{
			Name:        "ip",
			Usage:       "IP in the machine that we want to assign to the crawler",
			EnvVars:     []string{"ARMIARMA_IP"},
			DefaultText: config.DefaultIP,
		},
		&cli.IntFlag{
			Name:        "port",
			Usage:       "TCP and UDP port that the crawler will advertise to establish connections",
			EnvVars:     []string{"ARMIARMA_PORT"},
			DefaultText: fmt.Sprintf("%d", config.DefaultPort),
		},
		&cli.StringFlag{
			Name:        "metrics-ip",
			Usage:       "IP in the machine that will expose the metrics of the crawler",
			EnvVars:     []string{"ARMIARMA_METRICS_IP"},
			DefaultText: config.DefaultMetricsIP,
		},
		&cli.IntFlag{
			Name:        "metrics-port",
			Usage:       "Port that the crawler will use to expose pprof and prometheus metrics",
			EnvVars:     []string{"ARMIARMA_METRICS_PORT"},
			DefaultText: fmt.Sprintf("%d", config.DefaultMetricsPort),
		},
		&cli.StringFlag{
			Name:        "user-agent",
			Usage:       "Agent name that will identify the crawler in the network",
			EnvVars:     []string{"ARMIARMA_USER_AGENT"},
			DefaultText: config.DefaultUserAgent,
		},
		&cli.StringFlag{
			Name:        "clickhouse-address",
			Usage:       "ClickHouse server address",
			EnvVars:     []string{"ARMIARMA_CLICKHOUSE_ADDRESS"},
			Required:    true,
		},
		&cli.StringFlag{
			Name:        "clickhouse-database",
			Usage:       "ClickHouse database name",
			EnvVars:     []string{"ARMIARMA_CLICKHOUSE_DATABASE"},
			Required:    true,
		},
		&cli.StringFlag{
			Name:        "clickhouse-username",
			Usage:       "ClickHouse username",
			EnvVars:     []string{"ARMIARMA_CLICKHOUSE_USERNAME"},
			Required:    true,
		},
		&cli.StringFlag{
			Name:        "clickhouse-password",
			Usage:       "ClickHouse password",
			EnvVars:     []string{"ARMIARMA_CLICKHOUSE_PASSWORD"},
			Required:    true,
		},
		&cli.StringFlag{
			Name:        "peers-backup",
			Usage:       "Time interval that will be used to backup the peer_ids into a single table - allowing to reconstruct the network in past-crawled times",
			EnvVars:     []string{"ARMIARMA_BACKUP_INTERVAL"},
			DefaultText: config.DefaultActivePeersBackupInterval,
		},
		&cli.StringFlag{
			Name:        "remote-cl-endpoint",
			Usage:       "Remote Ethereum Consensus Layer Client to request metadata (experimental)",
			EnvVars:     []string{"ARMIARMA_REMOTE_CL_ENDPOINT"},
			DefaultText: config.DefaultCLRemoteEndpoint,
		},
		&cli.StringFlag{
			Name:        "fork-digest",
			Usage:       "Fork Digest of the Ethereum Consensus Layer network that we want to crawl",
			EnvVars:     []string{"ARMIARMA_FORK_DIGEST"},
			DefaultText: eth.DefaultForkDigest,
		},
		&cli.StringSliceFlag{
			Name:    "bootnode",
			Usage:   "List of bootnodes that the crawler will use to discover more peers in the network (One --bootnode <bootnode> per bootnode)",
			EnvVars: []string{"ARMIARMA_BOOTNODES"},
		},
		&cli.StringSliceFlag{
			Name:        "gossip-topic",
			Usage:       "List of gossipsub topics that the crawler will subscribe to",
			EnvVars:     []string{"ARMIARMA_GOSSIP_TOPICS"},
			DefaultText: "One --gossip-topic <topic> per topic",
		},
		&cli.StringSliceFlag{
			Name:    "subnet",
			Usage:   "List of subnets (gossipsub topics) that we want to subscribe the crawler to (One --subnet <subnet_id> per subnet)",
			EnvVars: []string{"ARMIARMA_SUBNETS"},
		},
		&cli.BoolFlag{
			Name:    "persist-connevents",
			Usage:   "Decide whether we want to track the connection-events into the DB (Disk intense)",
			EnvVars: []string{"ARMIARMA_PERSIST_CONNEVENTS"},
		},
		&cli.BoolFlag{
			Name:    "persist-msgs",
			Usage:   "Decide whether we want to track the msgs-metadata into the DB",
			EnvVars: []string{"ARMIARMA_PERSIST_MSGS"},
		},
		&cli.StringFlag{
			Name:    "val-pubkeys",
			Usage:   "Path of the file that has the pubkeys of those validators that we want to track (experimental)",
			EnvVars: []string{"ARMIARMA_VAL_PUBKEYS"},
		},
		&cli.StringFlag{
			Name:    "sse-ip",
			Usage:   "IP to expose the SSE server",
			EnvVars: []string{"ARMIARMA_SSE_IP"},
		},
		&cli.StringFlag{
			Name:    "sse-port",
			Usage:   "Port to expose the SSE server",
			EnvVars: []string{"ARMIARMA_SSE_PORT"},
		},
	}
}

func PrintVersion() {
	fmt.Println("Armiarma_" + Version)
}